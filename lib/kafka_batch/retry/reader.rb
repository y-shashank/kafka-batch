# frozen_string_literal: true

require "rdkafka"
require "oj"
require "base64"

module KafkaBatch
  module Retry
    # Assign-based reader for pending retry-tier messages (dashboard).
    # Starts at max(committed_offset, skip_until+1) per partition so delete-all
    # and consumer progress are reflected without scanning skipped offsets.
    class Reader
      POLL_TIMEOUT_MS = 2_000
      PER_TIER        = 50
      SCAN_MULTIPLIER = 4 # read extra to fill page after cancel filtering

      def initialize(consumer: nil)
        @consumer = consumer
      end

      # @param cursor [String, nil] opaque pagination token from a prior page
      # @param per_tier [Integer]
      # @return [Hash]
      def fetch_page(cursor: nil, per_tier: PER_TIER)
        per_tier = [[per_tier.to_i, 1].max, 200].min
        cursors  = decode_cursor(cursor)
        skip     = KafkaBatch::RetryCancel.skip_map
        commits  = committed_starts

        failures = []
        next_cursors = {}
        has_next = false

        KafkaBatch.config.retry_tiers.keys.each do |tier|
          topic = KafkaBatch.config.retry_topic_for(tier)
          tier_cursor = cursors[topic] || {}
          page, advance, more = read_tier(
            topic: topic,
            tier: tier,
            per_tier: per_tier,
            tier_cursor: tier_cursor,
            skip: skip,
            commits: commits[topic] || {}
          )
          failures.concat(page)
          next_cursors[topic] = advance if advance
          has_next ||= more
        end

        {
          failures: failures,
          has_next: has_next,
          cursor: has_next ? encode_cursor(next_cursors) : nil,
          per_tier: per_tier
        }
      end

      # Snapshot high-watermark-1 per retry partition for delete-all.
      # @return [Hash] { topic => { partition => offset } }
      def snapshot_watermarks
        out = {}
        KafkaBatch.config.retry_topics.each do |topic|
          parts = {}
          partition_ids(topic).each do |p|
            low, high = consumer.query_watermark_offsets(topic, p, POLL_TIMEOUT_MS)
            next if high <= low

            parts[p] = high - 1
          end
          out[topic] = parts unless parts.empty?
        end
        out
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch::Retry::Reader] snapshot failed: #{e.message}")
        {}
      end

      def close
        @consumer&.close
      rescue StandardError
        nil
      ensure
        @consumer = nil
      end

      private

      def read_tier(topic:, tier:, per_tier:, tier_cursor:, skip:, commits:)
        found = []
        advance = tier_cursor.dup
        more = false
        budget = per_tier * SCAN_MULTIPLIER

        partition_ids(topic).each do |partition|
          break if found.size >= per_tier

          low, high = consumer.query_watermark_offsets(topic, partition, POLL_TIMEOUT_MS)
          next if high <= low

          skip_until = skip_offset(skip, topic, partition)
          committed  = commits[partition]
          start =
            if tier_cursor[partition.to_s] || tier_cursor[partition]
              (tier_cursor[partition.to_s] || tier_cursor[partition]).to_i + 1
            else
              bases = [low]
              bases << (skip_until + 1) if skip_until
              bases << committed if committed && committed >= 0
              bases.max
            end
          next if start >= high

          end_off = high - 1
          read_forward(topic, partition, start, end_off, found, budget - found.size, tier) do |row|
            advance[partition.to_s] = row[:offset]
          end
        end

        page = found.first(per_tier)
        more = found.size > per_tier
        # If we filled the scan budget without filling the page, still allow next
        # when the last partition had unread data past advance.
        more ||= page.size >= per_tier

        [page, advance, more && !advance.empty?]
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch::Retry::Reader] tier #{tier} read failed: #{e.message}")
        [[], tier_cursor, false]
      end

      def read_forward(topic, partition, start_offset, end_offset, found, limit, tier)
        return if limit <= 0

        assign_at(topic, partition, start_offset)
        seen = 0
        offset = start_offset
        while offset <= end_offset && seen < limit && found.size < limit
          msg = consumer.poll(POLL_TIMEOUT_MS)
          break if msg.nil?
          next unless msg.topic == topic && msg.partition == partition
          break if msg.offset > end_offset

          offset = msg.offset + 1
          seen += 1
          row = decode_message(msg, tier)
          next if row.nil?
          next if KafkaBatch::RetryCancel.cancelled?(row[:job_id])

          found << row
          yield row if block_given?
        end
      rescue Rdkafka::RdkafkaError => e
        KafkaBatch.logger.warn(
          "[KafkaBatch::Retry::Reader] #{topic}/#{partition} read error: #{e.message}"
        )
      ensure
        unassign
      end

      def decode_message(msg, tier)
        data = parse_payload(msg.payload)
        job_id = data["job_id"].to_s
        return nil if job_id.empty?

        {
          job_id: job_id,
          batch_id: data["batch_id"],
          worker_class: data["worker_class"].to_s,
          attempt: data["attempt"].to_i,
          status: "retrying",
          next_retry_at: data["retry_after"],
          retry_to: data["retry_to"],
          topic: msg.topic,
          partition: msg.partition,
          offset: msg.offset,
          tier: tier.to_s,
          error_class: data["error_class"] || data["last_error_class"],
          error_message: data["error_message"] || data["last_error_message"],
          failed_at: data["failed_at"]
        }
      end

      def parse_payload(raw)
        Oj.load(raw, mode: :compat)
      rescue StandardError
        {}
      end

      def skip_offset(skip, topic, partition)
        v = skip["#{topic}:#{partition}"]
        return nil if v.nil? || v.to_s.empty?

        v.to_i
      end

      # Lowest committed offset across Ruby control + Go retry groups (pending window).
      def committed_starts
        out = Hash.new { |h, k| h[k] = {} }
        return out unless KafkaBatch::Lag.available?

        prefix = KafkaBatch.config.consumer_group
        groups = {
          "#{prefix}-control" => KafkaBatch.config.retry_topics,
          "#{prefix}-retry" => KafkaBatch.config.retry_topics
        }
        groups.each do |group, topics|
          data = KafkaBatch::Lag.read_group(group, topics)
          topics.each do |topic|
            parts = (data[group] || {})[topic] || {}
            parts.each do |partition, info|
              off = info[:offset].to_i
              next if off < 0

              p = partition.to_i
              cur = out[topic][p]
              out[topic][p] = cur.nil? ? off : [cur, off].min
            end
          end
        rescue StandardError => e
          KafkaBatch.logger.debug("[KafkaBatch::Retry::Reader] lag #{group}: #{e.message}")
        end
        out
      end

      def partition_ids(topic)
        KafkaBatch.ensure_karafka_configured! if defined?(Karafka)
        return [0] unless defined?(Karafka::Admin)

        meta = Karafka::Admin.cluster_info
        topics = meta.topics
        found =
          if topics.is_a?(Hash)
            topics[topic]
          else
            topics.find { |t| topic_name(t) == topic }
          end
        return [0] unless found

        count = found.respond_to?(:partition_count) ? found.partition_count : found[:partition_count]
        count = count.to_i
        count = 1 if count < 1
        (0...count).to_a
      rescue StandardError
        [0]
      end

      def topic_name(t)
        t.respond_to?(:topic_name) ? t.topic_name : t[:topic_name]
      end

      def encode_cursor(cursors)
        Base64.urlsafe_encode64(Oj.dump(cursors, mode: :compat), padding: false)
      end

      def decode_cursor(token)
        return {} if token.nil? || token.to_s.empty?

        Oj.load(Base64.urlsafe_decode64(token), mode: :compat)
      rescue StandardError
        {}
      end

      def assign_at(topic, partition, offset)
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic_and_partitions_with_offsets(topic, partition => offset)
        consumer.assign(tpl)
      end

      def unassign
        consumer.assign(Rdkafka::Consumer::TopicPartitionList.new)
      rescue StandardError
        nil
      end

      def consumer
        @consumer ||= build_consumer
      end

      def build_consumer
        cfg = KafkaBatch.config
        base = {
          "bootstrap.servers" => Array(cfg.brokers).join(","),
          "group.id" => "#{cfg.consumer_group}-retry-reader",
          "enable.auto.commit" => false,
          "auto.offset.reset" => "error"
        }
        overrides = (cfg.consumer_config || {}).each_with_object({}) do |(k, v), h|
          h[k.to_s] = v
        end
        Rdkafka::Config.new(base.merge(overrides)).consumer
      end
    end
  end
end
