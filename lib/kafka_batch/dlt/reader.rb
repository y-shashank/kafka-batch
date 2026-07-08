# frozen_string_literal: true

require "rdkafka"
require "oj"
require "base64"

module KafkaBatch
  module Dlt
    # Tail-reads the dead-letter Kafka topic for the dashboard (assign-based, no
    # offset commits). Newest messages first; cursor pagination via +before+.
    class Reader
      POLL_TIMEOUT_MS   = 2_000
      SCAN_PER_PARTITION = 100
      STATS_SAMPLE_LIMIT = 500

      DLT_TYPES = %w[
        job expired callback callback_error malformed_event incomplete_event
        malformed_callback malformed_ingest retry_routing
      ].freeze

      def initialize(topic: nil, consumer: nil)
        @topic    = topic || KafkaBatch.config.dead_letter_topic
        @consumer = consumer
      end

      # @return [Hash] { topic:, partitions:, total:, watermarks: { partition => { low:, high: } } }
      def watermarks
        parts = partition_ids
        marks = {}
        total = 0
        parts.each do |p|
          low, high = consumer.query_watermark_offsets(@topic, p, POLL_TIMEOUT_MS)
          marks[p]  = { low: low, high: high }
          total    += [high - low, 0].max
        end
        { topic: @topic, partitions: parts.size, total: total, watermarks: marks }
      end

      # @param type [String, nil] filter by dlt_type
      # @param before [String, nil] cursor from a prior page (older messages)
      # @param limit [Integer]
      # @return [Hash] { messages:, has_older:, cursor_older: }
      def fetch_page(type: nil, before: nil, limit: 25)
        limit = [limit.to_i, 1].max
        pool  = tail_messages(scan_per_partition: SCAN_PER_PARTITION)
        pool  = filter_type(pool, type) if type && !type.to_s.empty?
        pool  = apply_before(pool, decode_cursor(before)) if before

        pool.sort_by! { |m| sort_key(m) }.reverse!
        page   = pool.first(limit)
        older  = pool.size > limit
        cursor = page.last ? encode_cursor(page.last) : nil

        { messages: page, has_older: older, cursor_older: cursor }
      end

      # Sample recent messages for type breakdown (not a full-topic scan).
      # @return [Array<Hash>]
      def sample_messages(limit: STATS_SAMPLE_LIMIT)
        tail_messages(scan_per_partition: [limit, SCAN_PER_PARTITION].max)
          .sort_by { |m| sort_key(m) }
          .reverse
          .first(limit)
      end

      def close
        @consumer&.close
      rescue StandardError
        nil
      ensure
        @consumer = nil
      end

      private

      def partition_ids
        KafkaBatch.ensure_karafka_configured! if defined?(Karafka)
        return [0] unless defined?(Karafka::Admin)

        meta   = Karafka::Admin.cluster_info
        topics = meta.topics
        found  =
          if topics.is_a?(Hash)
            topics[@topic]
          else
            topics.find { |t| topic_name(t) == @topic }
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

      def tail_messages(scan_per_partition:)
        found = []
        partition_ids.each do |partition|
          low, high = consumer.query_watermark_offsets(@topic, partition, POLL_TIMEOUT_MS)
          next if high <= low

          start = [low, high - scan_per_partition].max
          read_forward(partition, start, high - 1, found)
        end
        found
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Dlt::Reader] tail read failed: #{e.message}")
        []
      end

      def read_forward(partition, start_offset, end_offset, found)
        assign_at(partition, start_offset)
        while start_offset <= end_offset
          msg = consumer.poll(POLL_TIMEOUT_MS)
          break if msg.nil?
          next unless msg.partition == partition

          if msg.offset > end_offset
            break
          end

          found << decode_message(msg)
          start_offset = msg.offset + 1
        end
      rescue Rdkafka::RdkafkaError => e
        KafkaBatch.logger.warn(
          "[KafkaBatch][Dlt::Reader] partition #{partition} read error: #{e.message}"
        )
      ensure
        unassign
      end

      def decode_message(msg)
        data = parse_payload(msg.payload)
        {
          partition:      msg.partition,
          offset:         msg.offset,
          timestamp:      msg.timestamp,
          dlt_type:       data["dlt_type"].to_s,
          dlt_at:         data["dlt_at"],
          batch_id:       data["batch_id"],
          job_id:         data["job_id"],
          worker_class:   data["worker_class"],
          callback_class: data["callback_class"],
          source_topic:   data["dlt_source_topic"],
          error_class:    data["dlt_error_class"],
          error_message:  data["dlt_error_message"],
          payload:        data
        }
      end

      def parse_payload(raw)
        Oj.load(raw, mode: :compat)
      rescue StandardError
        { "dlt_type" => "unknown", "dlt_raw_payload" => raw.to_s }
      end

      def filter_type(messages, type)
        t = type.to_s
        messages.select { |m| m[:dlt_type] == t }
      end

      def sort_key(msg)
        ts = msg[:dlt_at] || msg[:timestamp]
        [ts.to_s, msg[:partition].to_i, msg[:offset].to_i]
      end

      def apply_before(messages, cursor)
        return messages unless cursor

        cut = sort_key(cursor)
        messages.select { |m| sort_key(m) < cut }
      end

      def encode_cursor(msg)
        payload = "#{msg[:partition]}:#{msg[:offset]}:#{msg[:dlt_at] || msg[:timestamp]}"
        Base64.urlsafe_encode64(payload, padding: false)
      end

      def decode_cursor(token)
        return nil if token.nil? || token.to_s.empty?

        part, offset, ts = Base64.urlsafe_decode64(token).split(":", 3)
        { partition: part.to_i, offset: offset.to_i, dlt_at: ts, timestamp: ts }
      rescue StandardError
        nil
      end

      def assign_at(partition, offset)
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic_and_partitions_with_offsets(@topic, partition => offset)
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
          "bootstrap.servers"  => Array(cfg.brokers).join(","),
          "group.id"           => "#{cfg.consumer_group}-dlt-reader",
          "enable.auto.commit" => false,
          "auto.offset.reset"  => "error"
        }
        overrides = (cfg.consumer_config || {}).each_with_object({}) do |(k, v), h|
          h[k.to_s] = v
        end
        Rdkafka::Config.new(base.merge(overrides)).consumer
      end
    end
  end
end
