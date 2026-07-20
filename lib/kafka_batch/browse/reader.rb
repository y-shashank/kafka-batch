# frozen_string_literal: true

require "rdkafka"
require "oj"
require "base64"

module KafkaBatch
  module Browse
    # Assign-based Kafka browser for the dashboard: list pending (or seekable)
    # messages after a consumer group's committed offsets. Never joins the real
    # job consumer group and never commits.
    class Reader
      POLL_TIMEOUT_MS = 2_000
      PAGE_SIZE = 50
      MAX_PAGE = 100
      PAYLOAD_MAX_CHARS = 4_000

      def initialize(consumer: nil)
        @consumer = consumer
      end

      # Topics from the same lag map as the Kafka lag page (admin committed lag).
      def list_topics
        return [] unless KafkaBatch::Lag.available?

        rows = KafkaBatch::Lag.partitions.reject { |r| r[:log_archive] }
        rows.group_by { |r| [r[:group], r[:topic]] }.map do |(group, topic), parts|
          {
            group: group,
            topic: topic,
            partitions: parts.size,
            lag: parts.sum { |p| p[:lag].to_i },
            partition_meta: parts.map do |p|
              {
                partition: p[:partition],
                committed: p[:committed],
                end_offset: p[:end_offset],
                lag: p[:lag],
                never_consumed: p[:never_consumed]
              }
            end.sort_by { |p| p[:partition] }
          }
        end.sort_by { |t| [-t[:lag], t[:group], t[:topic]] }
      end

      # @param topic [String]
      # @param group [String] consumer group whose committed offsets define "pending"
      # @param partition [Integer, nil] restrict to one partition
      # @param start_offset [Integer, nil] explicit seek (defaults to committed)
      # @param cursor [String, nil] opaque next-page token
      # @param limit [Integer] page size (default 50)
      def fetch_page(topic:, group:, partition: nil, start_offset: nil, cursor: nil, limit: PAGE_SIZE)
        topic = topic.to_s
        group = group.to_s
        raise ArgumentError, "topic required" if topic.empty?
        raise ArgumentError, "group required" if group.empty?

        limit = [[limit.to_i, 1].max, MAX_PAGE].min
        cursors = decode_cursor(cursor)
        part_filter = partition.nil? || partition.to_s.empty? ? nil : partition.to_i
        explicit_start = start_offset.nil? || start_offset.to_s.empty? ? nil : start_offset.to_i

        commits = committed_meta(group, topic)
        parts = partition_ids(topic)
        parts = parts.select { |p| p == part_filter } if part_filter
        raise ArgumentError, "partition not found" if part_filter && parts.empty?

        messages = []
        advance = cursors.dup
        highs = {}

        parts.each do |p|
          break if messages.size >= limit

          low, high = consumer.query_watermark_offsets(topic, p, POLL_TIMEOUT_MS)
          highs[p] = high
          next if high <= low

          meta = commits[p] || {}
          advancing = advance.key?(p.to_s) || advance.key?(p)

          # Default = unprocessed only, using the same admin lag as the Kafka lag
          # page (never-consumed / lag 0 ⇒ no rows, even if the log retains history).
          unless explicit_start || advancing
            next if admin_lag(meta) <= 0
          end

          start = start_offset_for(p, advance, explicit_start, meta, low, high)
          next if start.nil? || start >= high

          end_off = high - 1
          budget = limit - messages.size
          floor = pending_floor(meta, explicit_start, low)
          read_forward(topic, p, start, end_off, messages, budget, floor: floor) do |row|
            advance[p.to_s] = row[:offset]
          end
        end

        page = messages.first(limit)
        more = page.size >= limit &&
               more_remaining?(topic, parts, advance, explicit_start, commits, highs)

        {
          messages: page,
          has_next: more,
          cursor: more ? encode_cursor(advance) : nil,
          limit: limit,
          topic: topic,
          group: group,
          partition: part_filter,
          start_offset: explicit_start,
          commits: commits.transform_values { |m| m[:offset] }.compact.transform_keys(&:to_s)
        }
      end

      def close
        @consumer&.close
      rescue StandardError
        nil
      ensure
        @consumer = nil
      end

      private

      # Same lag semantics as KafkaBatch::Lag / the Kafka lag page.
      def admin_lag(meta)
        return 0 if meta.nil? || meta.empty?
        return 0 if meta[:never_consumed]
        return 0 unless meta[:offset] && meta[:offset] >= 0

        lag = meta[:lag].to_i
        lag.negative? ? 0 : lag
      end

      # @return [Integer, nil] nil means skip partition (nothing pending / unknown)
      def start_offset_for(partition, advance, explicit_start, meta, low, high)
        if advance.key?(partition.to_s) || advance.key?(partition)
          return (advance[partition.to_s] || advance[partition]).to_i + 1
        end
        if explicit_start
          return [[explicit_start, low].max, high].min
        end

        committed = meta[:offset]
        if committed && committed >= 0
          # Kafka committed = next offset to fetch → pending is offset >= committed.
          [[committed, low].max, high].min
        else
          # never-consumed / unknown: lag page shows 0 — do not dump log history.
          nil
        end
      end

      def pending_floor(meta, explicit_start, _low)
        return explicit_start if explicit_start
        return meta[:offset] if meta[:offset] && meta[:offset] >= 0

        nil
      end

      # True when any partition still has broker messages after the last-read cursor.
      def more_remaining?(topic, parts, advance, explicit_start, commits, highs)
        parts.any? do |p|
          high = highs[p]
          unless high
            begin
              _low, high = consumer.query_watermark_offsets(topic, p, POLL_TIMEOUT_MS)
            rescue StandardError
              next false
            end
          end
          next false if high.to_i <= 0

          meta = commits[p] || {}
          last = advance[p.to_s] || advance[p]
          next_off =
            if last
              last.to_i + 1
            elsif explicit_start
              explicit_start.to_i
            elsif meta[:offset] && meta[:offset] >= 0 && admin_lag(meta) > 0
              meta[:offset].to_i
            else
              next false
            end
          next_off < high.to_i
        end
      end

      # @return [Hash{Integer => Hash}] partition => { offset:, lag:, never_consumed: }
      def committed_meta(group, topic)
        out = {}
        return out unless KafkaBatch::Lag.available?

        data = KafkaBatch::Lag.read_group(group, [topic])
        parts = dig_group_topic(data, group, topic)
        parts.each do |partition, info|
          info = info.is_a?(Hash) ? info : {}
          off = (info[:offset] || info["offset"]).to_i
          lag = (info[:lag] || info["lag"]).to_i
          if off.negative?
            out[partition.to_i] = { offset: nil, lag: 0, never_consumed: true }
          else
            out[partition.to_i] = { offset: off, lag: lag.negative? ? 0 : lag, never_consumed: false }
          end
        end
        out
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch::Browse::Reader] lag #{group}: #{e.message}")
        {}
      end

      def dig_group_topic(data, group, topic)
        return {} unless data.is_a?(Hash)

        topics = data[group] || data[group.to_s] || data[group.to_sym]
        return {} unless topics.is_a?(Hash)

        parts = topics[topic] || topics[topic.to_s] || topics[topic.to_sym]
        parts.is_a?(Hash) ? parts : {}
      end

      def read_forward(topic, partition, start_offset, end_offset, found, limit, floor: nil)
        return if limit <= 0

        assign_at(topic, partition, start_offset)
        offset = start_offset
        while offset <= end_offset && found.size < limit
          msg = consumer.poll(POLL_TIMEOUT_MS)
          break if msg.nil?
          next unless msg.topic == topic && msg.partition == partition
          break if msg.offset > end_offset

          offset = msg.offset + 1
          # Hard floor: never surface already-processed offsets.
          next if floor && msg.offset < floor

          row = decode_message(msg)
          next if row.nil?

          found << row
          yield row if block_given?
        end
      rescue Rdkafka::RdkafkaError => e
        KafkaBatch.logger.warn(
          "[KafkaBatch::Browse::Reader] #{topic}/#{partition} read error: #{e.message}"
        )
      ensure
        unassign
      end

      def decode_message(msg)
        data = parse_payload(msg.payload)
        {
          topic: msg.topic,
          partition: msg.partition,
          offset: msg.offset,
          timestamp: msg.timestamp&.to_i,
          job_id: data["job_id"].to_s,
          batch_id: data["batch_id"],
          job_type: data["job_type"] || data["worker_class"],
          worker_class: data["worker_class"],
          tenant_id: data["tenant_id"],
          attempt: data["attempt"],
          payload_preview: preview_payload(data),
          payload_bytes: msg.payload.to_s.bytesize
        }
      end

      def preview_payload(data)
        # Prefer args/kwargs (job body); fall back to compact metadata JSON.
        body =
          if data.key?("args") || data.key?("kwargs")
            { "args" => data["args"], "kwargs" => data["kwargs"] }.compact
          else
            data.reject { |k, _| %w[payload job message body].include?(k.to_s) }
          end
        json = Oj.dump(body, mode: :compat)
        return json if json.bytesize <= PAYLOAD_MAX_CHARS

        "#{json.byteslice(0, PAYLOAD_MAX_CHARS)}…(truncated)"
      rescue StandardError
        "{}"
      end

      def parse_payload(raw)
        Oj.load(raw, mode: :compat)
      rescue StandardError
        { "_raw" => raw.to_s.byteslice(0, 200) }
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
          "group.id" => "#{cfg.consumer_group}-browse-reader",
          "enable.auto.commit" => false,
          "auto.offset.reset" => "error",
          "enable.partition.eof" => true
        }
        overrides = (cfg.consumer_config || {}).each_with_object({}) do |(k, v), h|
          h[k.to_s] = v
        end
        Rdkafka::Config.new(base.merge(overrides)).consumer
      end
    end
  end
end
