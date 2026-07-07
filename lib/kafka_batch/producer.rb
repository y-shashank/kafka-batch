require "waterdrop"
require "oj"

module KafkaBatch
  # Thread-safe, singleton WaterDrop producer.
  # Wraps WaterDrop::Producer with a synchronous produce helper and
  # auto-initialisation from KafkaBatch.config.
  module Producer
    class << self
      # @return [WaterDrop::Producer]
      def instance
        @instance || @mutex.synchronize { @instance ||= build }
      end

      # Produce a single message synchronously.
      # Blocks until the broker acknowledges delivery (acks: all).
      #
      # @param topic   [String]
      # @param payload [Hash, String]
      # @param key       [String, nil]   partition key (murmur2_random hash)
      # @param partition [Integer, nil]  explicit partition number; skips key-hash when set
      # @param headers   [Hash]          optional Kafka headers
      def produce_sync(topic:, payload:, key: nil, partition: nil, headers: {})
        msg = encode_message(
          topic: topic, payload: payload, key: key, partition: partition, headers: headers
        )
        instance.produce_sync(**msg)
      rescue WaterDrop::Errors::BaseError, Rdkafka::RdkafkaError => e
        raise KafkaBatch::ProducerError, "Kafka produce failed: #{e.message}"
      end

      # Produce multiple messages in one shot and wait for all ACKs.
      #
      # Internally WaterDrop calls produce_async for each message, collecting
      # delivery handles, then awaits them all. librdkafka pipelines the sends
      # into one or a few Kafka MessageSets — the same throughput win as
      # Sidekiq's push_bulk, without the per-message round-trip overhead of
      # calling produce_sync in a loop.
      #
      # @param messages [Array<Hash>] each with keys: :topic, :payload,
      #                              :key (opt), :partition (opt), :headers (opt)
      #   When :partition is set, :key is ignored for that message.
      # @return [Array<Rdkafka::Producer::DeliveryHandle>]
      # @raise [KafkaBatch::PartialProduceError] on partial bulk failure
      def produce_many_sync(messages)
        encoded = encode_messages(messages)
        instance.produce_many_sync(encoded)
      rescue WaterDrop::Errors::ProduceManyError => e
        raise KafkaBatch::PartialProduceError.new(
          "Kafka bulk produce failed: #{e.message}",
          dispatched: e.dispatched
        )
      rescue WaterDrop::Errors::BaseError, Rdkafka::RdkafkaError => e
        raise KafkaBatch::ProducerError, "Kafka produce failed: #{e.message}"
      end

      # Count consecutive successfully delivered messages from the start of
      # +handles+ (WaterDrop delivery handles in enqueue order). Stops at the
      # first handle whose delivery report indicates failure.
      def prefix_delivered_count(handles)
        return 0 if handles.nil? || handles.empty?

        count = 0
        handles.each do |handle|
          report = handle.respond_to?(:create_result) ? handle.create_result : handle
          break unless delivery_report_ok?(report)

          count += 1
        rescue StandardError
          break
        end
        count
      end

      # Close and reset the producer (e.g. in tests or after fork).
      def reset!
        @mutex.synchronize do
          @instance&.close
          @instance = nil
        end
      end

      private

      def encode_messages(messages)
        messages.map { |m| encode_message(**m) }
      end

      def encode_message(topic:, payload:, key: nil, partition: nil, headers: {})
        msg = {
          topic:   topic,
          payload: encode(payload),
          headers: headers || {}
        }
        if !partition.nil?
          msg[:partition] = partition
        else
          msg[:key] = key&.to_s
        end
        msg
      end

      def delivery_report_ok?(report)
        return false unless report

        if report.respond_to?(:error)
          err = report.error
          return false if err && err.respond_to?(:null?) && !err.null?
          return false if err && !err.respond_to?(:null?)
        end

        report.respond_to?(:partition) && report.respond_to?(:offset)
      end

      def build
        cfg = KafkaBatch.config

        # WaterDrop requires every key under the `kafka` scope to be a symbol.
        # Normalise both the defaults and any user overrides to symbols so that,
        # e.g., a user-supplied "bootstrap.servers" actually replaces our
        # default rather than producing two differently-typed keys.
        defaults = {
          :"bootstrap.servers"        => cfg.brokers.join(","),
          :"request.required.acks"    => "all",   # strongest durability guarantee
          # Idempotent producer: dedups broker-side produce retries so the same
          # message can never be appended twice (different offsets) on a topic.
          # This is what makes offset-based completion counting safe – the worker
          # topic itself is guaranteed free of produce-retry duplicates.
          :"enable.idempotence"       => true,
          :"retry.backoff.ms"         => 200,
          :"socket.timeout.ms"        => 30_000,
          :"message.timeout.ms"       => 30_000,
          # ── Throughput / latency tuning ──────────────────────────────────────
          # Send as soon as messages are queued (no artificial linger). With
          # produce_many_sync all messages land in the librdkafka queue at once,
          # so they are already batched; no linger needed for throughput.
          :"queue.buffering.max.ms"   => 5,
          # Disable Nagle's algorithm: flush TCP segments immediately rather than
          # waiting to coalesce small packets. Meaningfully reduces per-produce
          # round-trip latency on LAN connections.
          :"socket.nagle.disable"     => true,
          # Use murmur2_random to match the Java Kafka producer's partitioning
          # algorithm. librdkafka's default (consistent_random) uses CRC32, which
          # produces DIFFERENT partition assignments for the same key. The fairness
          # ingest topic keys messages by tenant_id so each tenant lands on a fixed
          # partition — the dashboard's ingest-partition lookup widget also uses
          # murmur2, so both must agree or the widget shows the wrong partition.
          :"partitioner"              => "murmur2_random"
        }
        overrides = (cfg.producer_config || {}).each_with_object({}) do |(k, v), h|
          h[k.to_sym] = v
        end

        WaterDrop::Producer.new do |config|
          config.deliver = true
          config.kafka   = defaults.merge(overrides)
          config.logger  = cfg.logger
        end
      end

      # Serialize +payload+ to a UTF-8 string and optionally enforce a maximum
      # byte size so callers get a clear KafkaBatch::ProducerError instead of an
      # opaque rdkafka / WaterDrop error when the message exceeds broker limits.
      #
      # #21 fix: guard against oversized payloads before handing off to librdkafka.
      def encode(payload)
        encoded = payload.is_a?(String) ? payload : Oj.dump(payload, mode: :compat)
        max = KafkaBatch.config.max_message_bytes
        if max && max > 0 && encoded.bytesize > max
          raise KafkaBatch::ProducerError,
                "Payload too large: #{encoded.bytesize} bytes exceeds " \
                "config.max_message_bytes (#{max}). Reduce payload size or raise the limit."
        end
        encoded
      end
    end

    @mutex = Mutex.new
  end
end
