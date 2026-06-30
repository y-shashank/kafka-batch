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
      # @param key     [String, nil]   optional partition key
      # @param headers [Hash]          optional Kafka headers
      def produce_sync(topic:, payload:, key: nil, headers: {})
        instance.produce_sync(
          topic:   topic,
          payload: encode(payload),
          key:     key&.to_s,
          headers: headers
        )
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
      # @param messages [Array<Hash>] each with keys: :topic, :payload, :key (opt), :headers (opt)
      def produce_many_sync(messages)
        encoded = messages.map do |m|
          {
            topic:   m[:topic],
            payload: encode(m[:payload]),
            key:     m[:key]&.to_s,
            headers: m[:headers] || {}
          }
        end
        instance.produce_many_sync(encoded)
      rescue WaterDrop::Errors::BaseError, Rdkafka::RdkafkaError => e
        raise KafkaBatch::ProducerError, "Kafka produce failed: #{e.message}"
      end

      # Close and reset the producer (e.g. in tests or after fork).
      def reset!
        @mutex.synchronize do
          @instance&.close
          @instance = nil
        end
      end

      private

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
          :"socket.nagle.disable"     => true
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
