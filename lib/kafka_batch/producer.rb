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
        # WaterDrop::Errors::BaseError is the superclass of every WaterDrop
        # producer error, so this stays correct across WaterDrop versions
        # (older releases exposed differently-named subclasses).
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
          :"message.timeout.ms"       => 30_000
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

      def encode(payload)
        payload.is_a?(String) ? payload : Oj.dump(payload, mode: :compat)
      end
    end

    @mutex = Mutex.new
  end
end
