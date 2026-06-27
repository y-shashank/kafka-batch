module KafkaBatch
  # Include this module in any class that should act as a Kafka-batch worker.
  #
  # Example:
  #
  #   class ProcessOrderWorker
  #     include KafkaBatch::Worker
  #
  #     kafka_topic "orders.process"
  #     max_retries 5
  #     retry_backoff 10   # seconds (optional, overrides global default)
  #
  #     def perform(payload)
  #       Order.find(payload["order_id"]).process!
  #     end
  #   end
  #
  # The worker is registered automatically on include and will be picked up
  # by KafkaBatch.draw_routes when you set up Karafka routing.
  module Worker
    def self.included(base)
      base.extend(ClassMethods)
      KafkaBatch.register_worker(base)
    end

    module ClassMethods
      # Kafka topic this worker consumes from.
      def kafka_topic(name = nil)
        if name
          @kafka_topic = name.to_s
        else
          @kafka_topic || raise(ConfigurationError, "#{self}.kafka_topic is not set")
        end
      end

      # Maximum retry attempts before a job is sent to the DLT.
      # Defaults to KafkaBatch.config.max_retries.
      def max_retries(n = nil)
        if n
          @max_retries = n.to_i
        else
          @max_retries || KafkaBatch.config.max_retries
        end
      end

      # Per-retry backoff in seconds.
      # Defaults to KafkaBatch.config.retry_backoff.
      def retry_backoff(n = nil)
        if n
          @retry_backoff = n.to_i
        else
          @retry_backoff || KafkaBatch.config.retry_backoff
        end
      end
    end

    # Set by JobConsumer before #perform so a running job knows its batch.
    attr_accessor :kafka_batch_id

    # The batch this job belongs to, as a pushable Batch handle – or nil for a
    # standalone job. Lets a running job add more jobs to its own (open) batch
    # without threading the batch id around:
    #
    #   def perform(payload)
    #     batch&.push(ChildWorker, "parent_id" => payload["id"])
    #   end
    #
    # @return [KafkaBatch::Batch, nil]
    def batch
      return nil if kafka_batch_id.nil? || kafka_batch_id.to_s.empty?
      @kafka_batch ||= KafkaBatch::Batch.new(id: kafka_batch_id)
    end

    # Override this in your worker class.
    # @param payload [Hash] deserialized message payload
    def perform(payload)
      raise NotImplementedError, "#{self.class}#perform must be implemented"
    end
  end
end
