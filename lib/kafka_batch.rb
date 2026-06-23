require "logger"
require "oj"

require_relative "kafka_batch/version"
require_relative "kafka_batch/errors"
require_relative "kafka_batch/configuration"
require_relative "kafka_batch/instrumentation"
require_relative "kafka_batch/stores/base"
require_relative "kafka_batch/stores/mysql_store"
require_relative "kafka_batch/stores/redis_store"
require_relative "kafka_batch/producer"
require_relative "kafka_batch/worker"
require_relative "kafka_batch/batch"
require_relative "kafka_batch/reconciler"
require_relative "kafka_batch/consumers/job_consumer"
require_relative "kafka_batch/consumers/retry_consumer"
require_relative "kafka_batch/consumers/event_consumer"
require_relative "kafka_batch/consumers/callback_consumer"

module KafkaBatch
  class << self
    # ── Configuration ─────────────────────────────────────────────────────

    def configuration
      @configuration ||= Configuration.new
    end
    alias config configuration

    def configure
      yield configuration
    end

    # ── Store ──────────────────────────────────────────────────────────────

    # Returns the configured store singleton.
    # Thread-safe via double-checked locking.
    # @return [Stores::MysqlStore, Stores::RedisStore]
    def store
      return @store if @store
      store_mutex.synchronize do
        @store ||= begin
          config.validate!
          case config.store
          when :mysql
            Stores::MysqlStore.new
          when :redis
            Stores::RedisStore.new
          else
            raise ConfigurationError, "Unknown store: #{config.store}"
          end
        end
      end
    end

    # ── Worker registry ────────────────────────────────────────────────────

    # Called automatically when a class includes KafkaBatch::Worker.
    def register_worker(klass)
      workers_mutex.synchronize do
        @workers ||= []
        @workers << klass unless @workers.include?(klass)
      end
    end

    # All registered worker classes.
    # @return [Array<Class>]
    def workers
      workers_mutex.synchronize { Array(@workers) }
    end

    # ── Karafka routing helper ─────────────────────────────────────────────
    #
    # Call this inside your karafka.rb routes.draw block:
    #
    #   class KarafkaApp < Karafka::App
    #     routes.draw do
    #       KafkaBatch.draw_routes(self)
    #       # ... your own routes
    #     end
    #   end
    #
    def draw_routes(karafka_app)
      cfg = config

      karafka_app.routes.draw do
        # ── Internal topics ────────────────────────────────────────────────
        topic cfg.events_topic do
          consumer KafkaBatch::Consumers::EventConsumer
          group_id "#{cfg.consumer_group}-events"
        end

        topic cfg.callbacks_topic do
          consumer KafkaBatch::Consumers::CallbackConsumer
          group_id "#{cfg.consumer_group}-callbacks"
        end

        # Dedicated retry topic: RetryConsumer waits via Karafka pause()
        # then re-enqueues to the original job topic, keeping JobConsumer
        # partitions fully unblocked during backoff.
        topic cfg.retry_topic do
          consumer KafkaBatch::Consumers::RetryConsumer
          group_id "#{cfg.consumer_group}-retry"
        end

        # ── One consumer route per registered worker ────────────────────────
        KafkaBatch.workers.each do |worker_class|
          topic worker_class.kafka_topic do
            consumer KafkaBatch::Consumers::JobConsumer
            group_id "#{cfg.consumer_group}-jobs"
          end
        end
      end
    end

    # ── Topic validation ───────────────────────────────────────────────────

    # Verify that all KafkaBatch topics exist in the Kafka cluster.
    # Called at boot when config.validate_topics_on_boot = true.
    # Raises ConfigurationError with a list of missing topics.
    def validate_topics!
      required = [
        config.jobs_topic,
        config.events_topic,
        config.callbacks_topic,
        config.retry_topic,
        config.dead_letter_topic
      ].compact.uniq

      # Attempt to list topics via WaterDrop's internal Rdkafka handle
      existing = begin
        producer   = KafkaBatch::Producer.instance
        rd_handle  = producer.respond_to?(:client) ? producer.client : nil
        if rd_handle.respond_to?(:metadata)
          rd_handle.metadata(true, nil, 5000).topics.map(&:topic)
        else
          nil  # can't introspect – skip
        end
      rescue => e
        logger.warn("[KafkaBatch] validate_topics!: could not fetch topic list: #{e.message}")
        nil
      end

      return if existing.nil?  # skip if we couldn't fetch

      missing = required - existing
      unless missing.empty?
        raise ConfigurationError,
          "The following Kafka topics do not exist: #{missing.join(', ')}. " \
          "Create them or set config.validate_topics_on_boot = false to suppress this check."
      end

      logger.info("[KafkaBatch] All #{required.size} required topics verified.")
    end

    # ── Logging ────────────────────────────────────────────────────────────

    def logger
      config.logger
    end

    # ── Reset (for tests) ─────────────────────────────────────────────────

    def reset!
      @configuration = nil
      @store         = nil
      @workers       = []
      @store_mutex   = nil
      @workers_mutex = nil
      Producer.reset!
    end

    private

    def store_mutex
      @store_mutex ||= Mutex.new
    end

    def workers_mutex
      @workers_mutex ||= Mutex.new
    end
  end
end

# Load Rails integration if Rails is available
require_relative "kafka_batch/railtie" if defined?(Rails::Railtie)
