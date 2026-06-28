require "logger"
require "oj"

require_relative "kafka_batch/version"
require_relative "kafka_batch/errors"
require_relative "kafka_batch/configuration"
require_relative "kafka_batch/backoff"
require_relative "kafka_batch/instrumentation"
require_relative "kafka_batch/stores/base"
require_relative "kafka_batch/stores/mysql_store"
require_relative "kafka_batch/stores/redis_store"
require_relative "kafka_batch/producer"
require_relative "kafka_batch/cancellation_cache"
require_relative "kafka_batch/liveness"
require_relative "kafka_batch/lag"
require_relative "kafka_batch/fairness/scheduler"
require_relative "kafka_batch/fairness/dispatcher"
require_relative "kafka_batch/worker"
require_relative "kafka_batch/batch"
require_relative "kafka_batch/reconciler"
require_relative "kafka_batch/consumers/job_consumer"
require_relative "kafka_batch/consumers/retry_consumer"
require_relative "kafka_batch/consumers/event_consumer"
require_relative "kafka_batch/consumers/callback_consumer"
require_relative "kafka_batch/web"

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

    # Identifier for THIS process/pod, used to record which consumer ran a
    # batch's callbacks. Prefers the K8s pod name (ENV["HOSTNAME"]) and falls
    # back to the OS hostname; suffixed with the PID to disambiguate workers.
    def node_id
      @node_id ||= begin
        require "socket"
        host = ENV["HOSTNAME"]
        host = Socket.gethostname if host.nil? || host.empty?
        "#{host}##{Process.pid}"
      end
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

    # ── Fairness scheduler (multi-tenant WFQ) ───────────────────────────────

    # Optional Redis-backed virtual-time WFQ scheduler for STRICT weighted shares.
    # NOT used by the default fairness path (the Dispatcher needs no Redis) — it's
    # a standalone engine to build a custom dispatcher/worker around.
    # @return [Fairness::Scheduler]
    def fairness_scheduler
      @fairness_scheduler ||= Fairness::Scheduler.new
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
    # Call this INSIDE your karafka.rb routes.draw block, passing `self` (the
    # routing builder). Make sure your worker classes are loaded first so they
    # are registered (reference them, or eager-load):
    #
    #   class KarafkaApp < Karafka::App
    #     routes.draw do
    #       MyWorker  # ensure workers are loaded/registered
    #       KafkaBatch.draw_routes(self)
    #       # ... your own routes
    #     end
    #   end
    #
    # It creates TWO consumer groups so the control plane (events/callbacks/
    # retry) is isolated from job execution and isn't blocked behind long jobs:
    #   "<consumer_group>-control" – events + callbacks + retry
    #   "<consumer_group>-jobs"    – one topic per registered worker
    #
    # With config.concurrency > 1 (recommended), control messages are then
    # worked in parallel with jobs, so progress/callbacks propagate promptly.
    def draw_routes(builder)
      cfg     = config
      workers = KafkaBatch.workers

      # Karafka's routing DSL methods (consumer_group/topic/consumer) are private
      # and only resolve with implicit self, so define routes inside the builder
      # via instance_eval. Locals (cfg/workers) remain available via closure.
      fairness = cfg.fairness_enabled

      builder.instance_eval do
        consumer_group "#{cfg.consumer_group}-control" do
          topic(cfg.events_topic)    { consumer KafkaBatch::Consumers::EventConsumer }
          topic(cfg.callbacks_topic) { consumer KafkaBatch::Consumers::CallbackConsumer }
          topic(cfg.retry_topic)     { consumer KafkaBatch::Consumers::RetryConsumer }
        end

        if fairness
          # Fairness mode: jobs land on the ingest topic; the Dispatcher forwards
          # them (throttled) onto the ready topic, which the JobConsumer swarm
          # drains. No Redis or extra process on the path.
          consumer_group "#{cfg.consumer_group}-dispatch" do
            topic(cfg.fairness_ingest_topic) { consumer KafkaBatch::Fairness::Dispatcher }
          end
          consumer_group "#{cfg.consumer_group}-jobs" do
            topic(cfg.fairness_ready_topic) { consumer KafkaBatch::Consumers::JobConsumer }
          end
        elsif !workers.empty?
          consumer_group "#{cfg.consumer_group}-jobs" do
            workers.each do |worker_class|
              topic(worker_class.kafka_topic) { consumer KafkaBatch::Consumers::JobConsumer }
            end
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

      validate_fairness_partitions!(strict: true)
    end

    # Number of partitions on the fairness ingest topic, or nil if it can't be
    # determined (Karafka::Admin unavailable / cluster unreachable / topic missing).
    def fairness_ingest_partition_count
      return nil unless defined?(Karafka) && defined?(Karafka::Admin)

      topic = Karafka::Admin.cluster_info.topics
                            .find { |t| t[:topic_name] == config.fairness_ingest_topic }
      topic && topic[:partition_count]
    rescue => e
      logger.warn("[KafkaBatch] could not read partition count for '#{config.fairness_ingest_topic}': #{e.message}")
      nil
    end

    # Warn (or raise, when strict) if the fairness ingest topic has too few
    # partitions. Tenants are spread across partitions by key hash, so too few
    # means tenants collide onto one partition and fairness degrades — a single
    # partition gives no fairness at all. No-op unless fairness is enabled.
    # @param strict [Boolean] raise ConfigurationError instead of warning
    def validate_fairness_partitions!(strict: config.validate_topics_on_boot)
      return unless config.fairness_enabled

      count = fairness_ingest_partition_count
      return if count.nil?  # couldn't determine — don't false-alarm

      min = [config.fairness_min_ingest_partitions.to_i, 2].max
      return if count >= min

      msg = "[KafkaBatch] fairness_enabled but ingest topic '#{config.fairness_ingest_topic}' has " \
            "#{count} partition(s) (recommended >= #{min}). Tenants are hashed to partitions, so too " \
            "few means tenants share a partition (1 = no fairness at all). Recreate the topic with more " \
            "partitions (≈ your max concurrent tenant count)."

      raise ConfigurationError, msg if strict

      logger.warn(msg)
    end

    # ── Logging ────────────────────────────────────────────────────────────

    def logger
      config.logger
    end

    # ── Reset (for tests) ─────────────────────────────────────────────────

    def reset!
      @configuration      = nil
      @store              = nil
      @workers            = []
      @store_mutex        = nil
      @workers_mutex      = nil
      @fairness_scheduler = nil
      @node_id            = nil
      Producer.reset!
      CancellationCache.reset!
      Liveness.reset!
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
