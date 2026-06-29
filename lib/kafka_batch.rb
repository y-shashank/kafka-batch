# frozen_string_literal: true

# Full backend entry point — loads everything: UI layer + workers, consumers,
# producer, batch, reconciler, topics, fairness.
# Use this in processes that run Karafka consumers (the worker service).
#
# For web-only processes that only mount the dashboard, use:
#   require "kafka_batch/ui"

require "oj"
require_relative "kafka_batch/ui"    # config, store, lag, liveness, web, …

# ── Backend-only requires ────────────────────────────────────────────────────
require_relative "kafka_batch/producer"
require_relative "kafka_batch/consumers/consumption_gate"
require_relative "kafka_batch/topics"
require_relative "kafka_batch/fairness/scheduler"
require_relative "kafka_batch/fairness/dispatcher"
require_relative "kafka_batch/worker"
require_relative "kafka_batch/batch"
require_relative "kafka_batch/reconciler"
require_relative "kafka_batch/consumers/job_consumer"
require_relative "kafka_batch/consumers/priority_gate"
require_relative "kafka_batch/consumers/fast_p0_consumer"
require_relative "kafka_batch/consumers/fast_p1_consumer"
require_relative "kafka_batch/consumers/slow_p0_consumer"
require_relative "kafka_batch/consumers/slow_p1_consumer"
require_relative "kafka_batch/consumers/retry_consumer"
require_relative "kafka_batch/consumers/event_consumer"
require_relative "kafka_batch/consumers/callback_consumer"

module KafkaBatch
  class << self
    # ── Fairness scheduler ─────────────────────────────────────────────────

    # Optional Redis-backed WFQ scheduler. Not used by the default fairness
    # path — standalone engine for custom dispatchers.
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
    def workers
      workers_mutex.synchronize { Array(@workers) }
    end

    # True if any registered worker opts into the multi-tenant fair lane.
    def fairness?
      workers.any?(&:fairness?)
    end

    # ── Consumer groups ────────────────────────────────────────────────────
    # Returns the groups that exist given the current worker registry.
    # Used by draw_routes and (legacy) lag page fallback.

    def consumer_groups
      groups   = [control_consumer_group]
      groups << dispatch_consumer_group << jobs_fair_consumer_group if fairness?

      plain    = workers.reject(&:fairness?)
      fast_set = [config.fast_p0_topic, config.fast_p1_topic]
      slow_set = [config.slow_p0_topic, config.slow_p1_topic]

      groups << fast_consumer_group if plain.any? { |w| fast_set.include?(w.kafka_topic) }
      groups << slow_consumer_group if plain.any? { |w| slow_set.include?(w.kafka_topic) }

      other = plain.reject { |w| (fast_set + slow_set).include?(w.kafka_topic) }
      groups << jobs_consumer_group unless other.empty?

      groups
    end

    # ── Karafka routing ────────────────────────────────────────────────────

    def draw_routes(builder)
      cfg          = config
      all_workers  = KafkaBatch.workers
      fair_workers = all_workers.select(&:fairness?)
      plain_topics = all_workers.reject(&:fairness?).map(&:kafka_topic).uniq
      any_fair     = fair_workers.any?

      fast_set = [cfg.fast_p0_topic, cfg.fast_p1_topic]
      slow_set = [cfg.slow_p0_topic, cfg.slow_p1_topic]
      all_prio = fast_set + slow_set

      fast_p0_used = plain_topics.include?(cfg.fast_p0_topic)
      fast_p1_used = plain_topics.include?(cfg.fast_p1_topic)
      slow_p0_used = plain_topics.include?(cfg.slow_p0_topic)
      slow_p1_used = plain_topics.include?(cfg.slow_p1_topic)
      any_fast     = fast_p0_used || fast_p1_used
      any_slow     = slow_p0_used || slow_p1_used

      other_plain_topics = plain_topics.reject { |t| all_prio.include?(t) }

      builder.instance_eval do
        consumer_group "#{cfg.consumer_group}-control" do
          topic(cfg.events_topic)    { consumer KafkaBatch::Consumers::EventConsumer }
          topic(cfg.callbacks_topic) { consumer KafkaBatch::Consumers::CallbackConsumer }
          cfg.retry_topics.each do |retry_topic|
            topic(retry_topic) { consumer KafkaBatch::Consumers::RetryConsumer }
          end
        end

        if any_fair
          consumer_group "#{cfg.consumer_group}-dispatch" do
            topic(cfg.fairness_ingest_topic) { consumer KafkaBatch::Fairness::Dispatcher }
          end
          consumer_group "#{cfg.consumer_group}-jobs-fair" do
            topic(cfg.fairness_ready_topic) { consumer KafkaBatch::Consumers::JobConsumer }
          end
        end

        if any_fast
          consumer_group "#{cfg.consumer_group}-jobs-fast" do
            topic(cfg.fast_p0_topic) { consumer KafkaBatch::Consumers::FastP0Consumer } if fast_p0_used
            topic(cfg.fast_p1_topic) { consumer KafkaBatch::Consumers::FastP1Consumer } if fast_p1_used
          end
        end

        if any_slow
          consumer_group "#{cfg.consumer_group}-jobs-slow" do
            topic(cfg.slow_p0_topic) { consumer KafkaBatch::Consumers::SlowP0Consumer } if slow_p0_used
            topic(cfg.slow_p1_topic) { consumer KafkaBatch::Consumers::SlowP1Consumer } if slow_p1_used
          end
        end

        unless other_plain_topics.empty?
          consumer_group "#{cfg.consumer_group}-jobs" do
            other_plain_topics.uniq.each do |job_topic|
              topic(job_topic) { consumer KafkaBatch::Consumers::JobConsumer }
            end
          end
        end
      end
    end

    # ── Topic validation ───────────────────────────────────────────────────

    def validate_topics!
      required = KafkaBatch::Topics.specs.map { |s| s[:name] }.compact.uniq

      existing = begin
        producer   = KafkaBatch::Producer.instance
        rd_handle  = producer.respond_to?(:client) ? producer.client : nil
        rd_handle.respond_to?(:metadata) ? rd_handle.metadata(true, nil, 5000).topics.map(&:topic) : nil
      rescue => e
        logger.warn("[KafkaBatch] validate_topics!: could not fetch topic list: #{e.message}")
        nil
      end

      return if existing.nil?

      missing = required - existing
      unless missing.empty?
        raise ConfigurationError,
          "The following Kafka topics do not exist: #{missing.join(', ')}. " \
          "Create them or set config.validate_topics_on_boot = false to suppress this check."
      end

      logger.info("[KafkaBatch] All #{required.size} required topics verified.")
      validate_fairness_partitions!(strict: true)
    end

    def validate_fairness_partitions!(strict: config.validate_topics_on_boot)
      return unless fairness?
      count = fairness_ingest_partition_count
      return if count.nil?

      min = [config.fairness_min_ingest_partitions.to_i, 2].max
      return if count >= min

      msg = "[KafkaBatch] a worker opts into fairness but ingest topic " \
            "'#{config.fairness_ingest_topic}' has #{count} partition(s) " \
            "(recommended >= #{min}). Recreate the topic with more partitions."

      raise ConfigurationError, msg if strict
      logger.warn(msg)
    end

    # ── Reset (full — overrides core.rb's minimal version) ────────────────

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
      ConsumptionControl.reset!
    end

    private

    def workers_mutex
      @workers_mutex ||= Mutex.new
    end
  end
end

# Load Rails integration if Rails is available.
# (already loaded by kafka_batch/ui.rb if Rails is present; this is a no-op
# if the railtie has already been required.)
require_relative "kafka_batch/railtie" if defined?(Rails::Railtie)
