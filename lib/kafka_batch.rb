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
require_relative "kafka_batch/fairness/forwarder"
require_relative "kafka_batch/fairness/dispatcher"
require_relative "kafka_batch/worker"
require_relative "kafka_batch/batch"
require_relative "kafka_batch/schedule_poller"
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

    # Redis-backed WFQ scheduler for a fairness lane (:time | :throughput).
    # Alias of KafkaBatch.scheduler(type) (defined in core.rb with double-checked
    # locking + redis guard). Kept here for backwards-compatibility.
    def fairness_scheduler(type = :time)
      scheduler(type)
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

    # True if any registered worker opts into a multi-tenant fair lane.
    def fairness?
      workers.any?(&:fairness?)
    end

    # The fairness lanes actually in use, based on registered workers'
    # fairness_type. e.g. [:time], [:throughput], or [:time, :throughput].
    # @return [Array<Symbol>]
    def active_fairness_types
      workers.select(&:fairness?).map(&:fairness_type).uniq
    end

    # ── Consumer groups ────────────────────────────────────────────────────
    # Returns the groups that exist given the current worker registry.
    # Used by draw_routes and (legacy) lag page fallback.

    def consumer_groups
      groups   = [control_consumer_group]
      active_fairness_types.each do |ft|
        groups << dispatch_consumer_group(ft) << jobs_fair_consumer_group(ft)
      end

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
      # Fairness lanes actually in use (a lane's topics/consumers are only wired
      # when at least one worker opts into it). Captured as a local so it is
      # visible inside the builder.instance_eval closure below.
      active_fair_types = active_fairness_types

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

        # Each fairness lane gets its OWN dispatch + jobs-fair consumer groups
        # (…-dispatch-<lane> / …-jobs-fair-<lane>), so a lane can be isolated in its
        # own process — dedicated thread pool, dispatcher, and forwarder — via
        # `karafka server --include-consumer-groups <cg>-jobs-fair-time` etc. The
        # Dispatcher still derives its lane from the ingest topic name at runtime.
        active_fair_types.each do |ft|
          consumer_group "#{cfg.consumer_group}-dispatch-#{ft}" do
            # Karafka OSS concurrency is global (Karafka::App.config.concurrency).
            # On a dispatch process, set it >= config.fairness_dispatcher_concurrency
            # so multiple ingest partitions can forward in parallel.
            topic(cfg.fairness_ingest_topic(ft)) do
              consumer KafkaBatch::Fairness::Dispatcher
              # Bound how many ingest messages the Dispatcher drains into the
              # Redis WFQ window per consume call. Fairness ordering is done by
              # the Scheduler/Forwarder, not by this batch size.
              max_messages cfg.fairness_dispatcher_batch_size
            end
          end
          consumer_group "#{cfg.consumer_group}-jobs-fair-#{ft}" do
            topic(cfg.fairness_ready_topic(ft)) { consumer KafkaBatch::Consumers::JobConsumer }
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
      warn_dispatcher_concurrency!
    end

    # Karafka concurrency is a top-level app setting (not settable per consumer
    # group from within draw_routes). Warn at boot if it looks too low for the
    # dispatch group to forward all active partitions in parallel.
    def warn_dispatcher_concurrency!
      return unless fairness?
      return unless defined?(Karafka::App)

      karafka_concurrency =
        begin
          Karafka::App.config.concurrency.to_i
        rescue StandardError
          nil
        end
      return if karafka_concurrency.nil?

      needed = config.fairness_dispatcher_concurrency.to_i
      return if karafka_concurrency >= needed

      logger.warn(
        "[KafkaBatch] Karafka concurrency=#{karafka_concurrency} is lower than " \
        "config.fairness_dispatcher_concurrency=#{needed}. " \
        "Ingest partitions will be processed sequentially instead of in parallel, " \
        "which breaks per-tenant fairness under load. " \
        "Set `config.concurrency = #{needed}` (or higher) in your karafka.rb."
      )
    end

    def validate_fairness_partitions!(strict: config.validate_topics_on_boot)
      return unless fairness?
      min = [config.fairness_min_ingest_partitions.to_i, 2].max

      # Check each active lane's ingest topic independently.
      active_fairness_types.each do |type|
        count = fairness_ingest_partition_count(type)
        next if count.nil? || count >= min

        msg = "[KafkaBatch] a worker uses the #{type} fairness lane but ingest topic " \
              "'#{config.fairness_ingest_topic(type)}' has #{count} partition(s) " \
              "(recommended >= #{min}). Recreate the topic with more partitions."

        raise ConfigurationError, msg if strict
        logger.warn(msg)
      end
    end

    # ── Reset (full — overrides core.rb's minimal version) ────────────────

    def reset!
      @configuration   = nil
      @store           = nil
      @schedulers      = nil   # per-lane, shared with core.rb via fairness_scheduler alias
      @ingest_partition_count_cache = nil
      @schedule_store_instance = nil
      @schedule_store_mutex    = nil
      @workers         = []
      @store_mutex     = nil
      @workers_mutex   = nil
      @scheduler_mutex = nil
      @node_id         = nil
      Producer.reset!
      CancellationCache.reset!
      Liveness.reset!
      ConsumptionControl.reset!
      Fairness::Forwarder.stop! if defined?(Fairness::Forwarder)
      SchedulePoller.stop! if defined?(SchedulePoller)
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
