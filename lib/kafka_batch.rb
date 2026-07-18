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
require_relative "kafka_batch/dlt"
require_relative "kafka_batch/consumers/consumption_gate"
require_relative "kafka_batch/consumers/expired_job_handler"
require_relative "kafka_batch/topics"
require_relative "kafka_batch/fairness/scheduler"
require_relative "kafka_batch/fairness/tenant_partitions"
require_relative "kafka_batch/fairness/forwarder"
require_relative "kafka_batch/fairness/dispatcher"
require_relative "kafka_batch/execution_context"
require_relative "kafka_batch/handler_definition"
require_relative "kafka_batch/executors/ruby"
require_relative "kafka_batch/handler_registry"
require_relative "kafka_batch/handler_manifest"
require_relative "kafka_batch/worker"
require_relative "kafka_batch/uniqueness"
require_relative "kafka_batch/job_expiry"
require_relative "kafka_batch/batch"
require_relative "kafka_batch/callback"
require_relative "kafka_batch/callbacks/dispatcher"
require_relative "kafka_batch/schedule_poller"
require_relative "kafka_batch/reconciler"
require_relative "kafka_batch/workset"
require_relative "kafka_batch/super_fetch"
require_relative "kafka_batch/watermark"
require_relative "kafka_batch/consumers/job_consumer"
require_relative "kafka_batch/consumers/priority_gate"
require_relative "kafka_batch/priority/config"
require_relative "kafka_batch/priority/registry"
require_relative "kafka_batch/consumers/priority_job_consumer"
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
      return unless klass.name && !klass.name.to_s.empty?

      HandlerRegistry.register_ruby(klass)
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

    # True when any handler (worker or manifest) uses a fair lane for a runtime.
    def fair_handlers_for_runtime?(runtime, lane)
      lane = lane.to_sym
      rt   = runtime.to_sym
      workers.any? { |w| w.fairness? && w.fairness_type == lane && w.executor == rt } ||
        manifest_fair_handlers_for_runtime?(rt, lane)
    end

    # Kafka ready topic Ruby Karafka should consume for a fairness lane.
    # Returns nil when no ruby fair handlers are registered.
    def fair_ready_consume_topic(lane)
      lane = lane.to_sym
      return nil unless fair_handlers_for_runtime?(:ruby, lane)

      cfg = config
      if cfg.runtime_split_fair_ready?(lane)
        cfg.fairness_ready_topic(lane, :ruby)
      else
        cfg.fairness_ready_topic(lane)
      end
    end

    def manifest_fair_handlers_for_runtime?(runtime, lane)
      return false unless HandlerManifest.loaded?

      HandlerManifest.definitions.values.any? do |d|
        d.fairness_type == lane && d.runtime == runtime
      end
    end

    # Loaded priority group definitions from YAML (empty when no paths configured).
    def priority_registry
      @priority_registry ||= Priority::Registry.load(config.resolved_priority_config_paths)
    end

    # ── Consumer groups ────────────────────────────────────────────────────
    # Returns the groups that exist given the current worker registry.
    # Used by draw_routes and (legacy) lag page fallback.

    def consumer_groups
      groups   = [control_consumer_group]
      active_fairness_types.each do |ft|
        groups << dispatch_consumer_group(ft) << jobs_fair_consumer_group(ft)
      end

      registry = priority_registry
      groups.concat(registry.consumer_groups)

      reserved = registry.reserved_topics
      plain    = workers.reject(&:fairness?)
      other    = plain.reject { |w| reserved.include?(w.kafka_topic) }
      groups << jobs_consumer_group unless other.empty?

      groups
    end

    # ── Karafka routing ────────────────────────────────────────────────────

    # Load handler manifest (Go-only handlers) when config.handler_manifest_path is set.
    def load_handler_manifest!
      path = config.resolved_handler_manifest_path
      return unless path

      HandlerManifest.load!(path)
    end

    def draw_routes(builder)
      if config.daemon_mode?
        KafkaBatch.logger.warn(
          "[KafkaBatch] daemon_mode enabled — skipping Karafka consumers " \
          "(client-only process; run control/execution in separate Karafka pods)"
        )
        return
      end

      load_handler_manifest!

      cfg          = config
      registry     = priority_registry
      all_workers  = KafkaBatch.workers
      fair_workers = all_workers.select(&:fairness?)
      plain_topics = all_workers.reject(&:fairness?).map(&:kafka_topic).uniq
      # Ruby Karafka must not subscribe to Go execution topics even when the
      # shared handler/priority YAML lists them (needed for /lag Go groups).
      manifest_plain = HandlerManifest.loaded? ? HandlerManifest.ruby_plain_topics : []
      active_fair_types = active_fairness_types

      reserved           = registry.reserved_topics
      other_plain_topics = plain_topics.reject { |t| reserved.include?(t) }
      manifest_plain     = manifest_plain.reject { |t| reserved.include?(t) }
      all_plain_topics   = (other_plain_topics + manifest_plain).uniq
      registry.validate_plain_topics!(other_plain_topics) unless registry.empty?

      builder.instance_eval do
        consumer_group "#{cfg.consumer_group}-control" do
          topic(cfg.events_topic)    { consumer KafkaBatch::Consumers::EventConsumer }
          topic(cfg.callbacks_topic) { consumer KafkaBatch::Consumers::CallbackConsumer }
          cfg.retry_topics.each do |retry_topic|
            topic(retry_topic) { consumer KafkaBatch::Consumers::RetryConsumer }
          end
        end

        active_fair_types.each do |ft|
          consumer_group "#{cfg.consumer_group}-dispatch-#{ft}" do
            topic(cfg.fairness_ingest_topic(ft)) do
              consumer KafkaBatch::Fairness::Dispatcher
              max_messages cfg.fairness_dispatcher_batch_size
            end
          end
          ready_topic = KafkaBatch.fair_ready_consume_topic(ft)
          if ready_topic && !ready_topic.to_s.empty?
            consumer_group "#{cfg.consumer_group}-jobs-fair-#{ft}" do
              topic(ready_topic) { consumer KafkaBatch::Consumers::JobConsumer }
            end
          end
        end

        registry.configs.each do |prio_cfg|
          ruby_topics = prio_cfg.topics.reject { |t| HandlerManifest.go_only_topic?(t) }
          next if ruby_topics.empty?

          consumer_group prio_cfg.consumer_group do
            ruby_topics.each do |topic_name|
              rank = prio_cfg.topics.index(topic_name)
              consumer_klass = KafkaBatch::Consumers::PriorityJobConsumer.build(
                rank:                rank,
                mode:                prio_cfg.mode,
                higher_topics:       prio_cfg.higher_topics_for(topic_name),
                consumer_group:      prio_cfg.consumer_group,
                topic:               topic_name,
                weighted_interleave: prio_cfg.weighted_interleave
              )
              topic(topic_name) { consumer consumer_klass }
            end
          end
        end

        unless all_plain_topics.empty?
          consumer_group "#{cfg.consumer_group}-jobs" do
            all_plain_topics.uniq.each do |job_topic|
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
      validate_fair_ready_split!
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

    def validate_fair_ready_split!
      return unless fairness?

      load_handler_manifest! if config.resolved_handler_manifest_path && !HandlerManifest.loaded?

      %i[time throughput].each do |lane|
        go_fair   = fair_handlers_for_runtime?(:go, lane)
        ruby_fair = fair_handlers_for_runtime?(:ruby, lane)
        next unless go_fair && ruby_fair
        next if config.runtime_split_fair_ready?(lane)

        raise ConfigurationError,
              "hybrid fairness on #{lane} lane requires split ready topics " \
              "(fairness_#{lane}_ready_go and fairness_#{lane}_ready_ruby)"
      end
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

      if config.fairness_dynamic_tenant_partitions
        active_fairness_types.each do |type|
          Fairness::TenantPartitions.warm!(type)
        end
      end
    end

    # ── Reset (full — overrides core.rb's minimal version) ────────────────

    def reset!
      @configuration   = nil
      @priority_registry = nil
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
      SuperFetch.reset! if defined?(SuperFetch)
      Workset::ReclaimScheduler.reset! if defined?(Workset::ReclaimScheduler)
      Workset.reset! if defined?(Workset)
      Uniqueness.reset! if defined?(Uniqueness)
      ConsumptionControl.reset!
      Fairness::TenantPartitions.reset! if defined?(Fairness::TenantPartitions)
      Fairness::Forwarder.stop! if defined?(Fairness::Forwarder)
      SchedulePoller.stop! if defined?(SchedulePoller)
      Recurring::Ticker.stop! if defined?(Recurring::Ticker)
      AuditLog.reset! if defined?(AuditLog)
      Metrics.reset!  if defined?(Metrics)
      PerformanceMetrics.reset! if defined?(PerformanceMetrics)
      HandlerRegistry.reset! if defined?(HandlerRegistry)
      HandlerManifest.reset! if defined?(HandlerManifest)
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
