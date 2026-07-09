# frozen_string_literal: true

module KafkaBatch
  # Process-lifetime mutexes for singleton initialization (avoid lazy ||= races).
  STORE_MUTEX            = Mutex.new
  SCHEDULER_MUTEX        = Mutex.new
  SCHEDULE_STORE_MUTEX   = Mutex.new

  # Core module — configuration, store singleton, logger, and consumer-group
  # name helpers derived purely from config. No dependency on workers, consumers,
  # producers, or Karafka routing. Safe to load in a web-only process.
  class << self
    # ── Configuration ──────────────────────────────────────────────────────

    def configuration
      @configuration ||= Configuration.new
    end
    alias config configuration

    def configure
      yield configuration
    end

    # ── Store ──────────────────────────────────────────────────────────────

    # Returns the configured store singleton. Thread-safe via double-checked locking.
    # @return [Stores::MysqlStore, Stores::RedisStore]
    def store
      return @store if @store
      store_mutex.synchronize do
        @store ||= begin
          config.validate!
          case config.store
          when :mysql then Stores::MysqlStore.new
          when :redis then Stores::RedisStore.new
          else raise ConfigurationError, "Unknown store: #{config.store}"
          end
        end
      end
    end

    # ── Schedule store (perform_in / perform_at index) ─────────────────────

    # Returns the configured delayed-job index singleton. Detached from #store:
    # selected by config.schedule_store (:redis | :mysql). Thread-safe via
    # double-checked locking (same pattern as #store). Returns nil when the
    # Schedule classes aren't loaded (e.g. a process that never required them).
    # @return [Schedule::RedisStore, Schedule::MysqlStore, nil]
    def schedule_store
      return @schedule_store_instance if @schedule_store_instance
      return nil unless defined?(Schedule::RedisStore)

      schedule_store_mutex.synchronize do
        @schedule_store_instance ||= begin
          config.validate!
          case config.schedule_store
          when :mysql then Schedule::MysqlStore.new
          when :redis then Schedule::RedisStore.new
          else raise ConfigurationError, "Unknown schedule_store: #{config.schedule_store}"
          end
        end
      end
    end

    # ── Consumer group name helpers ────────────────────────────────────────
    # Derived entirely from config — no worker registration required.
    # Safe to call in both the web (UI-only) and worker (full) processes.

    def control_consumer_group
      "#{config.consumer_group}-control"
    end

    # Fair pipeline groups are PER LANE (:time | :throughput) so each lane can run
    # and scale in its own process (its own thread pool, dispatcher, forwarder).
    def dispatch_consumer_group(type)
      "#{config.consumer_group}-dispatch-#{type}"
    end

    def jobs_fair_consumer_group(type)
      "#{config.consumer_group}-jobs-fair-#{type}"
    end

    def jobs_consumer_group
      "#{config.consumer_group}-jobs"
    end

    # Go execution tier (kafka-batch-go kbatch worker) — mirrors pkg/config/groups.go.
    def go_worker_jobs_consumer_group
      "#{config.consumer_group}-go-worker-jobs"
    end

    def go_worker_priority_consumer_group(suffix)
      "#{config.consumer_group}-go-worker-#{suffix}"
    end

    def go_worker_fair_ready_consumer_group(type)
      "#{config.consumer_group}-go-worker-fair-ready-#{type}"
    end

    # ── Node identity ──────────────────────────────────────────────────────

    # Identifier for THIS process/pod. Prefers the K8s pod name (ENV["HOSTNAME"])
    # and falls back to the OS hostname; suffixed with the PID.
    def node_id
      @node_id ||= begin
        require "socket"
        host = ENV["HOSTNAME"]
        host = Socket.gethostname if host.nil? || host.empty?
        "#{host}##{Process.pid}"
      end
    end

    # ── Fairness Scheduler singleton ──────────────────────────────────────

    # Returns the process-wide Fairness::Scheduler instance. Redis is a hard
    # dependency of the gem (see Configuration#validate!), so this is expected to
    # be available in any full-backend process. Returns nil only when the
    # Scheduler class is not loaded (e.g. a UI-only process that requires
    # `kafka_batch/ui` but never `kafka_batch/fairness/scheduler`) or if init
    # genuinely fails — callers on the fairness path treat nil as fatal.
    # Thread-safe via double-checked locking (same pattern as #store). Because
    # Redis is mandatory, a nil result means "not built yet / transient build
    # failure", so we rebuild on the next call rather than caching the nil
    # (caching nil would make reset! — or a momentary Redis blip at boot —
    # permanently disable fairness for the process).
    def scheduler(type = :time)
      type = type.to_sym
      cached = (@schedulers ||= {})[type]
      return cached if cached

      scheduler_mutex.synchronize do
        @schedulers ||= {}
        return @schedulers[type] if @schedulers[type]

        @schedulers[type] =
          if defined?(Fairness::Scheduler)
            begin
              Fairness::Scheduler.new(type: type)
            rescue => e
              logger.warn("[KafkaBatch] Fairness::Scheduler(#{type}) init failed: #{e.message}")
              nil
            end
          end
      end
    end

    # ── Fairness (UI-safe fallbacks) ──────────────────────────────────────
    # The full backend (kafka_batch.rb) overrides fairness? with the real
    # worker-registry check. Core provides a safe default so the fairness
    # page renders (with an "inactive" notice) in UI-only processes.

    # True if any registered worker opts into multi-tenant fairness.
    # Overridden by kafka_batch.rb in full-backend mode.
    def fairness?
      false
    end

    # Fairness lanes in use. UI-only processes have no worker registry, so the
    # dashboard assumes both lanes may exist. Overridden by kafka_batch.rb
    # (full backend) with the real worker-registry-driven list.
    def active_fairness_types
      Configuration::FAIRNESS_TYPES.dup
    end

    # Partition count of a fairness lane's ingest topic, via Karafka::Admin.
    # Works in both UI-only and full-backend processes; returns nil on error.
    # Cached per-type for 60s per process to avoid a metadata round-trip per page.
    INGEST_PARTITION_COUNT_TTL = 60 # seconds

    def fairness_ingest_partition_count(type = :time)
      type   = type.to_sym
      cache  = (@ingest_partition_count_cache ||= {})
      cached = cache[type]
      if cached && (Process.clock_gettime(Process::CLOCK_MONOTONIC) - cached[:at]) < INGEST_PARTITION_COUNT_TTL
        return cached[:value]
      end

      value = fetch_ingest_partition_count(type)
      cache[type] = { value: value, at: Process.clock_gettime(Process::CLOCK_MONOTONIC) }
      value
    end

    private

    def fetch_ingest_partition_count(type = :time)
      ensure_karafka_configured!
      return nil unless defined?(Karafka) && defined?(Karafka::Admin)

      topic_name = config.fairness_ingest_topic(type)
      info       = Karafka::Admin.cluster_info

      # cluster_info.topics may return an Array of Structs/Hashes or a Hash
      # keyed by topic name, depending on the rdkafka-ruby version. Handle both.
      topics = info.topics
      found  =
        if topics.is_a?(Hash)
          topics[topic_name]
        else
          topics.find { |t| (t.respond_to?(:topic_name) ? t.topic_name : t[:topic_name]) == topic_name }
        end

      return nil unless found

      # Struct: found.partitions.size  |  Hash: found[:partition_count] or found[:partitions].size
      if found.respond_to?(:partitions)
        found.partitions.size
      elsif found.is_a?(Hash)
        found[:partition_count] || found[:partitions]&.size
      end
    rescue StandardError => e
      logger.warn("[KafkaBatch] could not read partition count for '#{config.fairness_ingest_topic(type)}': #{e.message}")
      nil
    end

    public

    # Ingest partition for a given tenant_id on a lane, or nil if partition count
    # is unavailable.
    def fairness_ingest_partition_for(tenant_id, type = :time)
      count = fairness_ingest_partition_count(type)
      return nil if count.nil? || count.zero?

      Partition.for_key(tenant_id.to_s, count)
    end

    # Returns the ingest partition for a tenant, or nil to fall back to key-hash.
    #
    # Resolution order (Fairness::TenantPartitions):
    #   1. config.fairness_tenant_partitions[tenant_id]
    #   2. Redis checkout when fairness_dynamic_tenant_partitions
    #   3. nil → murmur2_random key-hash
    def tenant_ingest_partition(tenant_id, type = :time)
      Fairness::TenantPartitions.resolve(tenant_id, type)
    end

    # Lazily configure Karafka with the minimum settings needed for
    # Karafka::Admin to function in a UI-only process (one that loads
    # kafka_batch/ui but never runs karafka.rb).
    #
    # Safe to call in all processes:
    #   - Full backend: Karafka is already configured via karafka.rb → no-op.
    #   - UI-only:      sets bootstrap.servers from KafkaBatch.config.brokers
    #                   so Karafka::Admin.read_lags_with_offsets works.
    def ensure_karafka_configured!
      # Karafka::App is always defined once karafka is required (it's the base
      # class). Karafka.setup / Karafka.config do NOT exist as module-level
      # methods in Karafka 2.x — always use Karafka::App.config directly.
      return unless defined?(Karafka) && defined?(Karafka::App)

      # Check if Karafka already has bootstrap.servers configured (set by the
      # app's karafka.rb via Karafka::App.setup). Check both symbol and string
      # keys since Karafka versions differ on internal representation.
      already_configured = begin
        kafka_cfg = Karafka::App.config.kafka
        kafka_cfg[:'bootstrap.servers'].to_s.length.positive? ||
          kafka_cfg['bootstrap.servers'].to_s.length.positive?
      rescue StandardError
        false
      end
      return if already_configured

      broker_list = Array(config.brokers).join(",")
      return if broker_list.empty?

      # Inject bootstrap.servers directly into Karafka's kafka config hash.
      # We cannot call Karafka::App.setup again (Karafka 2.x prevents double
      # setup), so we mutate the existing config hash in place.
      Karafka::App.config.kafka[:'bootstrap.servers'] = broker_list
    rescue StandardError => e
      logger.warn("[KafkaBatch] Karafka auto-config for admin reads failed: #{e.message}")
    end

    # ── Logging ────────────────────────────────────────────────────────────

    def logger
      config.logger
    end

    # ── Reset (for tests) ─────────────────────────────────────────────────

    # Resets all singletons. Overridden by the full backend load
    # (kafka_batch.rb) to also reset producer, workers, etc.
    def reset!
      @configuration    = nil
      @store            = nil
      @schedulers       = nil
      @ingest_partition_count_cache = nil
      @schedule_store_instance = nil
      @node_id          = nil
      CancellationCache.reset! if defined?(CancellationCache)
      Liveness.reset!          if defined?(Liveness)
      ConsumptionControl.reset! if defined?(ConsumptionControl)
      Fairness::TenantPartitions.reset! if defined?(Fairness::TenantPartitions)
      AuditLog.reset! if defined?(AuditLog)
      Metrics.reset!  if defined?(Metrics)
    end

    private

    def store_mutex
      STORE_MUTEX
    end

    def scheduler_mutex
      SCHEDULER_MUTEX
    end

    def schedule_store_mutex
      SCHEDULE_STORE_MUTEX
    end
  end
end
