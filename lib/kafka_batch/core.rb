# frozen_string_literal: true

module KafkaBatch
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

    # ── Consumer group name helpers ────────────────────────────────────────
    # Derived entirely from config — no worker registration required.
    # Safe to call in both the web (UI-only) and worker (full) processes.

    def control_consumer_group
      "#{config.consumer_group}-control"
    end

    def dispatch_consumer_group
      "#{config.consumer_group}-dispatch"
    end

    def jobs_fair_consumer_group
      "#{config.consumer_group}-jobs-fair"
    end

    def jobs_consumer_group
      "#{config.consumer_group}-jobs"
    end

    def fast_consumer_group
      "#{config.consumer_group}-jobs-fast"
    end

    def slow_consumer_group
      "#{config.consumer_group}-jobs-slow"
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

    # Returns the process-wide Fairness::Scheduler instance, or nil when Redis
    # is not configured or Scheduler is not loaded (e.g. UI-only deployments
    # that don't require `kafka_batch/fairness/scheduler`).
    # Thread-safe via double-checked locking (same pattern as #store).
    def scheduler
      return @scheduler if instance_variable_defined?(:@scheduler)
      scheduler_mutex.synchronize do
        return @scheduler if instance_variable_defined?(:@scheduler)
        @scheduler =
          if defined?(Fairness::Scheduler) &&
             config.redis_url && !config.redis_url.to_s.empty?
            begin
              Fairness::Scheduler.new
            rescue => e
              logger.warn("[KafkaBatch] Fairness::Scheduler init failed: #{e.message}")
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

    # Partition count of the fairness ingest topic, via Karafka::Admin.
    # Works in both UI-only and full-backend processes; returns nil on error.
    # Cached for 60s per process to avoid a metadata round-trip on every page load.
    INGEST_PARTITION_COUNT_TTL = 60 # seconds

    def fairness_ingest_partition_count
      cached = @ingest_partition_count_cache
      if cached && (Process.clock_gettime(Process::CLOCK_MONOTONIC) - cached[:at]) < INGEST_PARTITION_COUNT_TTL
        return cached[:value]
      end

      value = fetch_ingest_partition_count
      @ingest_partition_count_cache = { value: value, at: Process.clock_gettime(Process::CLOCK_MONOTONIC) }
      value
    end

    private

    def fetch_ingest_partition_count
      ensure_karafka_configured!
      return nil unless defined?(Karafka) && defined?(Karafka::Admin)

      topic_name = config.fairness_ingest_topic
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
      logger.warn("[KafkaBatch] could not read partition count for '#{config.fairness_ingest_topic}': #{e.message}")
      nil
    end

    public

    # Ingest partition for a given tenant_id, or nil if partition count
    # is unavailable.
    def fairness_ingest_partition_for(tenant_id)
      count = fairness_ingest_partition_count
      return nil if count.nil? || count.zero?

      Partition.for_key(tenant_id.to_s, count)
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
      @store_mutex      = nil
      @scheduler        = nil
      @scheduler_mutex  = nil
      @node_id          = nil
      CancellationCache.reset! if defined?(CancellationCache)
      Liveness.reset!          if defined?(Liveness)
      ConsumptionControl.reset! if defined?(ConsumptionControl)
    end

    private

    def store_mutex
      @store_mutex ||= Mutex.new
    end

    def scheduler_mutex
      @scheduler_mutex ||= Mutex.new
    end
  end
end
