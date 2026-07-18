require_relative "redis_client"

module KafkaBatch
  class Configuration
    # ── Store ────────────────────────────────────────────────────────────────
    # :redis  – (default) all batch ledger state in Redis
    # :mysql  – batch ledger in Redis + failures/pauses in MySQL
    attr_accessor :store
    # When store is :mysql, optional dedicated DB connection. nil = ActiveRecord::Base default.
    # Accepts: AR model class, database.yml name (:symbol), or connection Hash.
    attr_accessor :store_database_connection

    # ── Kafka connection ─────────────────────────────────────────────────────
    attr_accessor :brokers          # Array<String>  e.g. ["localhost:9092"]

    # ── Topic namespace ────────────────────────────────────────────────────────
    # All KafkaBatch topic names AND the consumer group derive from this prefix
    # unless set explicitly. e.g. `config.topic_prefix = "myapp"` →
    # "myapp.kafka_batch.jobs" and consumer group "myapp.kafka-batch". Default: "".
    attr_reader :topic_prefix

    def topic_prefix=(value)
      @topic_prefix = value.to_s.strip
    end

    # Prefix-aware name settings. Each getter derives "<prefix>.<base>" from
    # topic_prefix; assigning a value overrides it verbatim (no prefix applied).
    # Defaults (empty prefix) are the bare base names below.
    PREFIXED_SETTINGS = {
      jobs_topic:            "kafka_batch.jobs",        # shared default worker topic
      events_topic:          "kafka_batch.events",      # completion events
      callbacks_topic:       "kafka_batch.callbacks",   # batch callbacks
      dead_letter_topic:     "kafka_batch.dead_letter", # exhausted / poison jobs
      retry_topic:           "kafka_batch.jobs.retry",  # base; per-tier is <this>.short/.medium/.large
      scheduled_topic:       "kafka_batch.scheduled",   # durable payload store for perform_in/perform_at
      consumer_group:        "kafka-batch",             # base consumer group (suffixed -control/-dispatch/…)
      # Two independent fairness lanes — a worker picks one via `fairness_type`
      # (:time | :throughput). Each lane has its own ingest → ready topics,
      # scheduler, and tenant weights, so both run side by side.
      fair_time_ingest_topic:       "kafka_batch.fair_time_ingest",       # time-fairness intake
      fair_time_ready_topic:        "kafka_batch.fair_time_ready",        # legacy single ready (pre-v1.1)
      fair_time_ready_go_topic:     "kafka_batch.fair_time_ready.go",     # time-fairness go execution queue
      fair_time_ready_ruby_topic:   "kafka_batch.fair_time_ready.ruby",   # time-fairness ruby execution queue
      fair_throughput_ingest_topic: "kafka_batch.fair_throughput_ingest", # throughput-fairness intake
      fair_throughput_ready_topic:  "kafka_batch.fair_throughput_ready",  # legacy single ready (pre-v1.1)
      fair_throughput_ready_go_topic:   "kafka_batch.fair_throughput_ready.go",
      fair_throughput_ready_ruby_topic: "kafka_batch.fair_throughput_ready.ruby"
    }.freeze

    PREFIXED_SETTINGS.each do |name, base|
      define_method(name) do
        v = instance_variable_get(:"@#{name}")
        v.nil? ? prefixed(base) : v
      end
      define_method(:"#{name}=") { |val| instance_variable_set(:"@#{name}", val) }
    end

    # Custom plain-worker topics a UI-only dashboard (require: "kafka_batch/ui",
    # which never calls draw_routes and loads no worker classes) should list on
    # the /lag page. Values are used VERBATIM — matching how workers declare
    # `kafka_topic "orders.process"` — so this is a plain list, NOT prefix-aware.
    # Only affects the config-based /lag fallback; worker processes that draw
    # routes resolve custom topics from the routes directly.
    attr_accessor :extra_job_topics   # Array<String> – default []

    # ── Cancellation ─────────────────────────────────────────────────────────
    # When true, JobConsumer skips execution of jobs whose batch was cancelled.
    # The set of cancelled batch ids is cached per process and refreshed at most
    # once per cancellation_cache_ttl seconds (NOT read from the store on every
    # job), so cancellation takes effect within that window – some already-queued
    # jobs may still run before the next refresh, which is an accepted trade-off.
    attr_accessor :skip_cancelled_jobs   # Boolean – default true
    attr_accessor :cancellation_cache_ttl  # Integer – seconds; default 120

    # ── Liveness (running jobs / consumers dashboard) ─────────────────────────
    # Visibility into currently-running jobs and live consumer processes. Backed
    # by Redis (config.redis_url), which is a required dependency of the gem.
    #   :redis – (default) full per-job + per-consumer tracking in Redis, short
    #            TTL, best-effort behind a circuit breaker.
    #   :off   – disabled.
    attr_accessor :liveness_backend            # Symbol – default :redis
    attr_accessor :track_running_jobs          # Boolean – default true (gates :redis per-job writes; set false at scale)
    # Redis EX TTL (seconds) for kafka_batch:live:consumer:* heartbeats — when the
    # key expires the pod is treated as dead (/live + SuperFetch reclaim). Default 180
    # (≈9×20s heartbeat interval). Env: KAFKA_BATCH_LIVENESS_TTL.
    attr_accessor :liveness_ttl
    # How often (seconds) the background loop refreshes the heartbeat key. Default 20.
    # Env: KAFKA_BATCH_LIVENESS_HEARTBEAT_INTERVAL.
    attr_accessor :liveness_heartbeat_interval # Integer – seconds; default 20
    # How often each consumer process re-samples its own RSS/CPU for /live.
    # 0 disables stats (heartbeats still update last_seen). Default 15s.
    attr_accessor :liveness_stats_interval     # Integer – seconds; default 15

    # ── SuperFetch (in-partition job concurrency) ─────────────────────────────
    # Always on: Claim → Kafka mark → thread-pool #perform. Default 1 because
    # MRI threads do not run Ruby code in parallel (GVL); raise only for
    # IO-wait overlap and keep karafka_concurrency × super_fetch_concurrency ≤ 10
    # in production. See README "SuperFetch concurrency (Ruby)".
    attr_accessor :super_fetch_concurrency     # Integer – default 1
    # Max Claimed∨Queued∨Performing per process. 0 → 2× super_fetch_concurrency.
    attr_accessor :super_fetch_claim_window    # Integer – default 0 (auto 2×)
    attr_accessor :super_fetch_lease_ttl       # Integer – seconds; default 120
    # Steal/orphan grace after claim before a missing heartbeat is stealable.
    # Align with Go daemon reclaim (default 40 ≈ 2× heartbeat interval).
    attr_accessor :super_fetch_orphan_grace    # Integer – seconds; default 40
    # Control-plane orphan reclaim (parity with Go kbatch daemon). Default on.
    attr_accessor :super_fetch_reclaim_enabled # Boolean – default true
    attr_accessor :super_fetch_reclaim_interval # Integer – seconds; default 30
    attr_accessor :super_fetch_reclaim_limit   # Integer – per sweep; default 100
    # Seconds to wait for in-flight SuperFetch #perform on SIGTERM (default 30).
    # Leftovers stay in the Redis workset for control-plane reclaim.
    attr_accessor :super_fetch_drain_timeout   # Integer – seconds; default 30

    # Tier-3 (JobConsumer) execution mode: :superfetch (default — Redis working-set
    # ownership, offset marked ahead of #perform) or :watermark (Redis-free — commit
    # the contiguous completed-offset prefix; uncommitted work re-runs on crash/
    # rebalance). Watermark REQUIRES idempotent handlers + similar per-topic runtimes,
    # and every consumer on a topic must use the same mode. See the README
    # "Execution mode" section. Set via KAFKA_BATCH_EXECUTION_MODE.
    attr_accessor :execution_mode              # Symbol – :superfetch (default) | :watermark

    # ── Job uniqueness (per-worker `uniq true`) ───────────────────────────────
    # Redis-backed dedup of worker_class + payload while a job is queued or in
    # progress. Keys use 8-byte binary XXHash64 digests (not hex) to save RAM.
    attr_accessor :uniq_enabled                # Boolean – default true (master switch)
    attr_accessor :uniq_lock_ttl               # Integer – seconds; default 7 days
    # :skip – return nil from enqueue/push on duplicate; :raise – DuplicateJobError
    attr_accessor :uniq_on_duplicate           # Symbol – :skip (default) | :raise

    # ── Consumption pause/resume (/lag dashboard) ─────────────────────────────
    # Karafka consumers reload pause state from Redis (or MySQL when store is
    # :mysql and Redis is unavailable) at most this often. The Web UI always
    # reads fresh state.
    attr_accessor :consumption_control_refresh_interval # Integer – seconds; default 30

    # ── Retry behaviour ──────────────────────────────────────────────────────
    # Tiered retries: each delay tier has its own Kafka topic, so a slow tier
    # never head-of-line-blocks a fast one (within a topic, FIFO == due order).
    # By default the Nth retry walks the progression (1st→short, 2nd→medium,
    # 3rd+→large); a Worker can pin all its retries to one tier via `retry_tier`.
    attr_accessor :max_retries             # Integer – attempts before dead letter (worker can override)
    attr_accessor :retry_jitter            # Float   – +/- fraction of randomization (default 0.1)
    attr_accessor :retry_tiers             # Hash{Symbol=>Integer} – tier => delay seconds
    attr_accessor :retry_tier_progression  # Array<Symbol> – default tier per retry index
    # Maximum single pause duration (seconds) in RetryConsumer. When a retry is
    # further in the future than this, the consumer pauses for this long and then
    # re-checks, so the partition is never suspended for an extreme duration.
    attr_accessor :retry_max_pause_seconds # Integer – default 30

    # ── Delayed jobs (perform_in / perform_at) ───────────────────────────────
    # The Sidekiq perform_in/perform_at equivalent. The job payload is produced
    # to the durable `scheduled_topic`; a compact pointer (job_id:partition:offset)
    # scored by run-at time is kept in the SCHEDULE store (below), and a per-process
    # SchedulePoller re-produces each job onto its real topic when it comes due.
    #
    # Backend for the schedule index — DETACHED from `store` so the main ledger can
    # be Redis while the (potentially huge) schedule index lives in MySQL:
    #   :redis – (default) ZSET scored by run-at; RAM-resident, lowest latency.
    #   :mysql – kafka_batch_scheduled_jobs table; disk-resident, cheap at scale,
    #            native per-job cancel/lookup by primary key.
    attr_accessor :schedule_store            # Symbol – :redis (default) | :mysql
    # When schedule_store is :mysql, optional dedicated DB connection (see store_database_connection).
    attr_accessor :schedule_store_database_connection
    # Retries when Kafka produce succeeded but the schedule index write fails.
    attr_accessor :schedule_index_write_retries  # Integer – default 3
    attr_accessor :schedule_index_write_backoff  # Float – seconds; linear: attempt * backoff; default 0.05
    attr_accessor :schedule_poller_enabled   # Boolean – default false (opt in on scheduler pods)
    attr_accessor :schedule_poll_interval    # Float   – base seconds between polls when work is flowing; default 5.0
    # When a poll finds nothing due, the poller backs off (doubling the sleep up to
    # this cap) so idle pods stop hammering the schedule store; it snaps back to
    # schedule_poll_interval the moment a poll returns work. With many pods this is
    # the main throttle on idle DB/Redis load. Set == schedule_poll_interval to disable.
    attr_accessor :schedule_poll_max_interval # Float   – seconds; default 60.0
    attr_accessor :schedule_poll_jitter      # Float   – +/- fraction on the sleep (de-syncs pods); default 0.1
    attr_accessor :schedule_batch_size       # Integer – max due jobs claimed per tick; default 100
    # In-flight LEASE: a claimed-but-not-dispatched job is reclaimed after this many
    # seconds so a poller/process crash mid-dispatch cannot strand it (at-least-once).
    attr_accessor :schedule_lease_seconds    # Integer – default 60
    attr_accessor :schedule_reclaim_interval # Integer – seconds between reclaim sweeps; default 30

    # ── Recurring (cron) scheduler ────────────────────────────────────────────
    # Fires a registered manifest job on a repeating cron schedule. Shares the
    # kafka_batch_recurring_schedules / _fires tables AND the Redis leader lock
    # (kafka_batch:cron:leader_lock) with the Go daemon, so a Ruby and a Go
    # ticker can run against the same cluster without double-firing: at most one
    # holds the lock per window, and the (schedule_id, fire_at) ledger dedups
    # regardless. Enable on exactly the pods that should run it.
    attr_accessor :recurring_scheduler_enabled # Boolean – default false
    attr_accessor :recurring_window            # Float   – resolution/poll seconds; default 30
    attr_accessor :recurring_lock_ttl          # Integer – leader-lease TTL seconds; default 60
    attr_accessor :recurring_batch_size        # Integer – schedules per tick; default 100
    attr_accessor :recurring_misfire_grace     # Float   – within this of now always fires; default 60
    attr_accessor :recurring_max_backfill      # Integer – cap fires/schedule/tick; default 1000
    attr_accessor :recurring_recover_every     # Float   – pending-recovery sweep seconds; default 300
    attr_accessor :recurring_recover_grace     # Float   – pending age before re-enqueue; default 120
    attr_accessor :recurring_prune_every       # Float   – dispatched-ledger prune seconds; default 3600
    attr_accessor :recurring_prune_retention   # Float   – keep dispatched rows seconds; default 604800
    attr_accessor :recurring_heartbeat_every   # Float   – stale sweep seconds; default 60
    attr_accessor :recurring_stale_factor      # Float   – stale if idle > factor×interval; default 2.0
    # Longest allowed delay. MUST be <= the scheduled_topic's retention.ms, else a
    # job could point at an offset already removed by log cleanup. Schedules beyond
    # this are clamped down to it. Default 7 days.
    attr_accessor :max_schedule_horizon      # Integer – seconds; default 7 days
    # Replication factor passed to KafkaBatch::Topics.create_all! when the caller
    # does not override it. Use 1 for single-broker dev; 3+ for production.
    attr_accessor :topics_replication_factor # Integer – default 3

    # ── Completion-event emission retries ────────────────────────────────────
    # After a job succeeds, the consumer produces a completion event. If that
    # produce fails (transient Kafka issue) it is retried inline before giving
    # up and leaving the offset uncommitted for redelivery. The backoff sleeps on
    # the Karafka worker thread, so keep (retries * backoff) modest.
    attr_accessor :event_emit_retries  # Integer – attempts; default 3
    attr_accessor :event_emit_backoff  # Integer – seconds; linear: attempt * backoff

    # ── Redis ────────────────────────────────────────────────────────────────
    # Redis is a REQUIRED dependency (fairness scheduler + liveness). The :redis
    # store also keeps all batch state here.
    #
    # Configure with either:
    #   config.redis_url = "redis://localhost:6379/0"
    # or a Rails-style hash (mutually exclusive — setting one clears the other):
    #   config.redis = { host: "localhost", port: 6379, db: 0 }
    attr_reader :redis              # Hash — set via #redis=
    attr_accessor :redis_pool_size  # Integer

    def redis_url
      raw = @redis_url
      return raw if raw && !raw.to_s.empty?
      return KafkaBatch::RedisClient.url_for(@redis) if @redis.is_a?(Hash) && !@redis.empty?

      nil
    end

    def redis_url=(value)
      @redis_url = value.nil? ? nil : value.to_s
      @redis     = nil if value && !value.to_s.empty?
    end

    # Raw URL string when set explicitly (not derived from +redis+ hash).
    def redis_url_raw
      v = @redis_url
      v if v && !v.to_s.empty?
    end

    def redis=(value)
      unless value.is_a?(Hash)
        raise ConfigurationError, "config.redis must be a Hash (got #{value.class})"
      end

      @redis     = value.transform_keys(&:to_sym)
      @redis_url = nil
    end

    def redis_configured?
      (@redis_url && !@redis_url.to_s.empty?) ||
        (@redis.is_a?(Hash) && !@redis.empty?)
    end

    # ── TTL for batch metadata in Redis (:redis store) ──────────────────────
    attr_accessor :batch_ttl        # Integer – seconds; default 7 days

    # Maximum number of batch IDs kept in the ALL_INDEX ZSET used by the web UI
    # (:redis store). Oldest entries are evicted when the cap is reached so the
    # ZSET never grows unbounded.
    attr_accessor :all_index_max_size  # Integer – default 200_000

    # ── Failure metadata retention (MySQL store only) ────────────────────────
    # The real job data is durable in Kafka (retry / dead-letter topics), so
    # the default (:redis) store never persists per-job failure metadata.
    # These only apply to Stores::MysqlStore's kafka_batch_failures table:
    # failures_ttl bounds the reconciler purge window and max_failures_per_batch
    # is unused there (kept for config backward-compatibility).
    attr_accessor :failures_ttl              # Integer – seconds; default 1 day
    attr_accessor :max_failures_per_batch    # Integer – 0 = unlimited; default 1000
    attr_accessor :retry_cancel_ttl          # Integer – cancel/skip Redis TTL; default 7 days

    # ── Multi-tenant fairness (Redis-backed WFQ; Redis REQUIRED) ─────────────
    # Fairness is a PER-WORKER property (`fairness true` on the Worker class) —
    # there is no global enable switch. When a worker opts in, capacity is shared
    # dynamically across tenants via a Redis Weighted-Fair-Queuing scheduler:
    # one active tenant uses 100%, N split ~1/N (work-conserving), weighted by
    # per-tenant weight. Flow:
    #
    #   Batch.push → ingest topic (durable backlog, keyed by tenant)
    #     → Fairness::Dispatcher   (enqueue into the bounded Redis WFQ window)
    #     → Fairness::Forwarder     (checkout the fairest job; concurrency-gated;
    #                                forward to the ready topic)
    #     → ready topic → JobConsumer swarm → perform → Scheduler#complete

    # ── Two fairness lanes (per-worker, run simultaneously) ──────────────────
    # Fairness is chosen PER WORKER via `fairness_type` (:time | :throughput);
    # both lanes run at once and a single batch may contain jobs of both types.
    #   :time       – vtime advances at *completion* by actual_seconds / weight.
    #                 Fair weighted wall-clock time; best for uneven runtimes.
    #   :throughput – vtime advances by 1/weight at *checkout*. Fair over
    #                 dispatched job count; best when runtimes are similar.
    # Each lane has its own ingest/ready topics, Redis WFQ scheduler, and tenant
    # weights. The concurrency/window knobs below apply to EACH lane independently.
    FAIRNESS_TYPES = %i[time throughput].freeze

    # Deprecated: fairness is now per-worker (`fairness_type`), not a global mode.
    # Kept as a no-op setter so old initializers don't crash.
    def fairness_mode=(_value)
      logger&.warn(
        "[KafkaBatch] config.fairness_mode is removed — set `fairness_type :time` " \
        "or `fairness_type :throughput` on each Worker instead. Ignoring."
      )
    end

    # Kafka ingest/ready topic for a fairness lane.
    def fairness_ingest_topic(type)
      type.to_sym == :throughput ? fair_throughput_ingest_topic : fair_time_ingest_topic
    end

    def fairness_ready_topic(type, runtime = nil)
      ft = type.to_sym == :throughput ? :throughput : :time
      if runtime
        rt = runtime.to_sym
        case ft
        when :throughput
          rt == :go ? fair_throughput_ready_go_topic : fair_throughput_ready_ruby_topic
        else
          rt == :go ? fair_time_ready_go_topic : fair_time_ready_ruby_topic
        end
      else
        ft == :throughput ? fair_throughput_ready_topic : fair_time_ready_topic
      end
    end

    # True when per-runtime ready topics (.go / .ruby) are configured for a lane.
    # Mirrors Go config.RuntimeSplitFairReady.
    def runtime_split_fair_ready?(type = :time)
      ft = type.to_sym == :throughput ? :throughput : :time
      go_topic, ruby_topic =
        case ft
        when :throughput
          [fair_throughput_ready_go_topic.to_s, fair_throughput_ready_ruby_topic.to_s]
        else
          [fair_time_ready_go_topic.to_s, fair_time_ready_ruby_topic.to_s]
        end
      !go_topic.empty? && !ruby_topic.empty?
    end

    # Global in-flight window: max jobs forwarded to the ready topic but not yet
    # completed. Bounds ready-topic depth (keeps fairness dynamic) AND total
    # fair-lane concurrency. The per-tenant share is derived dynamically as
    # ceil(this / active_tenants): 1 active tenant → full window; N → window/N.
    attr_accessor :fairness_global_concurrency      # Integer – default 50

    # Optional HARD ceiling on a single tenant's in-flight jobs, layered on top
    # of the dynamic fair share. 0 = rely on the dynamic share only.
    attr_accessor :fairness_max_inflight_per_tenant # Integer – default 0

    # Active-tenant view — the denominator of the per-tenant fair-share cap.
    # The raw WFQ ring count (tenants with queued jobs *right now*) flickers as
    # tenants briefly drain their ready window, which would make caps jump around.
    # Instead the active count is cached in-process for this TTL and used as a
    # FLOOR (max with the instantaneous ring), so caps respond immediately to load
    # increases but don't balloon on transient drains. Source:
    #   :inflight_plus_ready – (default) distinct tenants with in-flight OR queued
    #                          work (Redis only; accurate; also yields the weighted
    #                          weight-sum so weighted concurrency stays smoothed).
    #   :ingest_lag          – count of ingest partitions with lag > 0 via the
    #                          Karafka Admin API (reflects Kafka-side backlog;
    #                          undercounts once the Dispatcher has drained ingest
    #                          into the Redis window, and can't supply per-tenant
    #                          weights — weighted mode then falls back to the ring
    #                          weight sum). Use only if you specifically want the
    #                          Kafka-backlog notion of "active".
    attr_accessor :fairness_active_count_ttl     # Integer – seconds; default 5
    attr_accessor :fairness_active_count_source  # Symbol – default :inflight_plus_ready

    # Weighted concurrency. When true (default), each active tenant's in-flight cap
    # is proportional to its weight:
    #   cap_t = floor(fairness_global_concurrency * weight_t / Σ active weights)
    # (min 1), so a weight-50 tenant runs ~50× the concurrency of a weight-1
    # tenant even when all tenants are saturated — enforcing the intended
    # job/time distribution, not just competing for slack. Still work-conserving:
    # a lone active tenant's slice equals the whole window. Costs one O(active)
    # weight sum per checkout (negligible at tens/low-hundreds of active tenants).
    # When false, every active tenant gets an EQUAL slice (ceil(global/active));
    # per-tenant weight then only affects selection ORDER under full saturation.
    # NOTE: tenant weights always live in Redis (per-lane WEIGHT hash); set_weight
    # writes there regardless of config.store.
    attr_accessor :fairness_weighted_concurrency    # Boolean – default true

    # Bounded per-tenant staging window in Redis. When full the Dispatcher pauses
    # the ingest partition (backpressure) so the durable backlog stays in Kafka.
    attr_accessor :fairness_ready_window            # Integer – default 500
    attr_accessor :fairness_default_weight          # Numeric – default 1.0

    # How long each process caches the tenant-weight map from Redis; weight
    # changes written via the /weights UI propagate within this window.
    attr_accessor :fairness_weight_cache_ttl        # Integer – seconds; default 60

    # How long the Forwarder sleeps when a checkout yields nothing (idle / window
    # full) before polling the scheduler again.
    attr_accessor :fairness_forwarder_idle_sleep    # Float – seconds; default 0.05

    # Explicit tenant → ingest-partition map. When a tenant_id is present, jobs
    # are produced directly to that partition (bypassing the hash partitioner).
    # Out-of-range values are ignored. Default {}.
    #
    # When fairness_dynamic_tenant_partitions is true, unmapped tenants are
    # assigned exclusively at first use via Redis (see Fairness::TenantPartitions).
    attr_accessor :fairness_tenant_partitions       # Hash{String => Integer}

    # When true (default), new tenants checkout a dedicated ingest partition from
    # Redis on first enqueue (per fairness lane). Config map entries always win.
    # Requires Redis and a warmed partition pool (boot / first checkout).
    # Disable with config.fairness_dynamic_tenant_partitions = false or
    # KAFKA_BATCH_FAIRNESS_DYNAMIC_TENANT_PARTITIONS=false.
    attr_accessor :fairness_dynamic_tenant_partitions  # Boolean – default true

    # In-process TTL for tenant → partition lookups (config + Redis). Default 30s.
    attr_accessor :fairness_tenant_partition_cache_ttl  # Integer – seconds

    # Max ingest messages the Dispatcher drains into the Redis window per consume
    # call (wired as Karafka `max_messages`). Fairness ordering is done by the
    # scheduler, not this batch size.
    attr_accessor :fairness_dispatcher_batch_size   # Integer – default 50

    # Expected Karafka concurrency on the dispatch process. Boot-warning hint
    # only (Karafka OSS concurrency is global; set it in karafka.rb).
    attr_accessor :fairness_dispatcher_concurrency  # Integer – default 5

    # TTL (seconds) on a fair-lane in-flight slot. Each checkout writes a lease
    # scored by (now + this); JobConsumer renews it while perform runs. If a
    # consumer dies mid-job the lease expires and the slot is reclaimed. MUST
    # exceed your longest expected job runtime — otherwise the lane admits extra
    # jobs past the concurrency budget (see fairness_slot_dedup_ttl for duplicates).
    attr_accessor :fairness_lease_ttl               # Integer – seconds; default 1800
    # Seconds to wait after a forwarding lease expires before reclaiming an
    # orphaned checkout (crash between LPOP and produce/confirm). Avoids racing a
    # slow produce on a live pod.
    attr_accessor :fairness_forwarding_recovery_grace # Float – seconds; default 5.0
    # TTL for per-slot execution dedup (ready-topic redelivery after reclaim).
    attr_accessor :fairness_slot_dedup_ttl          # Integer – seconds; default 0 (= lease_ttl)

    # Tenants spread across the ingest topic's partitions by key hash. With too
    # few partitions tenants collide and fairness degrades. The boot check warns
    # (or raises under validate_topics_on_boot) if the ingest topic has fewer.
    attr_accessor :fairness_min_ingest_partitions   # Integer – default 2

    # ── Priority queues (Sidekiq.yml-style, per-process YAML) ────────────────
    # Paths to priority group YAML files (see lib/kafka_batch/priority/config.rb).
    # Also read from ENV KAFKA_BATCH_PRIORITY_CONFIG (one path) and
    # KAFKA_BATCH_PRIORITY_CONFIGS (comma-separated). Workers opt in by setting
    # kafka_topic to a topic listed in a group's topics array. fairness true
    # always takes precedence. Each topic may belong to at most one consumer
    # group (validated at boot).
    attr_accessor :priority_config_paths          # Array<String> – default []
    # How often (seconds) lower-ranked consumers re-check higher-topic lag.
    attr_accessor :priority_lag_check_interval    # Integer – default 2
    # Weighted mode: lower ranks proceed 1-in-N while higher topics have lag.
    attr_accessor :priority_weighted_interleave   # Integer – default 4

    # ── Reconciliation ───────────────────────────────────────────────────────
    # A periodic sweep (inside the EventConsumer) that re-checks stuck "running"
    # batches and recovers lost callbacks.
    attr_accessor :reconciliation_interval  # Integer – seconds; default 300
    # Distributed-lock TTL so a crashed reconciler eventually releases the lock.
    attr_accessor :reconciler_lock_ttl      # Integer – seconds; default 600
    # Max batches processed per sweep (caps callback bursts during incidents).
    attr_accessor :max_reconcile_per_run    # Integer – default 100

    # ── Producer safety ──────────────────────────────────────────────────────
    # Raise a clear ProducerError when an encoded payload exceeds this size.
    # Default 1 MiB (Kafka's typical message.max.bytes). 0/nil disables the guard.
    attr_accessor :max_message_bytes  # Integer – default 1_048_576

    # Chunk size for #push_many / bulk scheduled produce. Sequential chunks preserve
    # gap-free batch_seq semantics while pipelining via produce_many_sync.
    attr_accessor :push_many_chunk_size  # Integer – default 500

    # ── Web audit log (optional) ─────────────────────────────────────────────
    # Persist mutating /kafka_batch UI actions to kafka_batch_audit_logs.
    attr_accessor :audit_enabled                 # Boolean – default false
    attr_accessor :audit_database_connection     # nil | Class | Symbol | Hash
    # Optional Proc(env) → actor string, or static string. Falls back to HTTP headers.
    attr_accessor :audit_actor

    # ── Metrics export (optional) ────────────────────────────────────────────
    # Bridges ActiveSupport::Notifications → StatsD/Datadog or a custom proc.
    attr_accessor :metrics_enabled   # Boolean – default false
    attr_accessor :metrics_adapter   # :statsd | :datadog | :proc
    attr_accessor :metrics_client    # StatsD/Datadog client, or callable for :proc
    attr_accessor :metrics_proc      # alias hook for :proc adapter
    attr_accessor :metrics_prefix    # metric name prefix; default "kafka_batch"

    # ── Performance dashboard metrics (optional; Redis-backed) ───────────────
    # Opt-in throughput/error-rate history for the Web UI's Performance page.
    # Subscribes to the existing job.processed / job.retried / job.failed /
    # workset.reclaimed instrumentation events and writes best-effort HINCRBY
    # counters into per-minute Redis hashes (never raises into the hot path —
    # same circuit-breaker pattern as Liveness). See KafkaBatch::PerformanceMetrics.
    attr_accessor :performance_metrics_enabled        # Boolean – default false
    # How long (seconds) each per-minute bucket lives before Redis expires it.
    # Also bounds the longest UI range (24h → 86400). Env: KAFKA_BATCH_PERFORMANCE_METRICS_RETENTION.
    attr_accessor :performance_metrics_retention
    # Cap on distinct job_type fields tracked per bucket; overflow is folded
    # into the "_other" field so a runaway number of job types can't bloat a
    # single Redis hash. Env: KAFKA_BATCH_PERFORMANCE_METRICS_MAX_JOB_TYPES.
    attr_accessor :performance_metrics_max_job_types
    # Bucket width in seconds (advanced — changing this after buckets already
    # exist mixes granularities until the old ones expire). Default 60 (1 min).
    # Env: KAFKA_BATCH_PERFORMANCE_METRICS_BUCKET_SECONDS.
    attr_accessor :performance_metrics_bucket_seconds
    # Fraction (0 < x <= 1.0) of events actually written to Redis, for very
    # high-throughput deployments that want to cut write volume. Default 1.0
    # (every event recorded). Env: KAFKA_BATCH_PERFORMANCE_METRICS_SAMPLE_RATE.
    attr_accessor :performance_metrics_sample_rate
    # Cluster-wide Redis RTT probe interval (seconds). Only the NX lock winner
    # issues a PING each tick (~4 probes/min at the default). Used when
    # performance_metrics_enabled. Env: KAFKA_BATCH_REDIS_RTT_PROBE_INTERVAL.
    attr_accessor :redis_rtt_probe_interval
    # Client timeout (seconds) for the RTT PING; timeouts/errors increment the
    # bucket's errors counter. Env: KAFKA_BATCH_REDIS_RTT_PROBE_TIMEOUT.
    attr_accessor :redis_rtt_probe_timeout

    # ── Handler manifest (Go + Ruby routing) ─────────────────────────────────
    # Optional YAML listing handlers (runtime/topic/retries). Loaded at boot
    # when set. Also via ENV KAFKA_BATCH_HANDLER_MANIFEST.
    attr_accessor :handler_manifest_path
    # Extra plain job topics for kafka-batch-go worker (daemon jobs_topics YAML).
    # Used by /lag to list the go-worker-jobs consumer group when manifest is absent.
    attr_accessor :jobs_topics   # Array<String> – default []

    # ── Daemon mode ────────────────────────────────────────────────────────────
    # When true, Karafka consumers are skipped in this process — use on API/client
    # pods. Run control and execution in dedicated Karafka deployments.
    attr_accessor :daemon_mode                 # Boolean – default false

    # ── Passthrough rdkafka config ───────────────────────────────────────────
    attr_accessor :producer_config  # Hash – merged on top of producer defaults
    attr_accessor :consumer_config  # Hash – merged on top of consumer defaults

    # ── Topic validation ─────────────────────────────────────────────────────
    # When true, verify all configured topics exist in Kafka during boot
    # (requires a broker connection at startup). Disabled by default.
    attr_accessor :validate_topics_on_boot  # Boolean – default false

    # ── Web dashboard ──────────────────────────────────────────────────────
    # Optional authentication backstop for KafkaBatch::Web. A callable(env) that
    # returns truthy to allow the request and falsey to reject it with 401. nil
    # (default) means the host app is solely responsible for protecting the mount
    # (e.g. `authenticate :admin do mount … end`). This is defence-in-depth, not
    # a replacement for host-level auth. Example:
    #   config.web_authenticator = ->(env) {
    #     ActionController::HttpAuthentication::Basic.with_credentials(env) { |u, p| … }
    #   }
    attr_accessor :web_authenticator

    # ── AI knowledge index (RAG corpus in Redis) ─────────────────────────────
    # Prebuilt chunks from ai/README.md + ai/FAQ.md are loaded into Redis at
    # boot (see KafkaBatch::Ai::KnowledgeIndex). Many UI pods may call sync!;
    # only one writer wins a short NX lock.
    #   - Knowledge chunks: rewritten only when packaged corpus_version changes
    #   - Config snapshot: refreshed at most every 24h on boot (config knobs change
    #     without a docs release). See lib/kafka_batch/ai/README.md.
    # Assistant must never touch operational ledger/fairness/workset keys — only
    # kafka_batch:ai:knowledge:* / ai:settings / ai:chat:*.
    attr_accessor :ai_knowledge_enabled          # Boolean – default true
    # Salt for AES-GCM encryption of OpenRouter API keys in Redis (AI Settings).
    # Required to save secrets. Prefer ENV KAFKA_BATCH_AI_ENCRYPTION_SALT.
    attr_accessor :ai_encryption_salt
    # Global shared chat history cap (Redis LIST entries). Default 500.
    attr_accessor :ai_chat_history_max_lines
    # How many knowledge chunks to inject into the OpenRouter prompt. Default 6.
    attr_accessor :ai_chat_context_chunks
    # Default OpenRouter model id when UI has not overridden it.
    attr_accessor :ai_openrouter_default_model

    # ── Logging ──────────────────────────────────────────────────────────────
    attr_accessor :logger

    def initialize
      @store                    = :redis
      @topic_prefix             = ""
      @brokers                  = ["localhost:9092"]
      @skip_cancelled_jobs      = true
      @cancellation_cache_ttl   = 120
      @liveness_backend         = :redis
      @track_running_jobs       = true
      # Redis EX TTL on kafka_batch:live:consumer:* (and live:job:*) heartbeats.
      # Override via config.liveness_ttl or KAFKA_BATCH_LIVENESS_TTL (seconds).
      @liveness_ttl                   = env_positive_int("KAFKA_BATCH_LIVENESS_TTL", 180)
      @liveness_heartbeat_interval    = env_positive_int("KAFKA_BATCH_LIVENESS_HEARTBEAT_INTERVAL", 20)
      @liveness_stats_interval        = 15
      @super_fetch_concurrency        = env_positive_int("KAFKA_BATCH_SUPER_FETCH_CONCURRENCY", 1)
      @super_fetch_claim_window       = env_positive_int("KAFKA_BATCH_SUPER_FETCH_CLAIM_WINDOW", 0)
      @super_fetch_lease_ttl          = env_positive_int("KAFKA_BATCH_SUPER_FETCH_LEASE_TTL", 120)
      @super_fetch_orphan_grace       = env_positive_int("KAFKA_BATCH_SUPER_FETCH_ORPHAN_GRACE", 40)
      @super_fetch_reclaim_enabled    = !truthy_env?("KAFKA_BATCH_SUPER_FETCH_RECLAIM_DISABLED")
      @super_fetch_reclaim_interval   = env_positive_int("KAFKA_BATCH_SUPER_FETCH_RECLAIM_INTERVAL", 30)
      @super_fetch_reclaim_limit      = env_positive_int("KAFKA_BATCH_SUPER_FETCH_RECLAIM_LIMIT", 100)
      @super_fetch_drain_timeout      = env_positive_int("KAFKA_BATCH_SUPER_FETCH_DRAIN_TIMEOUT", 30)
      @execution_mode                 = normalize_execution_mode(ENV["KAFKA_BATCH_EXECUTION_MODE"])
      @uniq_enabled             = true
      @uniq_lock_ttl            = 7 * 24 * 3600  # 7 days — covers max_schedule_horizon + retries
      @uniq_on_duplicate        = :skip
      @consumption_control_refresh_interval = 30
      @max_retries              = 7
      @retry_jitter             = 0.1  # +/- 10%
      @retry_tiers              = { short: 30, medium: 7 * 60, large: 20 * 60 }
      @retry_tier_progression   = %i[short medium large]
      @retry_max_pause_seconds  = 30
      @schedule_store           = :redis
      @schedule_poller_enabled  = false
      @schedule_poll_interval   = 5.0
      @schedule_poll_max_interval = 60.0
      @schedule_poll_jitter     = 0.1
      @schedule_batch_size      = 100
      @schedule_lease_seconds   = 60
      @schedule_reclaim_interval = 30
      @recurring_scheduler_enabled = %w[1 true yes].include?(ENV["KAFKA_BATCH_RECURRING_SCHEDULER_ENABLED"].to_s.strip.downcase)
      @recurring_window          = env_positive_float("KAFKA_BATCH_RECURRING_WINDOW", 30.0)
      @recurring_lock_ttl        = env_positive_int("KAFKA_BATCH_RECURRING_LOCK_TTL", 60)
      @recurring_batch_size      = env_positive_int("KAFKA_BATCH_RECURRING_BATCH_SIZE", 100)
      @recurring_misfire_grace   = env_positive_float("KAFKA_BATCH_RECURRING_MISFIRE_GRACE", 60.0)
      @recurring_max_backfill    = env_positive_int("KAFKA_BATCH_RECURRING_MAX_BACKFILL", 1000)
      @recurring_recover_every   = env_positive_float("KAFKA_BATCH_RECURRING_RECOVER_EVERY", 300.0)
      @recurring_recover_grace   = env_positive_float("KAFKA_BATCH_RECURRING_RECOVER_GRACE", 120.0)
      @recurring_prune_every     = env_positive_float("KAFKA_BATCH_RECURRING_PRUNE_EVERY", 3600.0)
      @recurring_prune_retention = env_positive_float("KAFKA_BATCH_RECURRING_PRUNE_RETENTION", 7 * 24 * 3600.0)
      @recurring_heartbeat_every = env_positive_float("KAFKA_BATCH_RECURRING_HEARTBEAT_EVERY", 60.0)
      @recurring_stale_factor    = env_positive_float("KAFKA_BATCH_RECURRING_STALE_FACTOR", 2.0)
      @max_schedule_horizon     = 7 * 24 * 3600  # 7 days (match scheduled_topic retention)
      @topics_replication_factor = 3
      @event_emit_retries       = 3
      @event_emit_backoff       = 1
      @redis_url                = "redis://localhost:6379/0"
      @redis                    = nil
      # SF + renewers (~claim_window) + Karafka floor. Override via redis_pool_size /
      # KAFKA_BATCH_REDIS_POOL_SIZE. See recommended_redis_pool_size.
      @redis_pool_size          = env_positive_int(
        "KAFKA_BATCH_REDIS_POOL_SIZE",
        recommended_redis_pool_size_for(@super_fetch_concurrency, @super_fetch_claim_window)
      )
      @batch_ttl                = 7 * 24 * 3600  # 7 days
      @failures_ttl             = 24 * 3600      # 1 day (metadata only; Kafka is the source of truth)
      @max_failures_per_batch   = 1000           # cap tracked failing jobs per batch (0 = unlimited)
      @retry_cancel_ttl         = 7 * 24 * 3600  # cancel set + skip watermarks TTL
      @fairness_global_concurrency      = 50
      @fairness_max_inflight_per_tenant = 0      # 0 = dynamic fair share only (ceil(window/active))
      @fairness_weighted_concurrency    = true   # false = equal in-flight cap per active tenant
      @fairness_active_count_ttl        = 5      # seconds to cache the smoothed active-tenant count
      @fairness_active_count_source     = :inflight_plus_ready  # or :ingest_lag
      @fairness_ready_window            = 500    # bounded ready jobs per tenant in Redis
      @fairness_default_weight          = 1.0
      @fairness_weight_cache_ttl        = 60
      @fairness_forwarder_idle_sleep    = 0.05
      @fairness_tenant_partitions       = {}
      # Exclusive ingest partitions for hot tenants (static map still wins).
      # Default on; set KAFKA_BATCH_FAIRNESS_DYNAMIC_TENANT_PARTITIONS=false to disable.
      @fairness_dynamic_tenant_partitions =
        if ENV.key?("KAFKA_BATCH_FAIRNESS_DYNAMIC_TENANT_PARTITIONS")
          truthy_env?("KAFKA_BATCH_FAIRNESS_DYNAMIC_TENANT_PARTITIONS")
        else
          true
        end
      @fairness_tenant_partition_cache_ttl = 30
      @fairness_dispatcher_batch_size   = 50
      @fairness_dispatcher_concurrency  = 5
      @fairness_min_ingest_partitions   = 2
      @fairness_lease_ttl               = 1800  # 30 min; must exceed max job runtime
      @fairness_forwarding_recovery_grace = 5.0
      @fairness_slot_dedup_ttl          = 0     # 0 → use fairness_lease_ttl
      @priority_lag_check_interval = 2
      @priority_config_paths         = []
      @priority_weighted_interleave  = 4
      @reconciliation_interval  = 300
      @reconciler_lock_ttl      = 600
      @max_reconcile_per_run    = 100
      @all_index_max_size       = 200_000
      @max_message_bytes        = 1_048_576  # 1 MiB; set to 0 to disable
      @push_many_chunk_size     = 500
      @store_database_connection            = nil
      @schedule_store_database_connection   = nil
      @schedule_index_write_retries         = 3
      @schedule_index_write_backoff         = 0.05
      @audit_enabled                        = false
      @audit_database_connection            = nil
      @audit_actor                          = nil
      @metrics_enabled                      = false
      @metrics_adapter                      = :statsd
      @metrics_client                       = nil
      @metrics_proc                         = nil
      @metrics_prefix                       = "kafka_batch"
      @performance_metrics_enabled          = truthy_env?("KAFKA_BATCH_PERFORMANCE_METRICS_ENABLED")
      @performance_metrics_retention        = env_positive_int("KAFKA_BATCH_PERFORMANCE_METRICS_RETENTION", 24 * 3600)
      @performance_metrics_max_job_types    = env_positive_int("KAFKA_BATCH_PERFORMANCE_METRICS_MAX_JOB_TYPES", 50)
      @performance_metrics_bucket_seconds   = env_positive_int("KAFKA_BATCH_PERFORMANCE_METRICS_BUCKET_SECONDS", 60)
      @performance_metrics_sample_rate      = env_positive_float("KAFKA_BATCH_PERFORMANCE_METRICS_SAMPLE_RATE", 1.0)
      @redis_rtt_probe_interval             = env_positive_float("KAFKA_BATCH_REDIS_RTT_PROBE_INTERVAL", 15.0)
      @redis_rtt_probe_timeout              = env_positive_float("KAFKA_BATCH_REDIS_RTT_PROBE_TIMEOUT", 0.2)
      @producer_config          = {}
      @consumer_config          = {}
      @validate_topics_on_boot  = false
      @extra_job_topics         = []
      @web_authenticator        = nil
      @daemon_mode              = truthy_env?("KAFKA_BATCH_DAEMON_MODE")
      @handler_manifest_path    = ENV["KAFKA_BATCH_HANDLER_MANIFEST"].to_s.strip
      @jobs_topics              = []
      @ai_knowledge_enabled =
        if ENV.key?("KAFKA_BATCH_AI_KNOWLEDGE_ENABLED")
          truthy_env?("KAFKA_BATCH_AI_KNOWLEDGE_ENABLED")
        else
          true
        end
      @ai_encryption_salt         = ENV["KAFKA_BATCH_AI_ENCRYPTION_SALT"].to_s
      @ai_chat_history_max_lines  = env_positive_int("KAFKA_BATCH_AI_CHAT_HISTORY_MAX_LINES", 500)
      @ai_chat_context_chunks     = env_positive_int("KAFKA_BATCH_AI_CHAT_CONTEXT_CHUNKS", 6)
      @ai_openrouter_default_model = ENV["KAFKA_BATCH_AI_OPENROUTER_DEFAULT_MODEL"].to_s.strip
      @logger                   = Logger.new($stdout).tap { |l| l.progname = "KafkaBatch" }
    end

    # ── Retry tier helpers ───────────────────────────────────────────────────

    # Kafka topic for a given retry tier, e.g. "kafka_batch.jobs.retry.short".
    def retry_topic_for(tier)
      "#{retry_topic}.#{tier}"
    end

    # All tier retry topics (one per configured tier).
    def retry_topics
      retry_tiers.keys.map { |t| retry_topic_for(t) }
    end

    # Tier for the upcoming retry. A worker override (if it's a valid tier) wins;
    # otherwise walk the progression by retry index (1st retry → progression[0]),
    # clamping to the last tier for all further retries.
    # @return [Symbol]
    def retry_tier_for(next_attempt, worker_tier = nil)
      wt = worker_tier&.to_sym
      return wt if wt && retry_tiers.key?(wt)

      prog = retry_tier_progression
      prog[[next_attempt.to_i - 1, prog.size - 1].min] || prog.last
    end

    # Delay (seconds) for a tier, with +/- retry_jitter applied to avoid storms.
    def retry_delay_for(tier)
      base = retry_tiers.fetch(tier.to_sym) { retry_tiers.values.first }.to_f
      j    = retry_jitter.to_f
      return base if j <= 0

      base * (1 + ((rand * 2) - 1) * j)
    end

    def validate!
      raise ConfigurationError, "store must be :mysql or :redis" unless %i[mysql redis].include?(@store)
      unless %i[mysql redis].include?(@schedule_store)
        raise ConfigurationError, "schedule_store must be :mysql or :redis"
      end
      raise ConfigurationError, "brokers must not be empty"       if Array(@brokers).empty?

      unless %i[superfetch watermark].include?(@execution_mode)
        raise ConfigurationError, "execution_mode must be :superfetch or :watermark"
      end

      unless %i[redis off].include?(@liveness_backend)
        raise ConfigurationError, "liveness_backend must be :redis or :off"
      end

      if @liveness_stats_interval.to_i.negative?
        raise ConfigurationError, "liveness_stats_interval must be >= 0"
      end

      # Redis is a HARD dependency: the multi-tenant fairness scheduler (WFQ ring,
      # in-flight counters, per-tenant ready windows) lives entirely in Redis, and
      # it drives the default fairness path.
      unless redis_configured?
        raise ConfigurationError,
          "Redis is required by KafkaBatch (fairness scheduler + liveness). " \
          "Set config.redis_url or config.redis."
      end

      # The time-fairness lane advances vtime only at completion, so a tenant is
      # kept from seizing the whole in-flight window by the dynamic fair-share cap
      # (ceil(global_concurrency / active_tenants)). That requires a finite global
      # window; if BOTH the window is unbounded (global_concurrency <= 0) AND no
      # hard per-tenant cap is set, a single tenant could monopolise the time lane.
      if @fairness_global_concurrency.to_i <= 0 && @fairness_max_inflight_per_tenant.to_i <= 0
        raise ConfigurationError,
              "The time-fairness lane requires either fairness_global_concurrency > 0 " \
              "(recommended — enables the dynamic fair-share cap) or " \
              "fairness_max_inflight_per_tenant > 0. Otherwise a single tenant can " \
              "monopolise the in-flight window (vtime only advances at completion)."
      end

      if @metrics_enabled
        adapter = @metrics_adapter
        unless %i[statsd datadog proc].include?(adapter)
          raise ConfigurationError, "metrics_adapter must be :statsd, :datadog, or :proc"
        end
        client = @metrics_proc || @metrics_client
        if adapter == :proc
          raise ConfigurationError, "metrics_proc or metrics_client callable required for :proc adapter" unless client.respond_to?(:call)
        elsif client.nil?
          raise ConfigurationError, "metrics_client required when metrics_enabled is true"
        end
      end

      if @performance_metrics_enabled
        if @performance_metrics_retention.to_i <= 0
          raise ConfigurationError, "performance_metrics_retention must be > 0"
        end
        if @performance_metrics_max_job_types.to_i <= 0
          raise ConfigurationError, "performance_metrics_max_job_types must be > 0"
        end
        if @performance_metrics_bucket_seconds.to_i <= 0
          raise ConfigurationError, "performance_metrics_bucket_seconds must be > 0"
        end
        rate = @performance_metrics_sample_rate.to_f
        if rate <= 0 || rate > 1.0
          raise ConfigurationError, "performance_metrics_sample_rate must be in (0, 1.0]"
        end
        if @redis_rtt_probe_interval.to_f <= 0
          raise ConfigurationError, "redis_rtt_probe_interval must be > 0"
        end
        if @redis_rtt_probe_timeout.to_f <= 0
          raise ConfigurationError, "redis_rtt_probe_timeout must be > 0"
        end
      end
    end

    # Apply topic_prefix to a topic name from a priority YAML file.
    # @param name [String]
    # @return [String]
    def resolve_topic(name)
      name = name.to_s.strip
      return name if name.empty?

      p = @topic_prefix.to_s.strip
      return name if p.empty? || name.start_with?("#{p}.")

      "#{p}.#{name}"
    end

    # All priority YAML paths from config + ENV.
    # @return [Array<String>]
    def resolved_handler_manifest_path
      path = @handler_manifest_path.to_s.strip
      return nil if path.empty?

      File.expand_path(path)
    end

    def resolved_priority_config_paths
      paths = Array(@priority_config_paths).map(&:to_s).map(&:strip).reject(&:empty?)
      single = ENV["KAFKA_BATCH_PRIORITY_CONFIG"].to_s.strip
      paths << single unless single.empty?
      multi = ENV["KAFKA_BATCH_PRIORITY_CONFIGS"].to_s.strip
      unless multi.empty?
        paths.concat(multi.split(",").map(&:strip).reject(&:empty?))
      end
      paths.map { |p| File.expand_path(p) }.uniq
    end

    def daemon_mode?
      @daemon_mode == true
    end

    # Recommended ConnectionPool size for SuperFetch + renewers + Karafka.
    # Floor 16; scales with SF concurrency and claim window.
    def recommended_redis_pool_size
      self.class.recommended_redis_pool_size_for(
        @super_fetch_concurrency,
        @super_fetch_claim_window
      )
    end

    def self.recommended_redis_pool_size_for(sf, claim_window)
      sf = sf.to_i
      sf = 1 if sf < 1
      win = claim_window.to_i
      win = sf * 2 if win < sf
      # perform slots + renewers (~window) + Karafka floor + misc
      [sf + win + 12, 16].max
    end

    # True when tier-3 job execution should use the Redis-free watermark executor.
    def watermark_mode?
      @execution_mode == :watermark
    end

    private

    # Accepts a String/Symbol/nil and returns :superfetch (default) or :watermark.
    # An unrecognized non-empty value is preserved so validate! can reject it with
    # a clear message rather than silently defaulting.
    def normalize_execution_mode(value)
      s = value.to_s.strip.downcase
      return :superfetch if s.empty?

      s.to_sym
    end

    def truthy_env?(key)
      %w[1 true yes].include?(ENV[key].to_s.strip.downcase)
    end

    def env_positive_int(key, default)
      v = ENV[key].to_s.strip
      return default if v.empty?

      n = Integer(v, 10)
      n.positive? ? n : default
    rescue ArgumentError, TypeError
      default
    end

    def env_positive_float(key, default)
      v = ENV[key].to_s.strip
      return default if v.empty?

      n = Float(v)
      n.positive? ? n : default
    rescue ArgumentError, TypeError
      default
    end

    def recommended_redis_pool_size_for(sf, claim_window)
      self.class.recommended_redis_pool_size_for(sf, claim_window)
    end

    # Apply topic_prefix to a base name: "" → base, "myapp" → "myapp.base".
    def prefixed(base)
      p = @topic_prefix.to_s.strip
      p.empty? ? base : "#{p}.#{base}"
    end
  end
end
