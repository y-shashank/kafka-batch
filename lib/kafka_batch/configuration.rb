module KafkaBatch
  class Configuration
    # ── Store ────────────────────────────────────────────────────────────────
    # :mysql  – uses ActiveRecord (requires kafka_batch migrations)
    # :redis  – uses Redis (no migrations needed)
    attr_accessor :store

    # ── Kafka connection ─────────────────────────────────────────────────────
    attr_accessor :brokers          # Array<String>  e.g. ["localhost:9092"]

    # ── Topic names ──────────────────────────────────────────────────────────
    attr_accessor :jobs_topic       # String  default: "kafka_batch.jobs"
    attr_accessor :events_topic     # String  default: "kafka_batch.events"
    attr_accessor :callbacks_topic  # String  default: "kafka_batch.callbacks"
    attr_accessor :dead_letter_topic # String  default: "kafka_batch.dead_letter"

    # ── Retry topic ──────────────────────────────────────────────────────────
    # Failed jobs are forwarded here with a retry_after timestamp instead of
    # sleeping inside the job consumer (which would block the Kafka partition).
    # The RetryConsumer waits via Karafka pause() then re-enqueues to the
    # original topic.
    attr_accessor :retry_topic       # String  default: "kafka_batch.jobs.retry"

    # ── Consumer ─────────────────────────────────────────────────────────────
    attr_accessor :consumer_group   # String

    # ── Cancellation ─────────────────────────────────────────────────────────
    # When true, JobConsumer skips execution of jobs whose batch was cancelled.
    # The set of cancelled batch ids is cached per process and refreshed at most
    # once per cancellation_cache_ttl seconds (NOT read from the store on every
    # job), so cancellation takes effect within that window – some already-queued
    # jobs may still run before the next refresh, which is an accepted trade-off.
    attr_accessor :skip_cancelled_jobs   # Boolean – default true
    attr_accessor :cancellation_cache_ttl  # Integer – seconds; default 120

    # ── Liveness (running jobs / consumers dashboard) ─────────────────────────
    # Visibility into currently-running jobs and live consumer processes.
    #   :redis – (default) full per-job tracking in Redis (config.redis_url),
    #            short TTL, best-effort. Most detailed; needs Redis.
    #   :store – consumer heartbeat + sampled "current job" in the configured
    #            store (e.g. MySQL). Bounded, low-impact (writes scale with
    #            consumers, NOT job throughput); reliable via last_seen + sweep.
    #   :off   – disabled.
    attr_accessor :liveness_backend            # Symbol – default :redis
    attr_accessor :track_running_jobs          # Boolean – default true (gates :redis writes)
    attr_accessor :liveness_ttl                # Integer – seconds; default 30 (staleness window)
    attr_accessor :liveness_heartbeat_interval # Integer – seconds; default 5 (:store write throttle)

    # ── Retry behaviour ──────────────────────────────────────────────────────
    # Fixed, short retry schedule (Kafka-friendly): the 1st retry after
    # retry_first_delay, every subsequent retry after retry_delay, with optional
    # +/- retry_jitter to avoid synchronized retry storms.
    attr_accessor :max_retries        # Integer – attempts before dead letter (worker can override)
    attr_accessor :retry_first_delay  # Integer – seconds before the 1st retry (default 10)
    attr_accessor :retry_delay        # Integer – seconds before each later retry (default 180)
    attr_accessor :retry_jitter       # Float   – +/- fraction of randomization (default 0.1)

    # After this many retries a still-failing job counts toward its batch's
    # on_complete (as failed) so the batch needn't wait for the full retry budget
    # — the job keeps retrying in the background up to max_retries. Set equal to
    # (or above) max_retries to disable early completion (the default 3 == default
    # max_retries, so default behaviour is unchanged). on_success is unaffected:
    # it still fires only when every job truly succeeds.
    attr_accessor :complete_after_retries  # Integer – default 3 (worker can override)

    # ── Completion-event emission retries ────────────────────────────────────
    # After a job succeeds, the consumer produces a completion event. If that
    # produce fails (transient Kafka issue) it is retried inline before giving
    # up and leaving the offset uncommitted for redelivery. These tune that
    # inline retry. NOTE: the backoff sleeps on the Karafka worker thread, so
    # keep the product (retries * backoff) modest.
    attr_accessor :event_emit_retries  # Integer – attempts; default 3
    attr_accessor :event_emit_backoff  # Integer – seconds; linear: attempt * backoff

    # ── Redis (only when store: :redis) ─────────────────────────────────────
    attr_accessor :redis_url        # String  e.g. "redis://localhost:6379/0"
    attr_accessor :redis_pool_size  # Integer

    # ── TTL for batch metadata in Redis ─────────────────────────────────────
    attr_accessor :batch_ttl        # Integer – seconds; default 7 days

    # ── Failure metadata retention (Redis store) ────────────────────────────
    # Failure records are only a convenience view for the dashboard – the real
    # job data is durable in Kafka (retry topic / dead-letter topic). To bound
    # Redis RAM you can keep this metadata for less time and/or cap how many
    # failing jobs are tracked per batch. When the cap is hit, additional NEW
    # failing jobs are not recorded (existing ones still update); the feature
    # keeps working, you just may not see every failure in the UI.
    attr_accessor :failures_ttl              # Integer – seconds; default 1 day
    attr_accessor :max_failures_per_batch    # Integer – 0 = unlimited; default 1000

    # ── Multi-tenant fairness (Kafka-only; NO Redis required) ────────────────
    # When enabled, capacity is shared dynamically across tenants — one active
    # tenant uses 100%, N split ~1/N (work-conserving, approximate) — using only
    # Kafka (ingest topic → Dispatcher → ready topic → JobConsumer swarm). The
    # durable backlog stays in Kafka; nothing is stored in Redis on this path.
    attr_accessor :fairness_enabled                 # Boolean – default false
    # The four settings below apply ONLY to the optional Redis-backed
    # KafkaBatch::Fairness::Scheduler (strict weighted shares), NOT the default
    # dispatcher, which uses the ingest/ready topics + watermarks above.
    attr_accessor :fairness_global_concurrency      # Integer – (Scheduler) total in-flight slots; default 50
    attr_accessor :fairness_max_inflight_per_tenant # Integer – (Scheduler) per-tenant cap; 0 = none (default)
    attr_accessor :fairness_ready_window            # Integer – (Scheduler) bounded ready jobs/tenant in Redis; default 500
    attr_accessor :fairness_default_weight          # Numeric – (Scheduler) default share weight; default 1.0

    # Kafka-ready-topic design: jobs land on the ingest topic (keyed
    # one-tenant-per-partition); a Dispatcher forwards them onto the ready topic
    # which a swarm of normal JobConsumers drains. The dispatcher throttles so the
    # ready topic's un-consumed depth stays between the low/high watermarks (this
    # is what keeps fairness dynamic). No Redis on the path.
    attr_accessor :fairness_ingest_topic   # String – default "kafka_batch.ingest"
    attr_accessor :fairness_ready_topic    # String – default "kafka_batch.ready"
    attr_accessor :fairness_ready_lag_high # Integer – pause forwarding above this; default 5000
    attr_accessor :fairness_ready_lag_low  # Integer – resume forwarding below this; default 1000

    # Tenants are spread across the ingest topic's partitions by key hash
    # (key = tenant_id). With too few partitions tenants collide onto the same
    # partition and fairness degrades (1 partition = none at all). The boot check
    # warns (or raises under validate_topics_on_boot) if the ingest topic has
    # fewer partitions than this. Set it near your max concurrent tenant count.
    attr_accessor :fairness_min_ingest_partitions # Integer – default 2

    # ── Reconciliation ───────────────────────────────────────────────────────
    # A periodic sweep that re-checks "running" batches that look stuck.
    attr_accessor :reconciliation_interval  # Integer – seconds; default 300

    # Max time a single reconciler sweep is expected to take. Used purely as
    # the distributed-lock TTL so a crashed reconciler eventually releases the
    # lock. Kept independent of the staleness threshold above.
    attr_accessor :reconciler_lock_ttl      # Integer – seconds; default 600

    # ── Passthrough rdkafka config ───────────────────────────────────────────
    # Merged on top of defaults for the producer.
    attr_accessor :producer_config  # Hash<String, Object>

    # Merged on top of defaults for every consumer.
    attr_accessor :consumer_config  # Hash<String, Object>

    # ── Topic validation ─────────────────────────────────────────────────────
    # When true, KafkaBatch verifies that all configured topics exist in Kafka
    # during Rails boot (requires a working broker connection at startup).
    # Disabled by default to avoid blocking startup in test/CI environments.
    attr_accessor :validate_topics_on_boot  # Boolean  default: false

    # ── Logging ──────────────────────────────────────────────────────────────
    attr_accessor :logger

    def initialize
      @store                    = :mysql
      @skip_cancelled_jobs      = true
      @cancellation_cache_ttl   = 120
      @liveness_backend            = :redis
      @track_running_jobs          = true
      @liveness_ttl                = 30
      @liveness_heartbeat_interval = 5
      @brokers                  = ["localhost:9092"]
      @jobs_topic               = "kafka_batch.jobs"
      @events_topic             = "kafka_batch.events"
      @callbacks_topic          = "kafka_batch.callbacks"
      @dead_letter_topic        = "kafka_batch.dead_letter"
      @retry_topic              = "kafka_batch.jobs.retry"
      @consumer_group           = "kafka-batch"
      @max_retries              = 3
      @retry_first_delay        = 10   # seconds
      @retry_delay              = 180  # seconds (3 minutes)
      @retry_jitter             = 0.1  # +/- 10%
      @complete_after_retries   = 3    # == max_retries default → no early completion by default
      @event_emit_retries       = 3
      @event_emit_backoff       = 2
      @redis_url                = "redis://localhost:6379/0"
      @redis_pool_size          = 5
      @batch_ttl                = 7 * 24 * 3600  # 7 days
      @failures_ttl             = 24 * 3600      # 1 day (metadata only; Kafka is the source of truth)
      @max_failures_per_batch   = 1000           # cap tracked failing jobs per batch (0 = unlimited)
      @fairness_enabled                 = false
      @fairness_global_concurrency      = 50
      @fairness_max_inflight_per_tenant = 0     # 0 = no per-tenant cap (rely on WFQ)
      @fairness_ready_window            = 500   # bounded ready jobs per tenant in Redis
      @fairness_default_weight          = 1.0
      @fairness_ingest_topic            = "kafka_batch.ingest"
      @fairness_ready_topic             = "kafka_batch.ready"
      @fairness_ready_lag_high          = 5000
      @fairness_ready_lag_low           = 1000
      @fairness_min_ingest_partitions   = 2
      @reconciliation_interval  = 300
      @reconciler_lock_ttl      = 600
      @producer_config          = {}
      @consumer_config          = {}
      @validate_topics_on_boot  = false
      @logger                   = Logger.new($stdout).tap { |l| l.progname = "KafkaBatch" }
    end

    def validate!
      raise ConfigurationError, "store must be :mysql or :redis" unless %i[mysql redis].include?(@store)
      raise ConfigurationError, "brokers must not be empty"       if Array(@brokers).empty?

      unless %i[redis store off].include?(@liveness_backend)
        raise ConfigurationError, "liveness_backend must be :redis, :store, or :off"
      end

      if @store == :redis
        raise ConfigurationError, "redis_url must be set for :redis store" if @redis_url.nil? || @redis_url.empty?
      end
    end
  end
end
