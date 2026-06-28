KafkaBatch.configure do |config|
  # ── State store ────────────────────────────────────────────────────────────
  # :mysql  – persistent, queryable, survives Redis restarts
  #           requires running: rails g kafka_batch:install --store mysql
  # :redis  – lower latency, no schema migration needed
  config.store = :mysql

  # ── Kafka brokers ──────────────────────────────────────────────────────────
  config.brokers = (ENV["KAFKA_BROKERS"] || "localhost:9092").split(",")

  # ── Topic names ────────────────────────────────────────────────────────────
  # Change these if you want a namespace prefix, e.g. "myapp.kafka_batch.jobs"
  config.jobs_topic        = "kafka_batch.jobs"
  config.events_topic      = "kafka_batch.events"
  config.callbacks_topic   = "kafka_batch.callbacks"
  config.dead_letter_topic = "kafka_batch.dead_letter"
  config.retry_topic       = "kafka_batch.jobs.retry"

  # ── Consumer group ─────────────────────────────────────────────────────────
  config.consumer_group = "kafka-batch"

  # ── Cancellation ───────────────────────────────────────────────────────────
  # When true, JobConsumer skips jobs whose batch was cancelled. Cancelled batch
  # ids are cached per process and refreshed at most once per
  # cancellation_cache_ttl seconds (no per-job store read), so cancellation is
  # eventually-consistent within that window.
  config.skip_cancelled_jobs    = true
  config.cancellation_cache_ttl = 120  # seconds

  # ── Live activity (running jobs / consumers dashboard) ───────────────────────
  # Backend for the /live page:
  #   :redis – (default) full per-job tracking in Redis (config.redis_url), TTL'd.
  #   :store – consumer heartbeat + sampled current job in the configured store
  #            (low-impact; needs the consumer_heartbeats migration on MySQL).
  #   :off   – disabled.
  config.liveness_backend = :redis
  config.track_running_jobs = true   # gates the per-job :redis writes
  config.liveness_ttl       = 30     # seconds (staleness window)
  config.liveness_heartbeat_interval = 5  # seconds (:store write throttle)

  # ── Retry behaviour ────────────────────────────────────────────────────────
  # Fixed, short schedule (Kafka-friendly): 1st retry after retry_first_delay,
  # every later retry after retry_delay, with +/- retry_jitter randomization.
  config.max_retries      = 3    # attempts before dead letter (override per Worker)
  config.retry_first_delay = 10  # seconds before the 1st retry
  config.retry_delay       = 180 # seconds before each later retry (3 min)
  config.retry_jitter      = 0.1 # +/- 10% to avoid retry storms

  # After this many retries a still-failing job counts toward its batch's
  # on_complete (counted as failed) so the batch needn't wait for the full retry
  # budget — the job keeps retrying up to max_retries in the background. Default
  # 3 == max_retries default, so default behaviour is unchanged; set max_retries
  # higher (e.g. 20) and this lower (e.g. 3) to cut on_complete latency.
  # on_success is unaffected (still fires only when every job truly succeeds).
  config.complete_after_retries = 3

  # ── Completion-event emission retries ──────────────────────────────────────
  # Inline retries when producing the post-job completion event fails. The
  # backoff sleeps on the Karafka worker thread, so keep retries * backoff small.
  config.event_emit_retries = 3   # attempts
  config.event_emit_backoff = 2   # seconds; linear: attempt * backoff

  # ── Redis (only used when store: :redis) ──────────────────────────────────
  config.redis_url       = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  config.redis_pool_size = 5
  config.batch_ttl       = 7 * 24 * 3600  # 7 days; set nil to never expire

  # ── Failure metadata retention (Redis) ────────────────────────────────────
  # Failure records are only a dashboard convenience – the real job data is
  # durable in Kafka (retry + dead-letter topics). Keep this metadata short and
  # bounded so it can't grow Redis RAM unbounded. At the cap, new failing jobs
  # stop being recorded (existing ones still update); you just may not see every
  # failure in the UI.
  config.failures_ttl           = 24 * 3600  # seconds; metadata retention
  config.max_failures_per_batch = 1000       # 0 = unlimited

  # ── Multi-tenant fairness (Kafka-only; NO Redis required) ──────────────────
  # Share capacity dynamically across tenants: 1 active tenant uses 100%, N split
  # ~1/N (work-conserving, approximate). Jobs land on the ingest topic (keyed by
  # tenant); the Dispatcher (auto-wired by draw_routes) forwards them onto the
  # ready topic — throttled so its depth stays between the watermarks — and the
  # normal JobConsumer swarm drains it. Tag jobs via
  # Batch.create(tenant_id: "...") / batch.push(Worker, payload, tenant_id: "...").
  config.fairness_enabled        = false  # opt-in
  config.fairness_ingest_topic   = "kafka_batch.ingest"  # per-tenant intake (durable backlog in Kafka)
  config.fairness_ready_topic    = "kafka_batch.ready"   # throttled execution queue
  config.fairness_ready_lag_high = 5000   # dispatcher pauses forwarding above this depth
  config.fairness_ready_lag_low  = 1000   # ...resumes below this depth
  # Tenants are hashed to ingest partitions, so the ingest topic needs enough
  # partitions (≈ max concurrent tenants) or tenants collide and fairness
  # degrades. You MUST pre-create the topic with enough partitions; this boot
  # check warns (raises under validate_topics_on_boot) if it has fewer:
  config.fairness_min_ingest_partitions = 2

  # The settings below apply ONLY to the optional Redis-backed
  # KafkaBatch::Fairness::Scheduler (strict weighted shares) — NOT the default
  # dispatcher above, which needs no Redis. Leave them unless you build on it.
  # config.fairness_global_concurrency      = 50
  # config.fairness_max_inflight_per_tenant = 0
  # config.fairness_ready_window            = 500
  # config.fairness_default_weight          = 1.0

  # ── Reconciliation ─────────────────────────────────────────────────────────
  # Batches stuck in "running" older than this threshold are re-evaluated.
  # Trigger via: rake kafka_batch:reconcile (or a cron job)
  config.reconciliation_interval = 300  # seconds
  # Distributed-lock TTL for a single reconciler sweep (max expected runtime).
  config.reconciler_lock_ttl     = 600  # seconds

  # ── Advanced: raw rdkafka / WaterDrop config overrides ────────────────────
  # config.producer_config = { "compression.type" => "snappy" }
  # config.consumer_config = { "fetch.min.bytes"  => "1024"   }

  # ── Logging ────────────────────────────────────────────────────────────────
  # Defaults to Rails.logger when running inside Rails
  # config.logger = Logger.new($stdout)
end
