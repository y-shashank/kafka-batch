# frozen_string_literal: true
#
# KafkaBatch configuration. Only the settings most installs care about are shown
# below — every other setting ships with a sensible default (see the README
# "Configuration reference", or KafkaBatch::Configuration, for the full list and
# how to tune retries, reconciliation, the fairness scheduler internals, etc.).
#
# Loaded in EVERY process (web, Sidekiq, Karafka). The Karafka server additionally
# `require "kafka_batch"` at the top of karafka.rb for the full backend.
require "kafka_batch/ui"

KafkaBatch.configure do |config|
  # ── State store ─────────────────────────────────────────────────────────────
  # :redis – (default) all batch ledger state in Redis; no migrations.
  # :mysql – batch ledger still in Redis; run `rails g kafka_batch:install --store mysql`
  #          then `rails db:migrate` for failures / pause tables.
  config.store = :<%= @store %>
  # config.store_database_connection = :kafka_batch_ops   # database.yml name, AR class, or Hash

  # ── Delayed-job (perform_in / perform_at) index store ─────────────────────────
  # Detached from `store` — the main ledger can be Redis while the (potentially
  # huge) schedule index lives on cheap MySQL disk.
  # :redis – (default) ZSET-based index, RAM-resident, lowest latency.
  # :mysql – kafka_batch_scheduled_jobs table; run with
  #          `rails g kafka_batch:install --schedule-store mysql` then `rails db:migrate`.
  config.schedule_store = :<%= @schedule_store %>
  # config.schedule_store_database_connection = :kafka_batch_schedule

  # Delayed-job poller. Disabled by default (config.schedule_poller_enabled = false).
  # Enable on scheduler pods — or on every pod in dev when KB_ROLE=all. For high pod
  # counts with schedule_store=:mysql, dedicate 2–3 scheduler pods and leave this
  # false on execution swarms so they don't all query MySQL.
  roles = ENV.fetch("KB_ROLE", "all").split(",").map(&:strip)
  config.schedule_poller_enabled =
    case ENV["KB_SCHEDULE_POLLER"]
    when "true"  then true
    when "false" then false
    else (roles & %w[all scheduler]).any?
    end
  #
  # Idle pods back off automatically (schedule_poll_interval → schedule_poll_max_interval)
  # so they don't hammer the store when nothing is due; jitter de-syncs them.
  # config.schedule_poll_interval     = 5.0    # base poll cadence when work is flowing
  # config.schedule_poll_max_interval = 60.0   # idle backoff ceiling (per pod)

  # ── Kafka brokers ─────────────────────────────────────────────────────────────
  config.brokers = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")

  # ── Redis (REQUIRED) ──────────────────────────────────────────────────────────
  # Redis is a hard dependency: it backs the multi-tenant fairness scheduler and
  # the live-activity dashboard (and, with store: :redis, all batch state).
  config.redis_url       = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  # Or a Rails-style hash (mutually exclusive with redis_url):
  # config.redis = { host: "localhost", port: 6379, db: 0 }
  # Size for ~150 consumer pods (each doing concurrent Redis work). Raise if you
  # see pool checkout timeouts under load.
  config.redis_pool_size = 10

  # ── Topic namespace ─────────────────────────────────────────────────────────
  # All topic names AND the consumer group derive from this prefix, so a single
  # setting namespaces everything (e.g. "myapp" → "myapp.kafka_batch.jobs",
  # consumer group "myapp.kafka-batch"). Leave blank for the bare defaults.
  config.topic_prefix = ENV["KAFKA_PREFIX"].to_s.strip

  # Custom plain-worker topics a UI-only dashboard should show on the /lag page.
  # Only needed for a dedicated web service (require: "kafka_batch/ui") that never
  # calls draw_routes — worker processes discover these from their routes. Verbatim.
  # config.extra_job_topics = %w[orders.process reports.rebuild]

  # ── Retries ─────────────────────────────────────────────────────────────────
  # Tiered retries: the Nth retry walks short → medium → large, each on its own
  # Kafka topic so a slow tier never head-of-line-blocks a fast one. A Worker can
  # override with `max_retries` / `retry_tier`.
  config.max_retries = 3
  # config.retry_tiers = { short: 30, medium: 7 * 60, large: 20 * 60 }  # seconds

  # ── Multi-tenant fairness (opt in per-worker) ─────────────────────────────────
  # Redis-backed Weighted-Fair-Queuing. There are TWO lanes; a worker opts into
  # one and both run simultaneously (a single batch may mix both):
  #
  #   class MyWorker
  #     include KafkaBatch::Worker
  #     fairness_type :time        # weighted wall-clock time (default; uneven runtimes)
  #     # fairness_type :throughput  # weighted job count (similar runtimes)
  #   end
  #
  # One active tenant uses 100% of the in-flight window; N split it evenly
  # (work-conserving). The knobs below apply to EACH lane independently.
  #
  # Rule of thumb: set this ≥ (fair-lane execution pods × karafka concurrency),
  # or ≥ (target fair throughput jobs/sec × p99 job duration seconds). The library
  # default is 50 (dev-sized); 1000 fits ~150 pods at concurrency ~7 with headroom.
  config.fairness_global_concurrency = 1000  # in-flight window per lane
  # config.fairness_max_inflight_per_tenant = 0   # optional hard per-tenant ceiling (0 = dynamic share)
  # In-flight slots are leases: if a consumer is hard-killed mid-job the slot is
  # reclaimed when this TTL expires, so a lane never stays wedged. MUST exceed your
  # longest job runtime (a longer job's slot is reclaimed early — soft overshoot).
  config.fairness_lease_ttl = 7200   # 2 hours — raise for longer jobs
  # Boot check: warn/raise if fair ingest has fewer partitions than this.
  # config.fairness_min_ingest_partitions = 300

  # Per-tenant weights control throughput share (edit live on /kafka_batch/weights).
  # Default is true — a weight-N tenant gets ~N× the in-flight concurrency of a
  # weight-1 tenant under saturation. Set false for equal cap per active tenant.
  # config.fairness_weighted_concurrency = false
  # config.fairness_weight_cache_ttl = 60   # secs before a weight change propagates across pods

  # Pin specific tenants to ingest partitions (common to both lanes):
  # config.fairness_tenant_partitions = { "acme" => 0, "globex" => 1 }
  #
  # Or let the system assign partitions automatically on first enqueue (Redis-backed,
  # one dedicated partition per tenant until the ingest topic is full):
  # config.fairness_dynamic_tenant_partitions = true
  # config.fairness_tenant_partition_cache_ttl = 30  # in-process lookup cache (seconds)

  # ── Priority queues (Sidekiq.yml-style, optional) ─────────────────────────────
  # One YAML file per consumer group; topics listed highest-priority first.
  # Workers opt in with kafka_topic; each topic may belong to exactly one group.
  # Also set via ENV KAFKA_BATCH_PRIORITY_CONFIG (one path) or
  # KAFKA_BATCH_PRIORITY_CONFIGS (comma-separated). Example files ship in
  # config/kafka_batch/priority/.
  # config.priority_config_paths = [
  #   Rails.root.join("config/kafka_batch/priority/jobs-fast.yml").to_s
  # ]
  # config.priority_lag_check_interval  = 2   # seconds between lag re-checks
  # config.priority_weighted_interleave = 4   # weighted mode: 1-in-N lower-rank jobs

  # ── /lag pause/resume ─────────────────────────────────────────────────────────
  # How often Karafka consumers re-read pause state from Redis/MySQL (default 30s).
  # Lower = pause/resume takes effect faster; higher = fewer Redis reads.
  # config.consumption_control_refresh_interval = 30

  # ── Handler manifest (runtime routing for Go + Ruby) ───────────────────────
  # config.handler_manifest_path = Rails.root.join("config/kafka_batch_handlers.yml").to_s

  # ── Producer safety ───────────────────────────────────────────────────────────
  # Raise a clear ProducerError instead of an opaque rdkafka error on oversized
  # payloads. 0/nil disables. Matches Kafka's typical 1 MiB broker default.
  config.max_message_bytes = 1_048_576

  # ── Kafka topic sizing ────────────────────────────────────────────────────────
  # Partition counts are fixed at topic creation. bin/create_kafka_topics.sh (and
  # rake kafka_batch:create_topics) default to ~150 pods × concurrency 10:
  #   jobs / priority / fair ready → 768   events → 48   fair ingest → 300
  # Override before first deploy, e.g. PARTITIONS=1500 ./bin/create_kafka_topics.sh
  # (forces one count for every topic — prefer per-topic tuning in the shell script).

  # ── Topic validation ──────────────────────────────────────────────────────────
  # Verify all topics exist in Kafka at boot (needs a broker connection). Off by
  # default so test/CI boot without a broker.
  config.validate_topics_on_boot = false

  # ── Logging ───────────────────────────────────────────────────────────────────
  # Defaults to Rails.logger under Rails.
  # config.logger = Logger.new($stdout)

  # ── Other settings (sensible defaults; uncomment to tune) ─────────────────────
  # config.consumer_group          = "kafka-batch"   # overrides the prefix-derived name
  # config.liveness_backend        = :redis          # or :off
  # config.liveness_ttl                 = 180             # seconds; Redis TTL on live:consumer:* heartbeats
  #                                                       # (env: KAFKA_BATCH_LIVENESS_TTL). Pod considered
  #                                                       # dead once the key expires without refresh.
  # config.liveness_heartbeat_interval  = 20              # seconds between background heartbeat refreshes
  #                                                       # (env: KAFKA_BATCH_LIVENESS_HEARTBEAT_INTERVAL).
  #                                                       # Default 180/20 ≈ 9 missed cycles before dead.
  # config.track_running_jobs      = true            # default; set false at high throughput
  #                                                  # (heartbeats still work; skips per-job /live writes)
  # config.liveness_stats_interval = 15              # RSS/CPU sample period for /live (0 = off)
  # config.reconciliation_interval = 300             # seconds between stuck-batch sweeps
  # config.max_failures_per_batch  = 1000            # 0 = unlimited (dashboard failure log)
  # config.producer_config = { "compression.type" => "snappy" }
  # config.consumer_config = { "fetch.min.bytes"  => "1024" }

  # ── Web audit log (optional) ────────────────────────────────────────────────
  # config.audit_enabled = true
  # config.audit_database_connection = :kafka_batch_audit
  # config.audit_actor = ->(env) { env["HTTP_X_FORWARDED_USER"] }

  # ── Metrics (StatsD / Datadog / custom proc) ────────────────────────────────
  # config.metrics_enabled = true
  # config.metrics_adapter = :statsd   # :datadog or :proc
  # config.metrics_client  = Statsd.new("localhost", 8125)
  # config.metrics_proc    = ->(name, payload, duration_ms) { ... }  # for Prometheus etc.
end
