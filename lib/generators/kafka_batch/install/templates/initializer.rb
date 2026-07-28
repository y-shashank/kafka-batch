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
  # Also used by the recurring (cron) tables when installed with --recurring.

  # ── Recurring (cron) scheduler ────────────────────────────────────────────────
  # Requires the kafka_batch_recurring_schedules / _fires tables:
  #   rails g kafka_batch:install --recurring && rails db:migrate
  # Enable on scheduler pods only (shares Redis leader lock + fire ledger with Go).
  # config.recurring_scheduler_enabled = true
  # Or: KAFKA_BATCH_RECURRING_SCHEDULER_ENABLED=true

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
  # Scales with SuperFetch (SF + renewers + Karafka). Override if you raise SF.
  # config.redis_pool_size = KafkaBatch.config.recommended_redis_pool_size
  config.redis_pool_size = 16

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

  # Once a lane goes fully idle (empty ring, no in-flight leases, no forwards, zero
  # ingest lag) for the debounce window, the virtual-time ledger is cleared (weights
  # kept) so each active period starts fair and vtime can't grow forever. On by default.
  # config.fairness_reset_vtime_when_idle = false
  # config.fairness_vtime_idle_reset_debounce = 15   # secs a lane must stay idle first

  # Dynamic exclusive ingest partitions are ON by default (one partition per tenant
  # until the ingest topic is full). Pin whales explicitly if you want fixed mapping:
  # config.fairness_tenant_partitions = { "acme" => 0, "globex" => 1 }
  # config.fairness_dynamic_tenant_partitions = false  # murmur2 key-hash only
  # config.fairness_tenant_partition_cache_ttl = 30    # in-process lookup cache (seconds)

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

  # ── AI assistant (OpenRouter + RAG over packaged docs) ───────────────────────
  # Dashboard chat bubble + /ai settings. Knowledge chunks sync to Redis on boot.
  # config.ai_knowledge_enabled = true
  # Required to store OpenRouter API keys (AES-GCM). Prefer a long random secret:
  # config.ai_encryption_salt = ENV.fetch("KAFKA_BATCH_AI_ENCRYPTION_SALT")
  # config.ai_chat_history_max_lines = 500   # global shared history (Redis LIST)
  # config.ai_openrouter_default_model = "openai/gpt-4o-mini"
  # Allowlisted O(1) read-only Redis lookups (batch status, fairness sizes, …):
  # config.ai_live_data_enabled = true
  # config.ai_live_data_max_calls = 3
  # OpenRouter tool-calling (often 400 on some providers) — leave off; use prefetch:
  # config.ai_live_data_model_tools = false

  # ── Health alerts (dashboard /alerts; Redis settings, encrypted secrets) ─────
  # Opt-in evaluator: lag stuck/growing, Redis RTT, no live consumers, reconciler
  # stale, fairness ingest backup, DLT rate, schedule depth, recurring cron stale.
  # Library/env are bootstrap only — UI Save wins in Redis and hot-reloads next tick.
  # Secrets (Slack/webhook/SMTP password) reuse config.ai_encryption_salt.
  # Docs: README "Health alerts"; AI corpus ai/README.md §46 + ai/FAQ.md section AS.
  # config.alerts_enabled = false
  # config.alerts_interval = 60
  # config.alerts_for_ticks = 3
  # config.alerts_resolve_ticks = 2
  # config.alerts_cooldown_seconds = 900
  # config.alerts_run_on_ui = false   # set true only if UI pods should evaluate
  # config.alerts_lag_threshold = 1000
  # config.alerts_lag_growth_min = 100
  # config.alerts_rtt_avg_ms = 50.0
  # config.alerts_rtt_max_ms = 200.0
  # config.alerts_dlt_per_minute = 50
  # config.alerts_schedule_pending_max = 10_000
  # config.alerts_reconciler_max_age = 900
  # config.alerts_fairness_ingest_lag = 5000

  # ── Tenant guard (per-tenant error-rate pause/throttle; fairness lanes only) ─
  # Watches a sliding per-tenant error-rate window and can auto-mitigate a
  # misbehaving tenant by throttling its fairness weight and/or pausing its
  # dedicated ingest partition (drain-safe; batch counting untouched). Manage the
  # live thresholds and see/reset actions on the dashboard "Tenant guard" page.
  # These are only DEFAULTS — the page overrides them at runtime.
  # config.tenant_guard_enabled = false
  # config.tenant_guard_window_seconds = 300         # sliding lookback
  # config.tenant_guard_min_samples = 50             # min ok+fail before acting
  # config.tenant_guard_error_rate_pct = 25.0        # fail/(ok+fail)*100 threshold
  # config.tenant_guard_include_retries = false      # count retries as failures
  # config.tenant_guard_mitigation = :throttle       # :none | :throttle | :pause | :throttle_then_pause
  # config.tenant_guard_throttle_weight = 0.1        # fairness weight applied on throttle
  # config.tenant_guard_auto_release_seconds = 900   # auto-disengage after N sec; nil = manual reset only
  # config.tenant_guard_grace_ticks = 0              # >0 = warn ticks before acting
  # config.tenant_guard_reconcile_interval = 15      # reconciler tick (auto-release + drift repair)

  # ── Metrics (StatsD / Datadog / custom proc) ────────────────────────────────
  # config.metrics_enabled = true
  # config.metrics_adapter = :statsd   # :datadog or :proc
  # config.metrics_client  = Statsd.new("localhost", 8125)
  # config.metrics_proc    = ->(name, payload, duration_ms) { ... }  # for Prometheus etc.
end
