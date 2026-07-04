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
  #          then `rails db:migrate` for failures / pause / weight tables.
  config.store = :<%= @store %>

  # ── Delayed-job (perform_in / perform_at) index store ─────────────────────────
  # Detached from `store` — the main ledger can be Redis while the (potentially
  # huge) schedule index lives on cheap MySQL disk.
  # :redis – (default) ZSET-based index, RAM-resident, lowest latency.
  # :mysql – kafka_batch_scheduled_jobs table; run with
  #          `rails g kafka_batch:install --schedule-store mysql` then `rails db:migrate`.
  config.schedule_store = :<%= @schedule_store %>

  # Delayed-job poller. Every consumer pod runs one poller thread by default, which
  # is fine for the Redis backend (atomic Lua). For the :mysql backend at high pod
  # counts, DEDICATE a few pods to polling and turn it OFF elsewhere so 150 pods
  # don't all query MySQL — set KB_SCHEDULE_POLLER=false on non-scheduler pods:
  # config.schedule_poller_enabled = ENV.fetch("KB_SCHEDULE_POLLER", "true") == "true"
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
  config.redis_pool_size = 5

  # ── Topic namespace ─────────────────────────────────────────────────────────
  # All topic names AND the consumer group derive from this prefix, so a single
  # setting namespaces everything (e.g. "myapp" → "myapp.kafka_batch.jobs",
  # consumer group "myapp.kafka-batch"). Leave blank for the bare defaults.
  config.topic_prefix = ENV["KAFKA_PREFIX"].to_s.strip

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
  #     fairness true
  #     fairness_type :time        # weighted wall-clock time (default; uneven runtimes)
  #     # fairness_type :throughput  # weighted job count (similar runtimes)
  #   end
  #
  # One active tenant uses 100% of the in-flight window; N split it evenly
  # (work-conserving). The knobs below apply to EACH lane independently.
  config.fairness_global_concurrency = 50   # in-flight window per lane (ready depth + concurrency)
  # config.fairness_max_inflight_per_tenant = 0   # optional hard per-tenant ceiling (0 = dynamic share)

  # ⚠ Make per-tenant weights actually control THROUGHPUT (edit them live on
  # /kafka_batch/weights). The library default is FALSE, which means weights only
  # bias selection order — under load every active tenant gets an EQUAL in-flight
  # cap, so throughput stays ~equal no matter the weight (the #1 "my weights do
  # nothing" gotcha). With this ON, a weight-N tenant gets ~N× the concurrency of a
  # weight-1 tenant. It's a no-op when all weights are equal, so it's safe to leave on.
  config.fairness_weighted_concurrency = true
  # config.fairness_weight_cache_ttl = 60   # secs before a weight change propagates across pods

  # Pin specific tenants to ingest partitions (common to both lanes; others use
  # hash routing):
  # config.fairness_tenant_partitions = { "acme" => 0, "globex" => 1 }

  # ── Producer safety ───────────────────────────────────────────────────────────
  # Raise a clear ProducerError instead of an opaque rdkafka error on oversized
  # payloads. 0/nil disables. Matches Kafka's typical 1 MiB broker default.
  config.max_message_bytes = 1_048_576

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
  # config.liveness_stats_interval = 15              # RSS/CPU sample period for /live (0 = off)
  # config.complete_after_retries  = 3               # count a job toward its batch after N retries
  # config.reconciliation_interval = 300             # seconds between stuck-batch sweeps
  # config.max_failures_per_batch  = 1000            # 0 = unlimited (dashboard failure log)
  # config.producer_config = { "compression.type" => "snappy" }
  # config.consumer_config = { "fetch.min.bytes"  => "1024" }
end
