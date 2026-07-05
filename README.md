# kafka-batch

[![CI](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/y-shashank/kafka-batch/badges/coverage.json)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)

Drop-in replacement for **Sidekiq Pro Batches** using Apache Kafka as the transport layer. Provides the same `on_success` / `on_complete` callback semantics, per-job retry with backoff, and idempotent completion tracking — at a fraction of the cost.

Built on the [Karafka](https://karafka.io) ecosystem: **WaterDrop** for producing, **Karafka consumers** for processing.

---

## Table of Contents

- [How it works](#how-it-works)
- [Installation](#installation)
- [Configuration](#configuration)
  - [Choosing a store](#choosing-a-store)
  - [Karafka routing](#karafka-routing)
    - [Scaling & partition sizing](#scaling--partition-sizing)
- [Completion counting & scalability](#completion-counting--scalability)
- [Defining workers](#defining-workers)
- [Creating batches](#creating-batches)
  - [Standalone jobs (no batch)](#standalone-jobs-no-batch)
  - [Batch.find and Batch.cancel](#batchfind-and-batchcancel)
- [Callbacks](#callbacks)
- [Retry behaviour](#retry-behaviour)
  - [Early batch completion (`complete_after_retries`)](#early-batch-completion-complete_after_retries)
- [Dead Letter Topic](#dead-letter-topic)
- [Priority queues](#priority-queues)
  - [Multi-tenant fairness (WFQ)](#multi-tenant-fairness-wfq)
    - [Enqueuing fair jobs](#enqueuing-fair-jobs)
    - [Optional: Redis-backed WFQ Scheduler (strict weighted shares)](#optional-redis-backed-wfq-scheduler-strict-weighted-shares)
- [Web UI](#web-ui)
- [Reconciler](#reconciler)
- [Instrumentation](#instrumentation)
- [Rake tasks](#rake-tasks)
- [Reliability guarantees](#reliability-guarantees)
- [Known limitations](#known-limitations)
- [Migrating from Sidekiq Pro Batches](#migrating-from-sidekiq-pro-batches)
- [Architecture deep-dive](#architecture-deep-dive)
- [Topic reference](#topic-reference)
- [Data reference](#data-reference)
  - [Redis keys](#redis-keys)
  - [MySQL tables](#mysql-tables)
- [Contributing](#contributing)

---

## How it works

```
┌──────────────────────────────────────────────────────────────────┐
│                        Your application                           │
│                                                                   │
│  Batch.create do |b|                                              │
│    b.push(MyWorker, { id: 1 })  ──┐                              │
│    b.push(MyWorker, { id: 2 })  ──┤──► Kafka: worker topic       │
│    b.push(MyWorker, { id: 3 })  ──┘   (idempotent producer)      │
│                                                                   │
│  Batch record written to Redis BEFORE first produce               │
└──────────────────────────────────────────────────────────────────┘
                │ (jobs topic)
   ┌────────────▼─────────────┐
   │    Karafka: JobConsumer   │
   │                          │
   │  worker.perform(payload)  │
   │    ├─ success ───────────┼──► kafka_batch.events
   │    │                     │    event carries source coords
   │    │                     │    {src_topic, src_partition,
   │    │                     │     src_offset}; keyed by
   │    │                     │     src_topic/src_partition
   │    └─ failure            │
   │        ├─ retriable ─────┼──► kafka_batch.jobs.retry.{short|medium|large}
   │        └─ exhausted ─────┼──► kafka_batch.dead_letter
   └──────────────────────────┘         +events (failed)
                │ (events topic)
   ┌────────────▼─────────────┐
   │  Karafka: EventConsumer   │
   │                          │
   │  store.record_completion_ │
   │  by_offset(...)           │   dedup: SADD job_id to
   │   per-batch job_id set    │   kafka_batch:b:dedup:{id}
   │    → O(jobs per batch)    │   (absorbs redelivered AND
   │    ├─ running ──► skip   │    re-produced events;
   │    ├─ duplicate ► skip   │    out-of-order OK)
   │    └─ done ──────────────┼──► kafka_batch.callbacks
   └──────────────────────────┘
                │ (callbacks topic)
   ┌────────────▼─────────────┐
   │ Karafka: CallbackConsumer │   at-least-once
   │                          │   (callbacks idempotent)
   │  callback_dispatched? ───┼── yes ─► skip (duplicate)
   │    │ no                   │
   │    ▼                      │
   │  on_success(batch)        │   invoke FIRST,
   │  on_complete(batch)       │   then claim_callback()
   │  claim_callback()  ───────┼── mark dispatched (CAS)
   └──────────────────────────┘
                                  (crash before claim ⇒
                                   re-invoke on redelivery,
                                   never a lost callback)

   ┌──────────────────────────┐
   │  Karafka: RetryConsumer   │  ◄── kafka_batch.jobs.retry.{short|medium|large}
   │                          │
   │  retry_after in future?  │
   │    ├─ yes ──► pause()    │  (Karafka partition pause –
   │    │         then retry   │   zero thread blocking)
   │    └─ no  ──► produce    │
   │              to retry_to  │──► original worker topic
   └──────────────────────────┘
```

> **Multi-tenant fairness** (per-worker opt-in via `fairness true`) inserts a stage before `JobConsumer` for *that worker*: `Batch.push` writes to a per-tenant **ingest** topic, a `Fairness::Dispatcher` loads it into a **Redis WFQ scheduler**, a `Fairness::Forwarder` checks out the fairest job (weighted, concurrency-gated) onto a **ready** topic, and a dedicated **`…-jobs-fair`** consumer group drains it. Plain workers keep going straight to their own topic in the **`…-jobs`** group. Everything downstream (events/callbacks/retry/DLT) is identical. **Redis is required.** See [Multi-tenant fairness](#multi-tenant-fairness-wfq).

---

## Installation

### Entry points

The gem ships two `require` entry points so each service loads only what it needs:

| Entry point | Use in | Loads |
|---|---|---|
| `kafka_batch` (default) | **Worker service** — runs Karafka consumers, processes jobs | Everything: consumers, producer, batch, reconciler, topics, fairness |
| `kafka_batch/ui` | **Web service** — mounts the dashboard only | Config, stores, lag, liveness, consumption control, web UI |

**Worker service** `Gemfile`:
```ruby
gem "kafka-batch"  # require: "kafka_batch" is the default
```

**Web/API service** `Gemfile` (dashboard only, no Karafka dependency at runtime):
```ruby
gem "kafka-batch", require: "kafka_batch/ui"
```

> **Why `require: "kafka_batch/ui"` and not `require: false` or `require: true`?**
>
> - `require: true` — Bundler calls `require "kafka-batch"` (the literal gem name with a dash). Ruby can't find a file called `kafka-batch.rb`, so boot raises `LoadError: cannot load such file -- kafka-batch`.
> - `require: false` — Bundler skips the gem entirely. The explicit `require "kafka_batch/ui"` in your initializer *does* load it, but only during the `:environment` Rake task — too late for `load_tasks` to discover the Railtie's rake tasks. Result: `rake aborted! Don't know how to build task 'kafka_batch:create_topics'`.
> - `require: "kafka_batch/ui"` — Bundler calls the correct `require` at startup, the Railtie registers its rake tasks before Rake runs, and the initializer's own `require "kafka_batch/ui"` is a no-op (already in `$LOADED_FEATURES`).

The web service still gets the full dashboard (all tabs, pause/resume, cancel/delete) — it just doesn't load any consumer or producer code. Configure it with the same store + topic settings and it reads live data from Redis (batch ledger), optional MySQL tables (failures / pauses / weights / scheduled-jobs), and the Kafka Admin API.

Run the installer:

```bash
# Redis store (default) — batch ledger in Redis; no migrations
bundle exec rails generate kafka_batch:install

# MySQL ancillary tables — failure log, pause state, tenant weights in MySQL
# (batch counters still live in Redis)
bundle exec rails generate kafka_batch:install --store mysql
```

This creates:
- `config/initializers/kafka_batch.rb` — pre-populated defaults
- `bin/create_kafka_topics.sh` — standalone bash script for CI/Docker topic provisioning (no Rails required)
- Database migrations (**`--store mysql` only** — failures, consumption pauses, tenant weights)

| Setting | `--store redis` (default) | `--store mysql` |
|---|---|---|
| Batch ledger | Redis (`redis_url`, `batch_ttl`, `all_index_max_size`) | Redis (same — counters are **not** in MySQL) |
| Failure log | Redis hash (`failures_ttl`, `max_failures_per_batch`) | MySQL `kafka_batch_failures` table |
| Migrations | None | `rails db:migrate` (3 tables; +1 with `schedule_store: :mysql` — see [MySQL tables](#mysql-tables)) |
| Reconciler lock | Redis `SET NX EX` | Redis `SET NX EX` (delegated) |

`config.redis_url` is **required in both modes** (fairness scheduler + liveness + batch ledger).

Run migrations only with `--store mysql`:

```bash
bundle exec rails db:migrate
```

Create the required Kafka topics. Choose the approach that fits your environment:

```bash
# Rake task — derives the full topic set from your config (requires Rails + Kafka broker):
bundle exec rake kafka_batch:create_topics

# Shell script — works without Rails, ideal for CI/Docker init containers:
KAFKA_BROKERS=localhost:9092 ./bin/create_kafka_topics.sh

# Dry-run first to see what would be created:
bundle exec rake kafka_batch:topics
```

Or create them manually (adjust partitions to your throughput):

```bash
# --replication-factor 1 is correct for a single-broker (local/dev) setup.
# For multi-broker production clusters, use the number of brokers (e.g. 3).

# Shared default queue (only if workers don't declare their own kafka_topic)
kafka-topics.sh --create --topic kafka_batch.jobs            --partitions 6  --replication-factor 1

# Priority queues (fast/slow × p0/p1 — always provisioned)
kafka-topics.sh --create --topic kafka_batch.jobs.fast_p0   --partitions 6  --replication-factor 1
kafka-topics.sh --create --topic kafka_batch.jobs.fast_p1   --partitions 6  --replication-factor 1
kafka-topics.sh --create --topic kafka_batch.jobs.slow_p0   --partitions 6  --replication-factor 1
kafka-topics.sh --create --topic kafka_batch.jobs.slow_p1   --partitions 6  --replication-factor 1

# Control plane
kafka-topics.sh --create --topic kafka_batch.events            --partitions 3  --replication-factor 1
kafka-topics.sh --create --topic kafka_batch.callbacks         --partitions 1  --replication-factor 1
kafka-topics.sh --create --topic kafka_batch.jobs.retry.short  --partitions 3  --replication-factor 1
kafka-topics.sh --create --topic kafka_batch.jobs.retry.medium --partitions 3  --replication-factor 1
kafka-topics.sh --create --topic kafka_batch.jobs.retry.large  --partitions 3  --replication-factor 1
kafka-topics.sh --create --topic kafka_batch.dead_letter       --partitions 1  --replication-factor 1
```

> The rake task (`kafka_batch:create_topics`) creates all of the above automatically, including the priority topics, using per-topic partition defaults.

---

## Configuration

Edit `config/initializers/kafka_batch.rb`:

```ruby
KafkaBatch.configure do |config|
  # ── State store ────────────────────────────────────────────────────
  # Batch ledger (counters, dedup, reconciler indexes) is ALWAYS in Redis.
  #   :redis  – (default) failure log also in Redis
  #   :mysql  – failure log + pause state + tenant weights in MySQL
  config.store = :redis

  # ── Kafka brokers ───────────────────────────────────────────────────
  config.brokers = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")

  # ── Topic namespace ─────────────────────────────────────────────────
  # One prefix namespaces ALL topic names and the consumer group, e.g.
  # "myapp" → "myapp.kafka_batch.jobs", consumer group "myapp.kafka-batch".
  # Leave "" for the bare defaults. Any individual name can still be set
  # explicitly (config.jobs_topic = "…") to override the derived value.
  config.topic_prefix = ENV["KAFKA_PREFIX"].to_s.strip

  # ── Cancellation ────────────────────────────────────────────────────
  # When true, JobConsumer skips jobs whose batch was cancelled. The set of
  # cancelled batch ids is cached per process and refreshed at most once per
  # cancellation_cache_ttl seconds (no per-job store read), so cancellation
  # takes effect within that window.
  config.skip_cancelled_jobs    = true
  config.cancellation_cache_ttl = 120  # seconds

  # ── Retry behaviour (global defaults; override per Worker class) ────
  # Tiered retries: each delay tier has its own Kafka topic, so a slow tier
  # never head-of-line-blocks a fast one. By default the Nth retry walks the
  # progression (1st→short, 2nd→medium, 3rd+→large); a Worker can pin all of
  # its retries to one tier via `retry_tier :medium`.
  config.max_retries           = 3            # attempts before dead letter
  config.retry_jitter          = 0.1          # +/- 10% randomization
  config.retry_tiers           = { short: 30, medium: 7 * 60, large: 20 * 60 } # seconds
  config.retry_tier_progression = %i[short medium large]
  # Maximum single-pause duration (seconds) in RetryConsumer. When a retry message
  # is further in the future than this, the consumer pauses for this long then
  # re-checks, so a partition is never suspended for extreme durations.
  config.retry_max_pause_seconds = 30
  # After this many retries a still-failing job counts toward on_complete while
  # it keeps retrying in the background up to max_retries (per-Worker override).
  # Default == max_retries default, so default behaviour is unchanged.
  config.complete_after_retries = 3

  # ── Completion-event emission retries (inline; blocks the worker thread) ─
  config.event_emit_retries = 3
  config.event_emit_backoff = 2  # seconds; sleep = attempt × backoff

  # ── Redis (REQUIRED — batch ledger, fairness, liveness) ─────────────
  config.redis_url       = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  config.redis_pool_size = 5
  config.batch_ttl       = 7 * 24 * 3600  # seconds until Redis batch keys expire
  config.all_index_max_size = 200_000       # cap on the UI batch-list index

  # ── Failure-log retention (store :redis only; :mysql uses SQL table) ─
  # Failure records are a dashboard convenience – the real job data is durable
  # in Kafka – so they get a shorter TTL and a per-batch cap to bound RAM.
  config.failures_ttl           = 24 * 3600  # seconds
  config.max_failures_per_batch = 1000        # 0 = unlimited

  # ── Live-activity backend (/live page; Redis-backed) ──
  #   :redis – (default) per-job + per-consumer tracking in Redis
  #   :off   – disable the /live page
  config.liveness_backend   = :redis
  config.liveness_ttl       = 30   # seconds a heartbeat/entry is "live"
  config.track_running_jobs = true # gate :redis per-job running-state writes

  # ── Multi-tenant fairness (Redis-backed WFQ; Redis REQUIRED) ─────────────
  # Two lanes run at once; a worker picks one with `fairness_type :time` (default)
  # or `fairness_type :throughput`. The knobs below apply to EACH lane independently.
  config.fairness_global_concurrency      = 50   # per-lane in-flight window (bounds ready depth + concurrency)
  config.fairness_max_inflight_per_tenant = 0    # optional hard per-tenant ceiling (0 = dynamic share only)
  config.fairness_ready_window            = 500  # bounded per-tenant staging window in Redis
  config.fairness_default_weight          = 1.0
  config.fairness_dispatcher_batch_size   = 50   # max ingest msgs drained into Redis per consume call
  config.fairness_dispatcher_concurrency  = 5    # expected Karafka concurrency on dispatch process; boot-warning only

  # Explicit tenant → ingest partition map (optional; bypasses the hash partitioner entirely).
  # Useful when you need deterministic assignment — e.g. a "hot" tenant on its own partition.
  # Tenants NOT listed here still use murmur2_random hashing.
  # config.fairness_tenant_partitions = { "acme" => 0, "globex" => 1 }

  # ── Priority queues (non-fair; 4-topic 2-group design) ───────────────────
  # These four topic names are derived from topic_prefix by default. Workers opt
  # in by setting kafka_topic to one of them (fairness true takes precedence).
  # Override only if you want different names:
  # config.fast_p0_topic = "kafka_batch.jobs.fast_p0"  # fast, critical priority
  # config.fast_p1_topic = "kafka_batch.jobs.fast_p1"  # fast, normal priority
  # config.slow_p0_topic = "kafka_batch.jobs.slow_p0"  # slow, critical priority
  # config.slow_p1_topic = "kafka_batch.jobs.slow_p1"  # slow, normal priority
  # How often (seconds) p1 consumers re-check p0 lag. Default 2.
  config.priority_lag_check_interval = 2

  # ── Reconciliation ───────────────────────────────────────────────────
  config.reconciliation_interval = 300  # seconds (re-check stuck "running" batches)
  config.reconciler_lock_ttl     = 600  # seconds; distributed-lock TTL for one sweep

  # ── Topic validation at boot ─────────────────────────────────────────
  # When true, Rails boot raises if any required topics are missing in Kafka.
  # Disable in CI / test environments where Kafka is not running.
  config.validate_topics_on_boot = false

  # ── Advanced rdkafka / WaterDrop config overrides ───────────────────
  # config.producer_config = { "compression.type" => "snappy" }
  # config.consumer_config = { "fetch.min.bytes"  => "1024"   }
end
```

### Full config reference

Every option on `KafkaBatch.config`:

| Option | Type | Default | Description |
|---|---|---|---|
| `store` | Symbol | `:redis` | `:redis` = batch ledger + failure log in Redis; `:mysql` = batch ledger in Redis + failures/pauses/weights in MySQL |
| `brokers` | Array&lt;String&gt; | `["localhost:9092"]` | Kafka bootstrap brokers |
| `topic_prefix` | String | `""` | Namespaces **all** topic names and the consumer group (e.g. `"myapp"` → `myapp.kafka_batch.jobs`, `myapp.kafka-batch`). Any name below can still be set explicitly to override the derived value |
| `consumer_group` | String | `"kafka-batch"` | Base consumer-group name (derived from `topic_prefix`; suffixed `-control`, `-dispatch-<lane>`, `-jobs-fair-<lane>`, `-jobs-fast`, `-jobs-slow`, `-jobs`) |
| `logger` | Logger | `Rails.logger` | Logger instance |
| `jobs_topic` | String | `"kafka_batch.jobs"` | Shared default job topic for workers that don't declare their own `kafka_topic` (non-fairness); derived from `topic_prefix` |
| `events_topic` | String | `"kafka_batch.events"` | Completion-event topic (derived from `topic_prefix`) |
| `callbacks_topic` | String | `"kafka_batch.callbacks"` | Batch-callback topic (derived from `topic_prefix`) |
| `retry_topic` | String | `"kafka_batch.jobs.retry"` | Retry-topic **prefix** (tier topics are `<prefix>.short/.medium/.large`); derived from `topic_prefix` |
| `dead_letter_topic` | String | `"kafka_batch.dead_letter"` | Dead-letter topic (derived from `topic_prefix`) |
| `max_retries` | Integer | `3` | Retry attempts before dead-letter (per-Worker override) |
| `retry_jitter` | Float | `0.1` | ± randomization on retry delays |
| `retry_tiers` | Hash | `{short: 30, medium: 420, large: 1200}` | Tier → delay (seconds) |
| `retry_tier_progression` | Array | `[:short, :medium, :large]` | Default tier per retry index (clamps to last) |
| `retry_max_pause_seconds` | Integer (s) | `30` | Maximum single Karafka `pause()` duration in `RetryConsumer`; when a retry is further in the future the consumer re-pauses in increments until due |
| `complete_after_retries` | Integer | `3` | Count a still-failing job toward `on_complete` after N retries (keeps retrying in bg; per-Worker override) |
| `event_emit_retries` | Integer | `3` | Inline retries when producing a completion event |
| `event_emit_backoff` | Integer (s) | `1` | Linear backoff for event-emit retries (`attempt × backoff`) |
| `skip_cancelled_jobs` | Boolean | `true` | Skip jobs whose batch was cancelled |
| `cancellation_cache_ttl` | Integer (s) | `120` | Refresh interval for the per-process cancelled-batch cache |
| `redis_url` | String | `"redis://localhost:6379/0"` | **Required.** Batch ledger, fairness scheduler, liveness, reconciler lock |
| `redis_pool_size` | Integer | `5` | Redis connection-pool size |
| `batch_ttl` | Integer (s) | `604800` (7d) | TTL for Redis batch keys (both store modes) |
| `all_index_max_size` | Integer | `200000` | Max batch IDs in the UI list index (`kafka_batch:index:all`) |
| `failures_ttl` | Integer (s) | `86400` (1d) | TTL for Redis failure hash (`store: :redis` only) |
| `max_failures_per_batch` | Integer | `1000` | Cap on tracked failing jobs per batch (`store: :redis`; `0` = unlimited) |
| `liveness_backend` | Symbol | `:redis` | `/live` source: `:redis` or `:off` (Redis-backed) |
| `liveness_ttl` | Integer (s) | `30` | How long a heartbeat/entry is considered live |
| `track_running_jobs` | Boolean | `true` | Gate per-job running-state writes (`:redis` liveness) |
| `fair_time_ingest_topic` | String | `"kafka_batch.fair_time_ingest"` | Time-lane per-tenant intake topic |
| `fair_time_ready_topic` | String | `"kafka_batch.fair_time_ready"` | Time-lane fairly-ordered execution topic |
| `fair_throughput_ingest_topic` | String | `"kafka_batch.fair_throughput_ingest"` | Throughput-lane per-tenant intake topic |
| `fair_throughput_ready_topic` | String | `"kafka_batch.fair_throughput_ready"` | Throughput-lane fairly-ordered execution topic |
| *(fairness lane)* | — | per-worker | Chosen on the Worker via `fairness_type :time` (default) / `:throughput` — **not** a global config. `:time` advances vtime by `duration/weight` at completion; `:throughput` by `1/weight` at checkout |
| `fairness_global_concurrency` | Integer | `50` | Per-lane in-flight window (forwarded-but-not-completed jobs). Bounds ready-topic depth and total per-lane concurrency; per-tenant share is `ceil(this / active_tenants)` |
| `fairness_max_inflight_per_tenant` | Integer | `0` | Optional **hard** per-tenant in-flight ceiling on top of the dynamic fair share; `0` = dynamic share only |
| `fairness_ready_window` | Integer | `500` | Bounded per-tenant staging window in Redis; when full the Dispatcher pauses the ingest partition (backpressure) |
| `fairness_default_weight` | Float | `1.0` | Default per-tenant weight when no override is set |
| `fairness_weight_cache_ttl` | Integer (s) | `60` | How long each process caches the tenant-weight map; weight changes propagate within this window |
| `fairness_forwarder_idle_sleep` | Float (s) | `0.05` | How long the Forwarder sleeps when a checkout yields nothing (idle / window full) before polling again |
| `fairness_min_ingest_partitions` | Integer | `2` | Warns (raises when `validate_topics_on_boot`) if the ingest topic has fewer partitions; set near max concurrent tenants |
| `fairness_dispatcher_batch_size` | Integer | `50` | Max ingest messages the Dispatcher drains into the Redis window per consume call (`max_messages` in the route) |
| `fairness_dispatcher_concurrency` | Integer | `5` | Expected Karafka concurrency on the dispatch process. **Boot warning only** — logged if `Karafka::App.config.concurrency` is lower |
| `fairness_lease_ttl` | Integer (s) | `1800` | TTL on a fair-lane in-flight slot. In-flight is the set of live **leases** (a global + per-tenant Redis ZSET), and the concurrency budget is the live-lease count — so a leaked slot (SIGKILL / OOM / node loss) is reclaimed automatically when its lease expires and can never permanently pin the lane. **Must exceed your longest expected job runtime** (+ forwarding latency); a job running longer has its slot reclaimed early (harmless soft concurrency overshoot). **Floored to 60s** — a smaller value would let leases expire on the next checkout and silently disable the budget |
| `fairness_tenant_partitions` | Hash | `{}` | Explicit `tenant_id → partition_number` overrides. Bypasses the hash partitioner entirely — the producer sends directly to that partition via WaterDrop's `partition:` parameter. Tenants not listed fall back to murmur2\_random. Out-of-range values are logged and ignored. |
| `max_reconcile_per_run` | Integer | `100` | Max batches the reconciler processes per sweep (both stuck-running and lost-callback independently); caps callback bursts during incidents |
| `max_message_bytes` | Integer | `1_048_576` | Raise `ProducerError` if an encoded payload exceeds this size (1 MiB default, matches Kafka's `message.max.bytes`); `0` disables the guard |
| `fast_p0_topic` | String | `"kafka_batch.jobs.fast_p0"` | Fast-group critical topic; set `kafka_topic` to this value on a worker to enroll it |
| `fast_p1_topic` | String | `"kafka_batch.jobs.fast_p1"` | Fast-group normal topic |
| `slow_p0_topic` | String | `"kafka_batch.jobs.slow_p0"` | Slow-group critical topic |
| `slow_p1_topic` | String | `"kafka_batch.jobs.slow_p1"` | Slow-group normal topic |
| `priority_lag_check_interval` | Integer (s) | `2` | How often p1 consumers re-check p0 lag; smaller = faster priority response, more Admin API calls |
| `reconciliation_interval` | Integer (s) | `300` | How often `EventConsumer` auto-triggers the reconciler (no cron needed); also the staleness threshold for stuck-running batches |
| `reconciler_lock_ttl` | Integer (s) | `600` | Distributed-lock TTL for one reconciler sweep |
| `producer_config` | Hash | `{}` | Raw rdkafka/WaterDrop producer overrides |
| `consumer_config` | Hash | `{}` | Raw rdkafka consumer overrides (merged into every consumer) |
| `validate_topics_on_boot` | Boolean | `false` | Raise at boot if required topics are missing |
| `extra_job_topics` | Array&lt;String&gt; | `[]` | Custom plain-worker topics a **UI-only** dashboard should list on the `/lag` page. Used verbatim (not prefix-aware). Only affects the config-based `/lag` fallback — worker processes that call `draw_routes` resolve custom topics from the routes automatically |

**Per-Worker overrides** (on the worker class, not `config`): `kafka_topic` (optional — defaults to `config.jobs_topic`; set to one of the four priority topic names to enroll in a priority group), `max_retries`, `complete_after_retries`, `retry_tier`, `fairness` (opt into the shared multi-tenant fair lane — takes precedence over `kafka_topic`).

### Choosing a store

KafkaBatch makes **two independent storage choices**. Kafka is always the source of truth for jobs and completion events. **Redis is always required** (`config.redis_url` — validated at boot).

#### 1. State store — `config.store` (`:redis` | `:mysql`)

The **batch ledger** (counters, completion dedup, callback claims, reconciler indexes, dashboard batch list) **always lives in Redis** regardless of this setting. The choice only affects where ancillary relational data is kept:

| | `:redis` (default) | `:mysql` |
|---|---|---|
| Batch ledger | Redis (`kafka_batch:b:*`, indexes, `kafka_batch:counts`) | Redis (same — delegated) |
| Failure log (`/failures`) | Redis hash per batch (`failures_ttl`) | MySQL `kafka_batch_failures` |
| Pause state (`/lag`) | Redis sets (preferred) | MySQL `kafka_batch_consumption_pauses` when Redis unavailable |
| Tenant weights (`/weights`) | Redis `kafka_batch:fair_<type>:weight` (per lane) | MySQL `kafka_batch_tenant_weights` keyed `(tenant_id, fairness_type)` (mirrored to Redis for WFQ Lua) |
| Migrations | None | up to 4 tables — failures, pauses, weights (`--store mysql`) + scheduled-jobs (`--schedule-store mysql`) |
| Best for | Simplest setup, lowest ops | SQL-queryable failure log, MySQL-backed weight admin |

Both modes implement the **same guarantees** (exactly-once counting via `job_id` dedup, callbacks, reconciler, open batches).

#### 2. Live-activity backend — `config.liveness_backend` (`:redis` | `:off`)

Powers **only** the `/live` dashboard page. It is Redis-backed (Redis is a required dependency) and independent of `config.store`.

| | `:redis` | `:off` |
|---|---|---|
| Source | Per-job + per-consumer keys in Redis (`redis_url`) | — |
| Detail | Every running job + live consumers | none |
| Write volume | Scales with **job throughput** (gated by `track_running_jobs`) | none |
| Resilience | Best-effort behind a circuit breaker; stale entries expire via `liveness_ttl` TTL | — |

> Keys are short-TTL'd (`liveness_ttl`) and expire on their own — no sweep needed. Set `track_running_jobs = false` to keep consumer heartbeats but skip per-job running-state writes.

### Batch ledger (always Redis)

No migrations. Batch state is a Redis hash at `kafka_batch:b:{batch_id}` (expires after `config.batch_ttl`, refreshed on each completion). Completion dedup uses a per-batch SET at `kafka_batch:b:dedup:{batch_id}` (member = `job_id`) so out-of-order job completions on the same partition are all counted.

The reconciler indexes (`kafka_batch:index:running`, `kafka_batch:index:done`) and O(1) status counters (`kafka_batch:counts`) are maintained automatically.

### MySQL ancillary tables (`store: :mysql` / `schedule_store: :mysql`)

Optional. Run migrations only for the backends you opt into (`--store mysql` and/or `--schedule-store mysql`). See [MySQL tables](#mysql-tables) for full column definitions.

| Table | Created when | Purpose |
|---|---|---|
| `kafka_batch_failures` | `store: :mysql` | Per-job failure log for `/failures` (upserted per failing job) |
| `kafka_batch_consumption_pauses` | `store: :mysql` | Pause/resume state for `/lag` when Redis is unavailable |
| `kafka_batch_tenant_weights` | `store: :mysql` | Per-tenant, **per-lane** WFQ weight overrides — key `(tenant_id, fairness_type)` (mirrored to Redis for checkout Lua) |
| `kafka_batch_scheduled_jobs` | `schedule_store: :mysql` | Delayed-job (`perform_in`/`perform_at`) pointer index |

```bash
bundle exec rails db:migrate
```

> **Upgrading from older versions:** if you previously migrated `kafka_batch_records` / `kafka_batch_consumer_offsets`, those tables are unused — batch counters now live only in Redis. Safe to drop when convenient.

### Karafka routing

Wire up KafkaBatch routes inside your `karafka.rb`. Call `KafkaBatch.draw_routes(self)` from **inside** `routes.draw`, and make sure your worker classes are **loaded first** (reference them or eager-load) so the registry is populated:

```ruby
class KarafkaApp < Karafka::App
  setup do |config|
    config.kafka = { "bootstrap.servers" => ENV["KAFKA_BROKERS"] }
    config.client_id = "my-app"
    # Recommended: >1 so control-plane messages (events/callbacks) are worked
    # in parallel with jobs and don't queue behind a long-running job.
    config.concurrency = 5
  end

  routes.draw do
    # Your own routes
    topic "my_app.events" do
      consumer MyEventsConsumer
    end

    # Ensure worker classes are registered before drawing routes:
    ProcessOrderWorker
    # KafkaBatch: control + dispatch + jobs-fair + jobs consumer groups
    KafkaBatch.draw_routes(self)
  end
end
```

`draw_routes` registers **up to six consumer groups**, each scaling independently:

| Group | Topic(s) | Consumer(s) | When present |
|---|---|---|---|
| `<consumer_group>-control` | `events`, `callbacks`, retry tiers | `EventConsumer`, `CallbackConsumer`, `RetryConsumer` | always |
| `<consumer_group>-dispatch-time` | `fair_time_ingest` | `Fairness::Dispatcher` | any `fairness_type :time` worker |
| `<consumer_group>-dispatch-throughput` | `fair_throughput_ingest` | `Fairness::Dispatcher` | any `fairness_type :throughput` worker |
| `<consumer_group>-jobs-fair-time` | `fair_time_ready` | `JobConsumer` | any `fairness_type :time` worker |
| `<consumer_group>-jobs-fair-throughput` | `fair_throughput_ready` | `JobConsumer` | any `fairness_type :throughput` worker |
| `<consumer_group>-jobs-fast` | `fast_p0_topic`, `fast_p1_topic` | `FastP0Consumer`, `FastP1Consumer` | any worker using a fast topic |
| `<consumer_group>-jobs-slow` | `slow_p0_topic`, `slow_p1_topic` | `SlowP0Consumer`, `SlowP1Consumer` | any worker using a slow topic |
| `<consumer_group>-jobs` | each plain worker's `kafka_topic` | `JobConsumer` | any non-fair, non-priority worker |

```
                    ┌─────────────────────────────────────────┐
                    │  {consumer_group}-control               │
                    │  events · callbacks · retry tiers       │
                    └─────────────────────────────────────────┘

  fair worker ──► ingest ──► ┌──────────────────────────────┐
                             │  {cg}-dispatch-<lane>         │
                             │  Fairness::Dispatcher         │
                             └──────────────┬───────────────┘
                                            ▼ ready
                             ┌──────────────────────────────┐
                             │  {cg}-jobs-fair-<lane>        │
                             │  JobConsumer (fair workers)   │
                             └──────────────────────────────┘

  fast worker ──────────────►┌──────────────────────────────┐
    (kafka_topic = fast_p0)  │  {consumer_group}-jobs-fast  │
    (kafka_topic = fast_p1)  │  FastP0/P1Consumer           │
                             └──────────────────────────────┘

  slow worker ──────────────►┌──────────────────────────────┐
    (kafka_topic = slow_p0)  │  {consumer_group}-jobs-slow  │
    (kafka_topic = slow_p1)  │  SlowP0/P1Consumer           │
                             └──────────────────────────────┘

  plain worker ─────────────►┌──────────────────────────────┐
                             │  {consumer_group}-jobs        │
                             │  JobConsumer (plain workers)  │
                             └──────────────────────────────┘
```

Group names are derived from `config.consumer_group` (default `"kafka-batch"`). Helpers:

```ruby
KafkaBatch.control_consumer_group   # => "kafka-batch-control"
KafkaBatch.dispatch_consumer_group  # => "kafka-batch-dispatch"    (when fairness?)
KafkaBatch.jobs_fair_consumer_group # => "kafka-batch-jobs-fair"   (when fairness?)
KafkaBatch.fast_consumer_group      # => "kafka-batch-jobs-fast"   (when fast topics used)
KafkaBatch.slow_consumer_group      # => "kafka-batch-jobs-slow"   (when slow topics used)
KafkaBatch.jobs_consumer_group      # => "kafka-batch-jobs"
KafkaBatch.consumer_groups          # => array of groups that should exist for current workers
```

### Scaling & partition sizing

This section answers the practical question: **when load on a topic varies from 100K to 50M jobs and you scale from 1 to ~150 consumer pods, how many partitions should each topic have** so consumers are neither starved (idle pods) nor a bottleneck (lag that busts your SLO)?

#### The one rule that governs everything

A consumer group drains a topic with at most this much parallelism:

```
effective_parallelism = min(partitions, consumer_pods × concurrency)
```

- Kafka assigns **each partition to exactly one consumer instance (pod)**; Karafka's `concurrency` then works several of a pod's assigned partitions in parallel within that pod. A single partition is processed **sequentially** (to preserve order).
- **Partitions are the hard ceiling on parallelism.** You can never run more useful worker threads on a topic than it has partitions.
- **Kafka only grows partitions, never shrinks them.** So you size **once, for your peak**, and let pod count float underneath it.
- **Idle pods** appear only when `pods > partitions` (some pods get zero partitions). **Starvation** (threads with no work) is just low load — expected; scale pods *down*, don't add partitions.

So the target is simple: **partitions ≥ (peak pods) × (concurrency)** on every topic whose consumer group you intend to scale to 150 pods. That lets a single pod own all partitions at low load (works through them via `concurrency`) and lets 150 pods each own a fair slice at peak — no idle pods anywhere in the 1→150 range.

#### Step 1 — size pods from your SLO and job duration

Partitions cap parallelism, but **job duration decides how many threads you actually need**. One partition = one sequential stream, so a thread's rate ≈ `1 / avg_job_seconds`:

| Avg job time | ~jobs/sec per partition (1 thread) |
|---|---|
| 5 ms | ~200 |
| 20 ms | ~50 |
| 100 ms | ~10 |
| 1 s | ~1 |
| 5 s | ~0.2 |
| 30 s | ~0.03 |

```
required_rate = total_jobs / slo_seconds                 # jobs/s you must sustain
threads       = ceil(required_rate × avg_job_seconds)    # = required_rate / per_thread_rate
pods          = ceil(threads / concurrency)              # your 1..150 budget
```

**Worked example — 50M jobs, 30-min SLO (1800s), concurrency = 5:** `required_rate = 50M / 1800 ≈ 27,800 jobs/s`.

| Avg job time | threads needed | pods @ C=5 | Fits 150 pods? |
|---|---|---|---|
| 20 ms | ~556 | ~112 | ✅ |
| 100 ms | ~2,780 | ~556 | ❌ — raise concurrency, lengthen SLO, or split the worker |
| 1 s | ~27,800 | ~5,560 | ❌ — throughput-bound; this workload needs hours, not 30 min |

The takeaway: **fast jobs (throughput lane) scale to 50M within 150 pods; slow jobs (time lane) are throughput-bound** — for 50M slow jobs, size the SLO to the math (`threads = required_rate × avg_job_seconds`), not the other way around. When `pods` exceeds your 150 budget, you are compute-bound and no partition count will help.

#### Step 2 — partition count per topic

Provision **execution** topics (where the 150-pod swarms live) for the peak: `peak_pods × concurrency`, rounded up with ~20–30% headroom for uneven assignment and future growth (remember: you can't shrink later). With **150 pods × concurrency 5 = 750 → round to 768**. Control-plane and index topics carry far less work and are sized much smaller. Recommended counts by deployment tier:

| Topic | Consumer group | Role | Small (≤1M, ≤5 pods) | Medium (≤10M, ≤30 pods) | Large (≤50M, ≤150 pods) |
|---|---|---|---|---|---|
| plain worker topics / `jobs` | `-jobs` | job execution | 12 | 128 | **768** (peak_pods × C) |
| `fair_time_ready` / `fair_throughput_ready` | `-jobs-fair` | job execution | 12 | 128 | **768** (peak_pods × C) |
| `jobs.fast_p0/p1`, `jobs.slow_p0/p1` | `-jobs-fast` / `-jobs-slow` | job execution | 12 | 128 | **768** (peak_pods × C) |
| `fair_time_ingest` / `fair_throughput_ingest` | `-dispatch` | tenant fairness (see below) | max(50, tenants/4) | max(50, tenants/4) | max(50, tenants/4) |
| `events` | `-control` | completion counting (batched per poll) | 6 | 24 | 64 |
| `callbacks` | `-control` | 1 msg per batch (low volume) | 3 | 6 | 12 |
| `jobs.retry.{short,medium,large}` | `-control` | only failing jobs | 3 | 12 | 32 |
| `scheduled` | *(no group — poller random-reads)* | perform_in/at payloads | 6 | 12 | 24 |
| `dead_letter` | *(manual)* | rare | 1 | 1 | 3 |

Notes that make this balanced rather than wasteful:

- **Execution topics dominate** — they're the only ones that need the full `peak_pods × concurrency`. Keying is by `job_id` (uniform hash), so load spreads evenly across partitions; no hot partition, no starved thread while lag exists.
- **`events`** carries one message per job (up to 50M) but the `EventConsumer` folds a whole poll into one Redis write, so it needs far fewer partitions than the execution topics — 64 is ample at 50M. It rides the `-control` group; give `-control` its own pods so long jobs never delay completion counting.
- **`scheduled` is not a consumer group** — the per-process `SchedulePoller` reads payloads by random `(partition, offset)` access, so its partition count only affects produce spread; 6–24 is plenty.
- **Fairness `*_ingest` topics size by tenants, not pods** (see [Partitioning & topic sizing](#partitioning--topic-sizing-important)) — the WFQ scheduler, not partition count, drives execution parallelism (that's the `*_ready` topic's job). Keep `-dispatch` pods ≈ ingest partitions.
- **Over-provisioning partitions is cheap insurance** against the no-shrink rule; the real cost is broker metadata, so keep per-topic counts in the hundreds (not thousands) and lean on `concurrency` to reach full utilization with fewer partitions.

#### Step 3 — scale pods dynamically (1 → 150) on lag

Partitions are fixed at the peak; **pod count is what you autoscale.** Drive a KEDA/HPA `ScaledObject` off each group's Kafka **consumer lag** (or the `/lag` dashboard numbers): scale up while lag on the group's topics grows, scale down when it drains. A single pod safely owns all 768 partitions at idle (it just works `concurrency` of them at a time); Kafka rebalances the assignment as pods join/leave, so you stay non-idle across the whole 1→150 range as long as `pods ≤ partitions`.

#### Step 4 — don't let 150 pods hammer the schedule store

If you use delayed jobs (`perform_in`/`perform_at`), **every** consumer pod runs a `SchedulePoller` thread by default. That's cheap on the Redis backend (one atomic Lua call per poll) but at 150 pods on the **MySQL** backend it means 150 pods issuing `SELECT … FOR UPDATE SKIP LOCKED` on a timer. Three protections:

1. **`SKIP LOCKED` / atomic claims** mean pollers never block each other — the concern is query *volume*, not contention.
2. **Adaptive idle backoff (automatic).** Idle pollers back off from `schedule_poll_interval` (5s) to `schedule_poll_max_interval` (60s), snapping back the instant work appears — an idle fleet drops to ~1 query/pod/60s (~12× fewer). `schedule_poll_jitter` de-syncs pods; staggering across many pods keeps due-job latency low even while backed off.
3. **Dedicate poller pods at scale (recommended).** Turn the poller off on the big swarms and run it on a small fixed set, via env: `config.schedule_poller_enabled = ENV.fetch("KB_SCHEDULE_POLLER","true") == "true"` — set `false` on your 150-pod worker Deployment and `true` on a 2–3 pod "scheduler" Deployment. Atomic claims let those few share the load with no leader election.

See [Controlling poller load with many pods](#controlling-poller-load-with-many-pods) for details.

#### Scaling consumer groups independently

Each lane has its own consumer group, so you scale them independently without one starving another:

| What you want | Knob |
|---|---|
| More fair-job throughput | Add `-jobs-fair-<lane>` pods; size the lane's **ready** topic to `peak_pods × concurrency` |
| More fast/slow-job throughput | Add `-jobs-fast` / `-jobs-slow` pods; size those topics the same way |
| More plain-job throughput | Add `-jobs` pods; size each plain worker's topic the same way |
| Faster dispatch from ingest → ready | Scale `-dispatch-<lane>` (≈ one pod per ingest partition is enough) |
| Prompt batch callbacks / retries | Scale `-control` separately so long jobs don't delay events |

**Each fairness lane has its own consumer groups** — `<cg>-dispatch-time` / `-dispatch-throughput` and `<cg>-jobs-fair-time` / `-jobs-fair-throughput` — so the two lanes can run in **separate processes** with independent thread pools and scaling (see [Running both fairness lanes on one process](#running-both-fairness-lanes-on-one-process) for what co-running means).

By default, `bundle exec karafka server` runs **all** registered groups in one process. To dedicate processes (and autoscale them) per lane, use Karafka's `--include-consumer-groups` flag:

```bash
# Control plane only (events, callbacks, retries)
bundle exec karafka server --include-consumer-groups kafka-batch-control

# TIME fairness lane only — its own dispatcher + forwarder + worker pool
bundle exec karafka server \
  --include-consumer-groups kafka-batch-dispatch-time,kafka-batch-jobs-fair-time

# THROUGHPUT fairness lane only — fully isolated from the time lane
bundle exec karafka server \
  --include-consumer-groups kafka-batch-dispatch-throughput,kafka-batch-jobs-fair-throughput

# Just the executors for one lane (run its dispatcher elsewhere)
bundle exec karafka server --include-consumer-groups kafka-batch-jobs-fair-time

# Priority / plain workers
bundle exec karafka server --include-consumer-groups kafka-batch-jobs-fast
bundle exec karafka server --include-consumer-groups kafka-batch-jobs-slow
bundle exec karafka server --include-consumer-groups kafka-batch-jobs
```

Replace `kafka-batch` with your `config.consumer_group` value. Use `--exclude-consumer-groups` to run everything *except* named groups.

> **Tip:** keep `config.concurrency > 1` on processes that include `-control` or job groups so partitions are worked in parallel. For production, a common split is: one (or more) `-control` processes, and independently sized per-lane swarms (`-dispatch-<lane>` + `-jobs-fair-<lane>`) plus `-jobs` / priority swarms. With `concurrency = 1`, a long-running job can delay (not starve) event/callback processing on the same process.

#### Preferred deployment: one Deployment per role (same image)

The recommended production topology is **one Deployment per role, all running the same image**, each selecting its consumer group(s) from an env var. `draw_routes` still registers every group (producing/admin need the full picture) — Karafka only *runs* the groups you include, so you subset at the server, not in `karafka.rb`.

A tiny wrapper maps a `KB_ROLE` env (a comma-separated list) to `--include-consumer-groups`, de-duped. Commit it as `bin/kb-server`:

```bash
#!/usr/bin/env bash
set -euo pipefail

CG="${KAFKA_PREFIX:+${KAFKA_PREFIX}.}kafka-batch"   # e.g. mothership.kafka-batch

groups_for() {
  case "$1" in
    control)          echo "$CG-control" ;;
    scheduler)        echo "$CG-control" ;;   # the poller rides the (light) control group
    fair-time)        echo "$CG-dispatch-time $CG-jobs-fair-time" ;;
    fair-throughput)  echo "$CG-dispatch-throughput $CG-jobs-fair-throughput" ;;
    jobs)             echo "$CG-jobs" ;;
    fast)             echo "$CG-jobs-fast" ;;
    slow)             echo "$CG-jobs-slow" ;;
    *) echo "unknown KB_ROLE component: $1" >&2; exit 1 ;;
  esac
}

ROLE="${KB_ROLE:-all}"
[ "$ROLE" = "all" ] && exec bundle exec karafka server   # run everything (dev / single node)

GROUPS=""
IFS=',' read -ra PARTS <<< "$ROLE"
for r in "${PARTS[@]}"; do
  r="$(echo "$r" | xargs)"; [ -z "$r" ] && continue
  for g in $(groups_for "$r"); do
    case ",$GROUPS," in *",$g,"*) : ;; *) GROUPS="${GROUPS:+$GROUPS,}$g" ;; esac  # dedupe
  done
done

exec bundle exec karafka server --include-consumer-groups "$GROUPS"
```

The schedule poller isn't a consumer group — it's a per-process thread gated by `config.schedule_poller_enabled`. Drive it from the same role list (with an optional hard override) in your initializer:

```ruby
# config/initializers/kafka_batch.rb
roles = ENV.fetch("KB_ROLE", "all").split(",").map(&:strip)
config.schedule_poller_enabled =
  case ENV["KB_SCHEDULE_POLLER"]          # optional explicit override
  when "true"  then true
  when "false" then false
  else (roles & %w[all scheduler]).any?   # default: poller only where scheduler/all is present
  end
```

| `KB_ROLE` | Runs | Poller | Autoscale on |
|---|---|---|---|
| `control` | events · callbacks · retries | off | events-topic lag |
| `scheduler` | control group **+ schedule poller** | on | fixed 1–3 pods |
| `fair-time` | `dispatch-time` + `jobs-fair-time` | off | `fair_time_ready` lag |
| `fair-throughput` | `dispatch-throughput` + `jobs-fair-throughput` | off | `fair_throughput_ready` lag |
| `jobs` | plain worker topics | off | `-jobs` lag |
| `fast` / `slow` | priority topics | off | those topics' lag |
| `all` | everything (dev / single node) | on | — |

```yaml
# k8s — same image everywhere; only the envs change per Deployment
containers:
- image: my-app:latest
  command: ["bin/kb-server"]
  env:
    - { name: KB_ROLE,        value: "fair-throughput" }
    - { name: KAFKA_PREFIX,   value: "mothership" }
    - { name: KB_CONCURRENCY, value: "10" }
```

**Clubbing roles into one pod.** `KB_ROLE` is a list, so you can co-locate roles: `KB_ROLE=control,scheduler` runs the control group **and** the poller in a single pod (the two `control` group names de-dupe to one `--include-consumer-groups`). Handy when your control-plane volume is small and you'd rather not run a separate scheduler Deployment. Arbitrary combinations work too (e.g. `KB_ROLE=control,jobs`).

> ⚠️ **Don't over-scale `control` / `scheduler`.** These are *control-plane* roles, not throughput roles, and they converge on shared hot state:
> - **`control`** — every `EventConsumer` increments the **same** per-batch counter hash (`kafka_batch:b:{batch_id}`) and the single `kafka_batch:counts` summary, plus the reconciler lock. For a hot batch, all completion events funnel onto that one key; adding control pods past a handful buys no throughput and just piles concurrent writers onto the hot key.
> - **`scheduler`** — every poller claims due jobs from the **same** schedule store: a hot Redis key/ZSET (Redis backend) or `SELECT … FOR UPDATE SKIP LOCKED` on `kafka_batch_scheduled_jobs` (MySQL backend). Claims are atomic so nothing double-fires, but more pollers = more contention on that hot key / row / table (and more store query volume), not more work done.
>
> Keep `control` to a few pods and `scheduler` to a small fixed 1–3 (that's why they're split from the job roles). Do your **horizontal scaling on the execution roles** — `jobs`, `fair-*`, `fast`/`slow` — where partitions, not a shared key, are the unit of parallelism. If you clubbed `control,scheduler`, remember that scaling that Deployment scales the poller too; at that point split `scheduler` back out (`KB_ROLE=scheduler`) and set `KB_ROLE=control` (poller off) on the wider control Deployment.

#### Running both fairness lanes on one process

If a process includes **both** lanes' groups (the default single-process setup), the lanes stay **independently fair among their own tenants** but **share that process's execution resources**:

- **Independent per lane:** two schedulers (separate Redis namespaces, weights, and `fairness_global_concurrency` budgets — so up to 2× in-flight across both), two forwarder threads, correct per-lane slot release (via `_fair_type`). A hot tenant in one lane never distorts the other lane's fairness.
- **Shared on the process:** both lanes' ready topics are drained by the **same Karafka `concurrency` thread pool**, and there is **no fairness *between* lanes** — the pool runs whatever's fetched, so a busy lane can take most of the CPU. Fairness is *within a lane, across tenants*; between lanes it's first-come-first-served.

So: co-running is correct and simplest — just size `config.concurrency` for the **combined** load of both lanes. If you need the lanes to *not* compete for CPU (separate scaling / SLOs), run them in separate processes using the per-lane `--include-consumer-groups` split above.

---

## Completion counting & scalability

Knowing when a batch is "done" requires idempotent counting over an at-least-once event stream. KafkaBatch deduplicates by **`job_id`**: each job counts at most once toward its batch, regardless of completion order on the source partition.

Each completion event carries the job message's immutable source coordinates (`src_topic`, `src_partition`, `src_offset`) for provenance and is keyed by `src_topic/src_partition` so completion processing spreads across event-topic partitions. Dedup itself uses `SADD` on `kafka_batch:b:dedup:{batch_id}` with the event's `job_id` as the member — redelivered or re-produced events for the same job are rejected; a later job with a lower source offset on the same partition is still counted.

**Guarantees**

- ✅ Exact completion counting; out-of-order parallel completions are handled correctly.
- ✅ Horizontally scalable completion processing (per-partition event keys, not per-batch row locks).
- ✅ Relies on the **idempotent producer** (enabled by default) so the worker topic itself can't contain produce-retry duplicates.

**Trade-offs / what is *not* guaranteed**

- ❌ No per-job audit in the batch ledger — only aggregate counts (`completed_count` / `failed_count`). Failures are visible in the dead-letter topic and the failure log (`/failures`).
- ❌ `perform` still runs **at-least-once** — workers must be idempotent (unchanged).
- ⚠️ Dedup state is O(jobs per batch) in Redis while the batch is active (expires with `batch_ttl`).

> **Why no Kafka transactions?** True exactly-once read-process-write with transactional offset commits is a Karafka **Pro** feature. `job_id` dedup plus the idempotent producer reaches the same *counting* guarantee on open-source Karafka — no broker transactions required.

---

## Defining workers

Include `KafkaBatch::Worker` and implement `#perform`:

```ruby
class ProcessOrderWorker
  include KafkaBatch::Worker

  kafka_topic            "orders.process" # optional – defaults to config.jobs_topic
  max_retries            5                # optional – overrides config.max_retries
  complete_after_retries 3                # optional – overrides config.complete_after_retries

  # payload is the Hash passed to Batch#push or Batch.enqueue
  def perform(payload)
    order = Order.find(payload["order_id"])
    order.process!
  end
end
```

> **Workers must be idempotent.** If `perform` succeeds but the subsequent event-emission fails, the job message is redelivered and `perform` runs again. Design your workers so running twice produces the same result (upsert, check-before-write, etc.).

### The `kafka_topic` is optional (shared default queue)

If a worker doesn't declare a `kafka_topic`, it falls back to `config.jobs_topic` (default `"kafka_batch.jobs"`). Several topic-less workers therefore **share one queue** — `JobConsumer` still dispatches each message to the correct worker via the `worker_class` embedded in the message, so this is safe:

```ruby
class SendEmailWorker
  include KafkaBatch::Worker      # no kafka_topic → uses config.jobs_topic
  def perform(payload) = Mailer.deliver(payload["id"])
end

class SyncCrmWorker
  include KafkaBatch::Worker      # also config.jobs_topic — same queue, different worker
  def perform(payload) = Crm.sync(payload["id"])
end
```

Declare an explicit `kafka_topic` when you want a worker isolated on its own topic/partitions (independent scaling, ordering, or lag). A worker that opts into **fairness** (`fairness true`) ignores its own topic — its jobs flow through the shared ingest → ready lane and are executed by the `-jobs-fair` consumer group instead of `-jobs`.

---

## Creating batches

```ruby
batch = KafkaBatch::Batch.create(
  on_success:  "BatchSuccessCallback",   # called if ALL jobs succeed
  on_complete: "BatchCompleteCallback",  # called when ALL jobs finish (any status)
  description: "Nightly report rebuild",  # optional human label, shown in the Web UI
  meta: { report_id: 42, user_id: 99 }  # arbitrary data forwarded to callbacks
) do |b|
  Order.find_each do |order|
    b.push(ProcessOrderWorker, { order_id: order.id })
  end
end

puts batch.id  # => "550e8400-e29b-41d4-a716-446655440000"
```

`description:` is an optional free-text label to help you tell batches apart in the dashboard (shown on both the list and detail pages). Stored in the Redis batch hash — no migration needed.

There is **no lock step**. A batch stays **open** and accepts more jobs — from anywhere, including from jobs that belong to it — until it **completes** (all jobs done → callback fires) or is cancelled. The completion callback fires automatically the moment the batch drains (`completed + failed >= total_jobs`).

The **block form is recommended** for one-shot population: the batch is held open for the duration of the block, so it cannot complete mid-population even if early jobs finish before later ones are pushed. When the block returns it is sealed and finalizes once everything is done.

> **Wrap the payload in `{ }`.** Because `push`/`push_many`/`enqueue` accept a
> `job_id:` keyword, a brace-less hash (e.g. `push(W, order_id: 1)`) is parsed by
> Ruby 3 as keyword arguments and raises `ArgumentError: unknown keyword`. Always
> pass the payload as an explicit Hash: `push(W, { order_id: 1 })`.

An optional explicit `job_id` can be passed for tracing:

```ruby
b.push(ProcessOrderWorker, { order_id: 1 }, job_id: "order-1-#{Time.now.to_i}")
```

### Adding jobs over time

**Add jobs from inside a running job** (the main reason there's no lock) — a worker can fan out into its *own* batch via `batch` (nil for standalone jobs). This is always safe: a running job is itself a pending unit, so the batch can't drain while it runs, and its children are counted before its own completion is recorded.

```ruby
class CrawlPageWorker
  include KafkaBatch::Worker
  kafka_topic "crawl.pages"

  def perform(payload)
    page = fetch(payload["url"])
    page.links.each do |link|
      batch&.push(CrawlPageWorker, { "url" => link })   # add child jobs to the same batch
    end
  end
end
```

**Push many at once** — grows `total_jobs` with a single store write, then produces each job:

```ruby
batch.push_many(ProcessUserWorker, users.map { |u| { "user_id" => u.id } })
# => ["job-uuid-1", "job-uuid-2", ...]
```

**Re-attach from another process** with `Batch.open(id)`:

```ruby
KafkaBatch::Batch.open(batch_id).push(ProcessUserWorker, { "user_id" => 7 })
```

- `Batch.create` **without a block** returns a `Batch` that is sealed immediately, so it completes as soon as it drains. If every pushed job can finish before you push more, prefer the block form — otherwise the callback may fire early and further pushes raise `KafkaBatch::BatchClosedError`.
- `Batch.open(id)` re-attaches to an existing batch so you can `push`/`push_many` from anywhere (raises `BatchNotFoundError` if unknown).
- `total_jobs` updates live as you push (visible in `Batch.find` and the [Web UI](#web-ui)).
- Pushing into a **completed** or **cancelled** batch raises `KafkaBatch::BatchClosedError`.
- If a `push` fails to produce, the job count is rolled back so the total stays accurate.

> The reconciler skips held (block-form, not-yet-sealed) batches, so an in-progress population is never mistaken for a stuck one.

### Standalone jobs (no batch)

```ruby
KafkaBatch::Batch.enqueue(ProcessOrderWorker, { order_id: 99 })
```

The job goes through the same retry / DLT flow but no batch completion tracking occurs. For a **fair worker** (`fairness true`), pass `tenant_id:` so the job lands on the ingest topic under the right tenant key — see [Enqueuing fair jobs](#enqueuing-fair-jobs).

### Batch.find and Batch.cancel

```ruby
# Look up the current state of a batch
batch = KafkaBatch::Batch.find(batch_id)
# => { id: "uuid", status: "running", completed_count: 42, total_jobs: 100, ... }

# Cancel a batch: remaining jobs are skipped and callbacks never fire
KafkaBatch::Batch.cancel(batch_id)
```

`cancel` sets `status` to `"cancelled"` in the store. With `config.skip_cancelled_jobs = true` (the default), the `JobConsumer` **skips execution** of not-yet-processed jobs in that batch — so cancelling effectively stops the remaining work.

To avoid a store read on every job, each consumer process caches the set of cancelled batch ids and refreshes it at most once per `config.cancellation_cache_ttl` seconds (default 120). Cancellation is therefore **eventually-consistent**: some already-queued jobs may still run until the next refresh — an accepted trade-off for throughput. The `EventConsumer` also treats a cancelled batch as a no-op, so callbacks never fire regardless.

Set `config.skip_cancelled_jobs = false` to disable the cancellation gate entirely (cancel then only suppresses callbacks).

You can also cancel (and delete) batches from the [Web UI](#web-ui).

---

## Callbacks

Callbacks are plain Ruby classes with a method matching the callback type:

```ruby
class BatchSuccessCallback
  # Called only when every job in the batch succeeded (failed_count == 0).
  def on_success(batch)
    AdminMailer.batch_complete(
      id:        batch["batch_id"],
      count:     batch["total_jobs"],
      meta:      batch["meta"]
    ).deliver_later
  end
end

class BatchCompleteCallback
  # Called when all jobs finish regardless of individual failure count.
  def on_complete(batch)
    if batch["failed_count"].positive?
      Sentry.capture_message("Batch #{batch['batch_id']} had failures", extra: batch)
    end
    TempStorage.delete(batch.dig("meta", "temp_dir"))
  end
end
```

The `batch` hash passed to callbacks:

```ruby
{
  "batch_id"        => "uuid",
  "outcome"         => "success",   # "success" | "complete"
  "total_jobs"      => 1000,
  "completed_count" => 998,
  "failed_count"    => 2,
  "on_success"      => "BatchSuccessCallback",
  "on_complete"     => "BatchCompleteCallback",
  "meta"            => { "report_id" => 42 },
  "finished_at"     => "2024-01-15T10:30:00Z",
  "reconciled"      => false        # true if fired by the reconciler
}
```

| Callback | When it fires |
|---|---|
| `on_success` | All jobs succeeded (`failed_count == 0`) |
| `on_complete` | All jobs finished regardless of failures |

**At-least-once guarantee (callbacks must be idempotent):** `CallbackConsumer` invokes the callbacks **first**, then claims dispatch by setting `callback_dispatched_at`. Because callback messages are keyed by `batch_id`, all callbacks for a batch land on a single partition and are processed sequentially, so a duplicate message is cheaply suppressed by the pre-invocation `callback_dispatched?` check. A crash between invocation and the claim results in re-invocation on redelivery — never a silently lost callback. This matches Sidekiq Pro's "callbacks may run more than once" semantics, so **make your callbacks idempotent**.

**Unresolvable class names:** If the callback class doesn't exist (typo, rename after deploy), the message is forwarded to `dead_letter_topic` with `dlt_type: "callback"` instead of being silently dropped.

**Callback exceptions forwarded to DLT:** If a callback class raises `StandardError` at runtime, the error is forwarded to the DLT with `dlt_type: "callback_error"` so it is visible and replayable. Dispatch is still claimed afterwards (the failure is captured in the DLT) — if you need retry semantics on a callback, make the callback class a `KafkaBatch::Worker` itself.

---

## Retry behaviour

When a job raises an exception, `JobConsumer` catches it and takes one of two paths based on the current attempt count:

**Retriable (attempt < max_retries):**
The message is produced to the retry topic **for its delay tier** (`<retry_topic>.short` / `.medium` / `.large`) with two extra fields:
- `retry_after` — ISO8601 timestamp of when to re-enqueue
- `retry_to` — the original worker topic to re-enqueue to

The `JobConsumer` partition is immediately freed for the next message. No thread blocking occurs.

**RetryConsumer** (subscribed to all tier topics) picks up the message. If `retry_after` is still in the future, it calls Karafka's `pause(offset, ms)` to suspend that partition for up to `config.retry_max_pause_seconds` seconds (default 30s) at a time, then checks again. When the message is due it re-enqueues to `retry_to` and commits.

**Exhausted (attempt == max_retries):**
A `failed` event is emitted to the events topic (so the batch counter is updated) and the message is forwarded to the dead-letter topic.

### Tiered retries

Each delay tier gets its **own Kafka topic**. Because messages within a topic are consumed in (roughly) produce order — which here equals due order, since a fixed delay is added to every message — a slow tier can never head-of-line-block a fast one. The defaults:

| Tier | Delay | Topic |
|------|-------|-------|
| `short`  | 30s    | `kafka_batch.jobs.retry.short`  |
| `medium` | 7 min  | `kafka_batch.jobs.retry.medium` |
| `large`  | 20 min | `kafka_batch.jobs.retry.large`  |

Each delay has `±retry_jitter` (default 10%) randomization to avoid synchronized retry storms.

**Default progression:** the Nth retry walks `retry_tier_progression` (`[:short, :medium, :large]`), clamping to the last tier for all further retries. So with `max_retries: 3` a job retries at ~30s (short), ~7m (medium), ~20m (large), then dead-letters.

**Per-worker tier pinning:** a worker can route *all* of its retries to a single tier — useful when a class of jobs should always back off slowly (or quickly) regardless of attempt:

```ruby
class WebhookWorker
  include KafkaBatch::Worker
  kafka_topic "webhooks.deliver"
  max_retries 6
  retry_tier  :medium   # every retry waits ~7 min, never short or large
end
```

Tune the delays and progression globally:

```ruby
config.retry_tiers            = { short: 30, medium: 7 * 60, large: 20 * 60 }
config.retry_tier_progression = %i[short medium large]
```

> For long downstream outages this exhausts retries within ~`max_retries × largest tier delay`; raise `max_retries` or replay from the DLT. The **time until the next retry** is recorded on each retrying failure and shown in the dashboard's *Job failures* "Next retry" column (e.g. `in 2m 47s`).

> **Migration note:** the three tier topics (`<retry_topic>.short/.medium/.large`) replace the single `<retry_topic>`. Create them before deploying and drain any in-flight messages on the old single retry topic first (it is no longer consumed).

Override attempts per worker with `max_retries`, and the tier with `retry_tier`.

## Delayed jobs (`perform_in` / `perform_at`)

The Sidekiq equivalent for scheduling a job to run later. **Two naming conventions decide which method you call:**

- **`_in` takes a duration** (seconds from now, e.g. `6 * 60` or `6.minutes`); **`_at` takes an absolute `Time`** (e.g. `Time.now + 3600`). Passing a duration to an `_at` method schedules it for ~1970.
- **`enqueue_*` are standalone class methods** on `KafkaBatch::Batch` (job has no batch); **`push_*` are instance methods** on a batch (the job counts toward that batch's `total_jobs`, so `on_complete` waits for it). Inside a `create do |b|` block, call `b.push_*` — **not** `b.enqueue_*` (which doesn't exist on the instance).

### Full API matrix

| | delay (interval → `_in`) | absolute time (`_at`) |
|---|---|---|
| **Single, standalone** | `Worker.perform_in(6.minutes, payload)` <br> `Batch.enqueue_in(360, Worker, payload)` | `Worker.perform_at(t, payload)` <br> `Batch.enqueue_at(t, Worker, payload)` |
| **Single, into a batch** (`b`) | `b.push_in(6.minutes, Worker, payload)` | `b.push_at(t, Worker, payload)` |
| **Bulk, standalone** | `Worker.perform_bulk_in(360, payloads)` <br> `Batch.enqueue_many_in(360, Worker, payloads)` | `Worker.perform_bulk_at(t, payloads)` <br> `Batch.enqueue_many_at(t, Worker, payloads)` |
| **Bulk, into a batch** (`b`) | `b.push_many_in(6.minutes, Worker, payloads)` | `b.push_many_at(t, Worker, payloads)` |

`payload` is a single Hash; `payloads` is an Array of Hashes (one per job). `perform_async(payload)` runs a single job immediately (no delay).

```ruby
MyWorker.perform_async("id" => 1)               # run now
MyWorker.perform_in(5 * 60, "id" => 1)          # run in 5 minutes
MyWorker.perform_at(Time.now + 3600, "id" => 1) # run at an absolute time

# Inside a batch — use push_* (instance), the batch waits for the delayed jobs:
KafkaBatch::Batch.create(on_complete: "MyCallback") do |b|
  b.push_in(6 * 60, MyWorker, "id" => 1)                        # one delayed job
  b.push_many_in(6 * 60, MyWorker, 10.times.map { |i| { "n" => i } })  # N delayed jobs, one round-trip
end
```

> **Payload keys round-trip as strings.** The message is serialized to JSON, so `{ n: 1 }` (symbol key) arrives in `perform` as `payload["n"]` — read it with a **string** key, not `payload[:n]`. Prefer string keys when enqueuing to avoid surprises.

**Bulk** (`*_many_*` / `perform_bulk_*`) schedules many jobs sharing one run-at time: all payloads are produced to the scheduled topic with one `produce_many_sync`, and their pointers are written with one `ZADD` (Redis) / one `INSERT` (MySQL). The batch form grows `total_jobs` by the payload count in a single atomic reservation and rolls it all back if scheduling fails.

**Why not the retry-topic `pause` approach?** That works for retries because their delays are short and monotonic, but pausing a Kafka partition blocks *every* later message behind the longest delay — wrong for arbitrary user scheduling. Instead, delayed jobs use the same design Sidekiq itself uses: a time-ordered index drained by a poller.

**How it works.** The job payload is produced to a durable `scheduled_topic`; the schedule index stores only a **compact pointer** `job_id:partition:offset` scored by run-at time (so index size is independent of payload size). A per-process **`SchedulePoller`** claims due pointers, reads their payloads back from Kafka (grouped by partition, sorted by offset, so scattered reads become near-sequential), and re-produces each job onto its real topic — where the normal `JobConsumer` / fairness lane runs it unchanged.

**Pluggable index backend**, detached from `config.store`:

```ruby
config.schedule_store = :redis   # default — ZSET, RAM-resident, lowest latency
config.schedule_store = :mysql   # table, disk-resident, cheap at scale, native per-job cancel
```

**At-least-once / crash recovery.** `claim_due` moves due pointers to a *leased* state; the poller re-produces them and only then `ack`s. If a poller (or the whole process) crashes between claim and ack, the lease expires and `reclaim` — run by *any* poller in *any* process — returns the pointer to pending, so nothing is lost. Claims are atomic per backend (Redis Lua / MySQL `SELECT … FOR UPDATE SKIP LOCKED`), so every consumer process can run a poller with no double-dispatch and no leader election. Duplicates are rare and safe (completions dedup by `job_id`; the producer is idempotent).

**Cancellation.** With `:mysql`, `rake kafka_batch:cancel_scheduled JOB_ID=…` deletes the pending row and decrements its batch. With `:redis`, cancel the batch — the poller drops cancelled jobs at dispatch via the `CancellationCache`.

**Retention constraint.** The pointer references a Kafka offset, so **set the `scheduled_topic`'s `retention.ms` ≥ `config.max_schedule_horizon`** (default 7 days); schedules beyond the horizon are clamped. If a payload is ever gone (retention), the poller drops the pointer and logs rather than looping.

#### Controlling poller load with many pods

Every consumer pod runs one `SchedulePoller` thread. That's fine for the Redis backend (each poll is one atomic Lua call), but with the **MySQL** backend and, say, 150 pods, you don't want 150 pods issuing `SELECT … FOR UPDATE SKIP LOCKED` every few seconds. Three layers keep that in check:

1. **`SKIP LOCKED` already prevents contention** — pollers grab disjoint rows and never block each other; the concern is query *volume*, not lock waits.
2. **Adaptive idle backoff (automatic).** When a poll finds nothing due, each poller backs off exponentially from `schedule_poll_interval` (5s) up to `schedule_poll_max_interval` (60s), snapping back to the base cadence the instant a poll returns work. So an idle fleet settles at ~1 query per pod per 60s instead of per 5s (~12× fewer), yet ramps straight back up under load. `schedule_poll_jitter` (±10%) de-syncs pods so they never poll in lockstep — and because polls are staggered across many pods, due-job latency stays low even while each pod is backed off.
3. **Dedicate poller pods (recommended at scale).** Turn the poller *off* on most pods and run it on a small, fixed set. Wire `schedule_poller_enabled` to an env var and set it per deployment:

   ```ruby
   config.schedule_poller_enabled = ENV.fetch("KB_SCHEDULE_POLLER", "true") == "true"
   ```
   ```yaml
   # workers Deployment (100+ pods): KB_SCHEDULE_POLLER=false  → no polling
   # a small "scheduler" Deployment (2–3 pods): KB_SCHEDULE_POLLER=true → polls
   ```
   `claim_due` is atomic, so 2–3 concurrent pollers safely share the load (SKIP LOCKED hands each a disjoint slice) and give you HA without a leader election. Scale that small pool up only if the due-rate outgrows it.

Config knobs: `schedule_poller_enabled`, `schedule_poll_interval` (5s), `schedule_poll_max_interval` (60s idle-backoff cap), `schedule_poll_jitter`, `schedule_batch_size` (100), `schedule_lease_seconds` (60), `schedule_reclaim_interval` (30s), `max_schedule_horizon`. Operational: `rake kafka_batch:scheduled` lists pending jobs. When `schedule_store = :mysql`, run the `CreateKafkaBatchScheduledJobs` migration.

### Early batch completion (`complete_after_retries`)

A persistently-failing job can otherwise hold up its batch's **`on_complete`** for the whole retry budget (`max_retries × largest tier delay`), even when every other job is done. To cap that latency, a job counts toward its batch (as *failed*) after **`complete_after_retries`** retries (default **3**) — while it **keeps retrying in the background** up to `max_retries`:

```ruby
class FlakyWorker
  include KafkaBatch::Worker
  kafka_topic "flaky.jobs"
  max_retries            20   # keep trying for a long time
  complete_after_retries 3    # ...but don't make the batch wait past 3 retries
end
```

- The batch's `on_complete` fires once all jobs have either succeeded **or** hit `complete_after_retries`.
- Counting is **exactly once** — a `batch_counted` flag rides the retry message, so the later background retries (success or exhaustion) never double-count.
- **`on_success` is unaffected**: it still fires only when every job genuinely succeeds. A batch with an early-counted job reports outcome `complete` (a job was counted failed), so `on_success` won't fire even if that job later succeeds in the background.
- Default `complete_after_retries` (3) == default `max_retries` (3), so **default behaviour is unchanged** — set `max_retries` higher to benefit.

**Event emission retries:** If `perform` succeeds but the subsequent produce to `kafka_batch.events` fails (transient Kafka issue), the gem retries emission up to `EVENT_EMIT_RETRIES` (3) times with a short backoff. If all retries fail, the offset is left uncommitted so Karafka redelivers the job message and `perform` runs again. This is why workers must be idempotent.

---

## Dead Letter Topic

Jobs that exhaust all retries, and callback classes that cannot be resolved, are forwarded to `kafka_batch.dead_letter`. The payload is the original message augmented with:

```json
{
  "dlt_type":          "job",
  "dlt_source_topic":  "orders.process",
  "dlt_error_class":   "ActiveRecord::RecordNotFound",
  "dlt_error_message": "Couldn't find Order with id=99",
  "dlt_at":            "2024-01-15T10:30:00Z"
}
```

For unresolvable callback classes (`dlt_type: "callback"`) and callback runtime errors (`dlt_type: "callback_error"`):

```json
{
  "dlt_type":            "callback",
  "dlt_callback_class":  "MySuccessCallback",
  "dlt_callback_method": "on_success",
  "dlt_error_class":     "NameError",
  "dlt_error_message":   "uninitialized constant MySuccessCallback",
  "dlt_source_topic":    "kafka_batch.callbacks",
  "dlt_at":              "2024-01-15T10:30:00Z"
}
```

For malformed JSON payloads (events or callbacks topics), the raw payload is forwarded as:

```json
{
  "dlt_type":          "malformed_event",
  "dlt_source_topic":  "kafka_batch.events",
  "dlt_raw_payload":   "...",
  "dlt_error_class":   "ArgumentError",
  "dlt_error_message": "Invalid JSON in event: ...",
  "dlt_at":            "2024-01-15T10:30:00Z"
}
```

Subscribe a consumer in your `karafka.rb` to alert, log, or trigger manual replay:

```ruby
topic KafkaBatch.config.dead_letter_topic do
  consumer DeadLetterConsumer
end
```

---

## Priority queues

For non-fair jobs, kafka-batch offers a **4-topic, 2-group** priority system that lets you separate critical work from normal work without any runtime topic provisioning.

### Design

```
  fast_p0_topic ─────────────────────────────────────────────────────────►
  fast_p1_topic ──► {consumer_group}-jobs-fast ──► FastP0/P1Consumer ──►
                    (short-running jobs; weighted priority)

  slow_p0_topic ─────────────────────────────────────────────────────────►
  slow_p1_topic ──► {consumer_group}-jobs-slow ──► SlowP0/P1Consumer ──►
                    (long-running jobs; strict priority)
```

**Fast group** — for short-running jobs. When `fast_p0` has lag, `FastP1Consumer` pauses briefly (`priority_lag_check_interval` seconds) and yields CPU time to `FastP0Consumer`. Because fast jobs complete quickly the pause is small, and p1 never starves.

**Slow group** — for long-running jobs. When `slow_p0` has any lag, `SlowP1Consumer` pauses entirely until p0 is empty. No new p1 jobs start while p0 has a backlog. In-flight p1 jobs are not preempted (strict priority applies to *selection*, not execution).

Both groups are **independently scalable** and completely isolated from each other, from the fairness lane, and from the plain `-jobs` group.

### Enrolling a worker

A worker opts into priority simply by declaring `kafka_topic` to be one of the four configured topic names:

```ruby
class CheckoutWorker
  include KafkaBatch::Worker
  kafka_topic KafkaBatch.config.fast_p0_topic   # short job, critical → fast p0
end

class ReportWorker
  include KafkaBatch::Worker
  kafka_topic KafkaBatch.config.fast_p1_topic   # short job, normal → fast p1
end

class BackfillWorker
  include KafkaBatch::Worker
  kafka_topic KafkaBatch.config.slow_p0_topic   # long job, critical → slow p0
end

class BulkExportWorker
  include KafkaBatch::Worker
  kafka_topic KafkaBatch.config.slow_p1_topic   # long job, normal → slow p1
end
```

You can also reference the topic string directly — the config accessors just make it refactor-safe:

```ruby
kafka_topic "kafka_batch.jobs.fast_p0"
```

`draw_routes` automatically creates the `-jobs-fast` group when any worker uses `fast_p0_topic` or `fast_p1_topic`, and `-jobs-slow` when any worker uses `slow_p0_topic` or `slow_p1_topic`. No routing changes are needed in your `karafka.rb`.

> **`fairness true` takes precedence.** A worker with both `fairness true` and a priority `kafka_topic` routes through the fairness ingest lane — the priority topic is ignored.

### How the priority gate works

The priority gate uses `KafkaBatch::Lag` (the same Karafka Admin API wrapper the web UI uses) to check p0 lag. The check is **rate-limited per consumer instance** — at most once per `priority_lag_check_interval` seconds — so the cluster is not hit on every message. If the Admin API is unreachable, the check **fails open** (the consumer processes its messages rather than blocking indefinitely).

### Creating the topics

The `kafka_batch:create_topics` rake task always creates all four priority topics (they're included unconditionally, so they're ready before any worker adopts them):

```bash
bundle exec rake kafka_batch:create_topics
```

### Caveats

- Strict priority (slow group) applies to **new job selection only** — in-flight p1 jobs run to completion even if p0 work arrives. To guarantee p0 headroom under sustained p1 load, add dedicated consumer capacity to the slow group (scale up `-jobs-slow` concurrency) so p0 consumers are never fully blocked.
- The lag check fires an Admin API call at most once per `priority_lag_check_interval` — so there is a short window (up to that interval) after p0 work arrives before p1 consumers pause.
- Priority topics participate in the normal retry/DLT/callback flow identically to any other plain worker topic.

---

## Multi-tenant fairness (WFQ)

When many tenants (businesses) push jobs into the same system, a naive Kafka topic processes them roughly FIFO — so one tenant dumping 10M jobs starves everyone behind it. KafkaBatch shares capacity dynamically across tenants using a **Redis-backed Weighted-Fair-Queuing (WFQ) scheduler**. Redis is a **required** dependency of the gem.

Fairness is a **per-worker opt-in** (`fairness true` on the Worker class) — there's no global switch. Fair workers share the ingest → ready lanes (consumer groups `-dispatch` and `-jobs-fair`); plain workers use their own topics in the `-jobs` group. Both can run in the same Karafka process or in **separate processes** for independent scaling — see [Scaling consumer groups independently](#scaling-consumer-groups-independently).

**Two lanes run simultaneously** — a worker picks one with `fairness_type` (a single batch may mix both):

- **`fairness_type :time`** (default) — each tenant gets a fair, weighted share of **wall-clock execution time**. Virtual time advances at job *completion* by `duration / weight`. Correct for uneven job runtimes (e.g. 20–60 s jobs).
- **`fairness_type :throughput`** — each tenant gets a fair, weighted share of **dispatched job count**. Virtual time advances at *checkout* by `1 / weight`. Best when all tenants' jobs have similar runtimes.

Each lane is fully independent: its own ingest/ready topics, Redis WFQ scheduler (namespaces `kafka_batch:fair_time:*` / `kafka_batch:fair_throughput:*`), and per-tenant weights. Concurrency is shared work-conservingly **within each lane**: the per-lane in-flight window (`fairness_global_concurrency`) is split as `ceil(window / active_tenants)` — **1 active tenant uses the whole window; N split it evenly** — with an optional hard per-tenant ceiling (`fairness_max_inflight_per_tenant`).

```ruby
class CampaignSendWorker
  include KafkaBatch::Worker
  fairness true                 # go through the fair lane…
  fairness_type :time           # …weighted by wall-clock time (default)
end

class WebhookFanoutWorker
  include KafkaBatch::Worker
  fairness true
  fairness_type :throughput     # weighted by job count (uniform, fast jobs)
end

class InternalSyncWorker
  include KafkaBatch::Worker
  kafka_topic "internal.sync"   # plain worker, dedicated topic (no fairness)
end
```

The fair lane gives you:

- **1 active tenant → 100%** of the in-flight window; **2 → ~50:50**; **N → ~1/N each** — **work-conserving** (an idle tenant's share is instantly redistributed via the dynamic `ceil(window / active_tenants)` cap).
- **Weighted, exact fairness** via a virtual-time ring — not fetch-batch approximation. Weights are per-tenant and editable live on the **⚖ Weights** page.
- The durable, unbounded backlog stays in **Kafka** (the ingest topic); only a small bounded per-tenant staging window (`fairness_ready_window`) lives in Redis, with backpressure to Kafka when it fills.
- Choice of **time-based** or **job-count** fairness per worker via `fairness_type`, with both lanes active at once.

> **Does `fairness true` need Redis? Yes.** Redis is a **hard dependency** of KafkaBatch. The scheduler's virtual-time ring, in-flight counters, and per-tenant ready windows all live in Redis, and it drives the default fairness path (`config.redis_url` must be set — `Configuration#validate!` raises otherwise). The pieces:
> - **`Fairness::Dispatcher`** (consumer group `-dispatch-<lane>`) drains the lane's ingest topic into the bounded Redis WFQ window, pausing an ingest partition when a tenant's window is full (backpressure).
> - **`Fairness::Forwarder`** (one background thread **per lane** per dispatch process) checks out the fairest next job — concurrency-gated — and forwards it to the lane's ready topic.
> - **`JobConsumer`** (consumer group `-jobs-fair-<lane>`) runs `perform` and calls `Scheduler(lane)#complete(tenant, duration:)`, which releases the in-flight slot and (in the `:time` lane) advances the tenant's virtual time by `duration / weight`.
> The **⚖ Weights** page's "In-flight now" and "Queued" columns are now live, because the default path drives the scheduler's `checkout`/`complete`.

### Enqueuing fair jobs

For workers with `fairness true`, `push` and `enqueue` route to the **ingest** topic (not the worker's `kafka_topic`). The dispatcher forwards to **ready**, and `JobConsumer` on the `-jobs-fair` group runs `perform`. Everything downstream (events, callbacks, retry, DLT) is the same as plain workers.

**Always pass `tenant_id`** for multi-tenant fairness — the ingest message is keyed by `tenant_id` so tenants spread across partitions. If omitted, the gem falls back to `batch_id` (batch jobs) or `job_id` (standalone), which still works but won't share capacity fairly across tenants.

#### Standalone fair job (no batch)

Use `Batch.enqueue` with `tenant_id:`. `batch_id` is `nil` — no completion tracking or callbacks.

```ruby
# One-off fair job for tenant "acme"
KafkaBatch::Batch.enqueue(
  CampaignSendWorker,
  { "user_id" => 42, "campaign_id" => 99 },
  tenant_id: "acme"
)

# Fire-and-forget across many tenants
%w[acme globex initech].each do |tenant|
  KafkaBatch::Batch.enqueue(
    CampaignSendWorker,
    { "user_id" => 1 },
    tenant_id: tenant
  )
end
```

#### Fair job inside a batch

Set a default tenant on the batch; each `push` inherits it. Batch completion and callbacks work as usual.

```ruby
batch = KafkaBatch::Batch.create(
  on_success:  "CampaignBatchSuccessCallback",
  on_complete: "CampaignBatchCompleteCallback",
  tenant_id:   "acme",
  description: "Campaign #99 send",
  meta:        { campaign_id: 99 }
) do |b|
  User.where(business_id: acme.id).find_each do |user|
    b.push(CampaignSendWorker, { "user_id" => user.id })
  end
end
```

Override the tenant on individual pushes (same batch, multiple tenants):

```ruby
batch = KafkaBatch::Batch.create(on_complete: "Cb", tenant_id: "acme") do |b|
  b.push(CampaignSendWorker, { "user_id" => 1 })                      # tenant: acme
  b.push(CampaignSendWorker, { "user_id" => 2 }, tenant_id: "globex") # tenant: globex
end
```

#### Mixed batch (fair + plain workers)

Fair and plain workers can share one batch — each job routes by its worker class:

```ruby
batch = KafkaBatch::Batch.create(on_complete: "MixedCallback", tenant_id: "acme") do |b|
  b.push(CampaignSendWorker, { "user_id" => 1 })   # fair  → ingest → ready
  b.push(InternalSyncWorker, { "record_id" => 1 }) # plain → internal.sync topic
end
```

| | Standalone | Batch |
|---|---|---|
| API | `Batch.enqueue(Worker, payload, tenant_id: "acme")` | `batch.push(Worker, payload)` or `Batch.create(..., tenant_id: "acme") { \|b\| ... }` |
| `batch_id` | `nil` | batch UUID |
| Callbacks | none | `on_success` / `on_complete` |
| Fair routing | ingest → ready | same |
| Ingest Kafka key | `tenant_id`, or `job_id` if omitted | `tenant_id`, per-push override, or `batch_id` fallback |

Monitor ingest/ready depth on the Web UI **Time Fairness** (`/kafka_batch/fairness/time`) and **Throughput Fairness** (`/kafka_batch/fairness/throughput`) pages.

Each lane has its own topics (auto-derived from `topic_prefix`); the concurrency knobs apply per lane:

```ruby
# Per-lane ingest/ready topics (defaults shown):
config.fair_time_ingest_topic           = "kafka_batch.fair_time_ingest"
config.fair_time_ready_topic            = "kafka_batch.fair_time_ready"
config.fair_throughput_ingest_topic     = "kafka_batch.fair_throughput_ingest"
config.fair_throughput_ready_topic      = "kafka_batch.fair_throughput_ready"
config.fairness_global_concurrency      = 50    # per-lane in-flight window (bounds ready depth + concurrency)
config.fairness_max_inflight_per_tenant = 0     # optional hard per-tenant ceiling (0 = dynamic share only)
config.fairness_ready_window            = 500   # bounded per-tenant staging window in Redis
config.fairness_default_weight          = 1.0
```

### Dispatcher / forwarder fairness tuning

The default configuration gives good, exact fairness out of the box with **a single dispatch process**. You don't need to change anything to start — understand these knobs to tune further.

#### How 1 dispatch process achieves fairness across all tenants

Fairness is decided by the **Redis WFQ scheduler**, not by Kafka fetch balance, so a single dispatch process is sufficient regardless of ingest-partition layout:

**`fairness_global_concurrency` (default: 50)** is the in-flight window — the number of jobs forwarded to the ready topic but not yet completed. It bounds the ready-topic depth (so a newly active tenant only ever waits behind ~this many jobs — keeping fairness *dynamic*) and caps total fair-lane concurrency.

**Dynamic fair share.** Each tenant may hold at most `ceil(fairness_global_concurrency / active_tenants)` in-flight slots: **1 active tenant → the whole window; N active → window/N each** (work-conserving). `fairness_max_inflight_per_tenant` is an optional hard ceiling layered on top (0 = rely purely on the dynamic share). This dynamic cap is what enforces interleaving in the `:time` lane, where virtual time only advances at completion.

> **⚠ Weights need `fairness_weighted_concurrency = true` to change throughput.** This is the #1 "I set weights and nothing happened" gotcha:
> - With the **default `false`**, every active tenant gets an **equal** in-flight cap (`ceil(global_concurrency / active)`) under saturation, so weight only biases **selection order** — it does not change the throughput split once the lanes are contended. In the **`:time` lane** the effect is near-zero (equal caps + equal-duration jobs → equal throughput); in the **`:throughput` lane** you still see ordering skew.
> - Set **`config.fairness_weighted_concurrency = true`** to make each tenant's cap proportional to its weight (`floor(global_concurrency × w_t / Σ active w)`, min 1) — this is what turns weight into a real throughput share in both lanes.
> - Weights change the **share under sustained contention**, never eventual **totals**: enqueue 10 for A and 10 for B, drain fully, and both run 10 regardless of weight. Measure rate while both are backlogged.
> - **On-the-fly changes:** the throughput lane and the weighted caps read Redis directly (immediate); the time-lane completion path uses a per-process cache, so cross-pod propagation takes up to `fairness_weight_cache_ttl` (default 60s — lower it to react faster). Verify a weight actually landed with `redis-cli HGETALL kafka_batch:fair_time:weight` (or `…fair_throughput:weight`).

**`fairness_type`** (per worker) selects which lane a worker uses, i.e. what is shared fairly: weighted wall-clock time (`:time`) or weighted job count (`:throughput`). Both lanes run at once.

**Karafka concurrency (set in `karafka.rb`)** governs how many ingest partitions the Dispatcher drains into Redis in parallel and how many ready-topic messages the JobConsumer swarm executes concurrently. It does **not** affect fairness ordering (the scheduler owns that) — it affects raw throughput. `fairness_dispatcher_concurrency` only drives a boot warning if the Karafka setting looks too low.

```ruby
# karafka.rb
Karafka::App.setup do |config|
  config.concurrency = 5
end
```

The **`Fairness::Forwarder`** runs as one background thread per dispatch process; multiple processes may run it safely (each `checkout` is a single atomic Redis Lua call), so the fair lane is HA without double-dispatching.

#### Explicit tenant-to-partition mapping

By default the producer uses the `murmur2_random` partitioner to spread tenants evenly across ingest partitions (matching the Web UI partition lookup widget). To pin a specific tenant to a specific partition — for isolation, predictable routing, or debugging — use the explicit map:

```ruby
config.fairness_tenant_partitions = {
  "acme"   => 0,   # always lands on ingest partition 0
  "globex" => 1,   # always lands on ingest partition 1
  # all other tenants → murmur2_random hash
}
```

When a tenant is in the map, the producer passes `partition:` directly to WaterDrop (bypassing the partitioner entirely). The Web UI partition lookup widget shows a **configured** badge for these tenants. Out-of-range partition numbers (≥ ingest partition count) are logged as a warning and fall back to hash-based routing.

> If you pin a tenant that was previously hash-routed to a different partition, existing messages in the old partition are still consumed normally — no data migration needed.

### How it's wired

Execution stays on ordinary `JobConsumer`s; fairness is achieved by controlling the *order* jobs reach them, using a Redis virtual-time ring. Each stage has its **own consumer group** so fair and plain throughput scale independently:

```
push → ingest topic (per lane, durable backlog, keyed by tenant)
        │
   {cg}-dispatch-<lane>            (<lane> = time | throughput)
   Fairness::Dispatcher: enqueue → bounded Redis WFQ window (this lane's scheduler)
        │   (pauses the ingest partition when a tenant's window is full)
        ▼
   Redis WFQ ring (virtual-time ordered, per-tenant ready windows)
        │
   Fairness::Forwarder (one background thread PER LANE, per dispatch process)
        │   checkout the fairest job — global + dynamic per-tenant
        │   concurrency gated — and forward it to this lane's ready topic
        ▼
   ready topic (per lane)
        │
   {cg}-jobs-fair-<lane>
   JobConsumer swarm → perform → events → Scheduler(lane)#complete

plain worker push → worker topic → {consumer_group}-jobs → JobConsumer → events
```

What makes this fair:

1. **Virtual-time WFQ.** `#checkout` always picks the ready tenant with the smallest virtual time. In the `:throughput` lane vtime advances by `1/weight` at checkout; in the `:time` lane it advances by `duration/weight` when the JobConsumer calls `#complete` after `perform`. Returning idle tenants are re-admitted at the current minimum vtime so they cannot hoard capacity accrued while idle (**work-conserving**).
2. **Bounded in-flight window.** `fairness_global_concurrency` caps forwarded-but-not-completed jobs, keeping the ready topic shallow so a newly active tenant only ever waits behind ~a window's worth of work. The dynamic `ceil(window / active_tenants)` per-tenant share prevents any one tenant from filling the window.

`draw_routes` wires this automatically when **any** registered worker declares `fairness true`. The durable, unbounded backlog stays in the **ingest topic (Kafka)**; the ready topic + existing retry/DLT path keep the usual at-least-once guarantees. **Retries for fair workers** re-enter the **ready** topic directly (skipping the scheduler) and do not re-hold an in-flight slot.

> **Redis is required.** The virtual-time ring, in-flight counters, and per-tenant ready windows all live in Redis (`config.redis_url`). `Configuration#validate!` raises if it is unset.

### The WFQ Scheduler (engine behind the default path)

`KafkaBatch::Fairness::Scheduler` is the Redis virtual-time WFQ engine that the default fairness path is built on (`Fairness::Dispatcher` enqueues into it; `Fairness::Forwarder` checks out of it; `JobConsumer` completes into it). It has no Karafka dependency — it is pure Redis Lua — so you can also drive it directly if you want to build a custom dispatcher/worker loop.

**`/weights` dashboard:** because the default path now drives `checkout`/`complete`, the "In-flight now" and "Queued" cards and the per-tenant In-flight / vtime columns are **live**. Per-tenant weights set on this page take effect within `fairness_weight_cache_ttl` seconds across all processes.

The Scheduler's API:

```ruby
sched = KafkaBatch.scheduler   # process-wide singleton (Redis is required and always configured)

sched.enqueue(tenant_id, payload)     # push a job into the tenant's ready queue; returns :ok or :full
sched.checkout                         # pull the next job fairly; returns { tenant_id:, payload: } or nil
sched.complete(tenant_id, duration: n) # release the in-flight slot; advances vtime in the :time lane
sched.set_weight(tenant_id, 2.0)      # give tenant double throughput share
sched.delete_weight(tenant_id)        # revert to default_weight
sched.all_tenants                      # Array<Hash> — powers the /weights page
sched.stats                            # global snapshot (active tenants, total in-flight, budget)
```

#### Custom dispatcher example

Below is a minimal Karafka consumer that feeds the Scheduler (`enqueue`) from the ingest topic and then drives `checkout`/`complete` in a tight worker loop. In practice you would likely run the enqueue side inside `Fairness::Dispatcher` (or your own Karafka consumer) and the checkout/complete side inside a separate worker thread pool — but both halves are shown here for clarity.

```ruby
# app/consumers/wfq_dispatcher_consumer.rb
#
# Karafka consumer: reads from the fairness ingest topic and loads
# each job into the Scheduler's bounded per-tenant ready queue.
# A separate thread (WfqWorkerThread below) drains jobs via checkout.
class WfqDispatcherConsumer < Karafka::BaseConsumer
  def consume
    sched = KafkaBatch.scheduler
    return unless sched   # Redis not configured — no-op

    messages.each do |message|
      payload    = message.raw_payload             # already JSON string
      tenant_id  = Oj.load(payload)["tenant_id"].to_s

      result = sched.enqueue(tenant_id, payload)   # :ok or :full
      if result == :full
        # Ready window is at capacity for this tenant — apply backpressure.
        # Pause this partition for a moment so the worker thread can drain it.
        pause(message.offset, 500)
        return
      end
    end

    mark_as_consumed!(messages.last)
  end
end
```

```ruby
# app/workers/wfq_worker_thread.rb
#
# A plain Ruby thread (start it from an initializer or a Karafka
# lifecycle hook) that continuously pulls jobs from the Scheduler
# and runs them.  Nothing Karafka-specific here — the Scheduler is
# pure Ruby + Redis.
class WfqWorkerThread
  POLL_INTERVAL = 0.05   # seconds to sleep when nothing is ready

  def self.start(concurrency: 5)
    concurrency.times.map { new.tap(&:run_async) }
  end

  def run_async
    Thread.new { run }
  end

  def run
    sched = KafkaBatch.scheduler
    loop do
      job = sched.checkout    # { tenant_id:, payload: } or nil
      if job.nil?
        sleep POLL_INTERVAL
        next
      end

      tenant_id = job[:tenant_id]
      payload   = Oj.load(job[:payload])
      started   = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      begin
        worker_class = Object.const_get(payload["worker_class"])
        worker_class.new.perform(payload)
      rescue => e
        KafkaBatch.logger.error("[WfqWorker] #{tenant_id} failed: #{e.message}")
        # production code: emit a failure event, forward to DLT, etc.
      ensure
        duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        # complete MUST be called even on failure so the in-flight slot is released.
        # Pass duration: so the :time lane accounts for actual wall-clock usage.
        sched.complete(tenant_id, duration: duration)
      end
    end
  end
end
```

```ruby
# config/initializers/wfq_worker.rb
#
# Start the worker threads after Rails has fully booted.
# Adjust concurrency to match fairness_global_concurrency.
Rails.application.config.after_initialize do
  if KafkaBatch.scheduler && !Rails.env.test?
    WfqWorkerThread.start(concurrency: KafkaBatch.config.fairness_global_concurrency)
    KafkaBatch.logger.info("[WfqWorker] started #{KafkaBatch.config.fairness_global_concurrency} threads")
  end
end
```

With this in place, `checkout` and `complete` write to `kafka_batch:fair_<type>:leases` (+ the per-tenant `…:lease:<tenant_id>`) and `kafka_batch:fair_<type>:ring` (per lane), so the **In-flight now** and **Queued** counters on the `/weights` page become live. Weights set via the UI propagate to all dispatcher processes within `fairness_weight_cache_ttl` seconds.

> **Checkout returns `nil` when:**
> - The global concurrency budget (`fairness_global_concurrency`) is exhausted — all slots full.
> - No tenant currently has jobs in its ready queue — everything is drained or waiting on `enqueue`.
>
> In both cases the worker thread sleeps `POLL_INTERVAL` and retries. In production, use a proper semaphore or condition variable instead of `sleep` to avoid busy-waiting.

### Scaling fair vs plain jobs

Because fair jobs (`-jobs-fair`) and plain jobs (`-jobs`) are separate consumer groups:

- A burst of fair, multi-tenant campaign work does **not** block plain internal/sync jobs on `-jobs`, and vice versa.
- Size **ready** partitions and `-jobs-fair` consumer count for fair throughput; size plain worker topics and `-jobs` consumers for non-fair throughput.
- The dispatcher's throttle watches lag on `-jobs-fair` only — plain job backlog does not affect fair forwarding.

See [Scaling consumer groups independently](#scaling-consumer-groups-independently) for `--include-consumer-groups` examples.

### Partitioning & topic sizing (important)

The ingest and ready topics have **different sizing constraints** — one is about tenant spread, the other about consumer throughput. Size them independently.

#### Ingest topic — sized for tenant fairness

Tenants are mapped to ingest partitions by **Kafka's key-hash partitioner** (message key = `tenant_id`). Consequences:

- A given tenant **always lands on the same partition** (consistent hashing); tenants spread across partitions by hash.
- It is **hash-based, not a guaranteed 1-tenant-per-partition mapping.** When you have more tenants than partitions (or on hash collisions), **multiple tenants share a partition** and FIFO-contend with each other — fairness *between those tenants* degrades. A single-partition topic gives **no fairness at all**.
- **You must pre-create the ingest topic with enough partitions.** The gem does **not** create topics or set partition counts. Relying on Kafka auto-create yields the broker default (often 1 partition).

Size ingest partitions to your **expected concurrent active tenants**, not your total tenant count — in typical SaaS deployments most tenants are idle at any time:

> **Ingest partitions ≈ `max(50, total_distinct_tenants / 4)`**
>
> At minimum 50; scale to ~25% of total tenants once that is larger. If 1 in 4 tenants is active on average, this gives good per-tenant partition isolation without over-provisioning.

Keep `-dispatch` consumer count equal to ingest partition count — a dispatch consumer without a partition is wasted and adds rebalance overhead.

#### Ready topic — sized for consumer throughput

This is a different constraint. Kafka assigns **at most one partition per consumer** in a group. If `-jobs-fair` has 100 pods and the ready topic has only 64 partitions, 36 pods get assigned zero partitions and sit permanently idle — they joined the group, participated in the rebalance, and do no work.

> **Ready partitions ≥ consumer pod count** — the hard minimum so every pod gets ≥1 partition
>
> **Ready partitions = pod count × Karafka concurrency** — for full thread utilization during bursts

With 100 pods and `concurrency = 5`: 500 ready partitions means each pod gets 5 partitions and all 5 threads stay busy under load. There is a natural ceiling though — the forwarder's in-flight window (`fairness_global_concurrency`) caps how deep the ready topic ever gets, so going much beyond `pods × concurrency` adds broker metadata overhead for no throughput gain.

```bash
# Example: 400 total tenants, 100 job-consumer pods, concurrency = 5.
# Size each lane you use (fair_time_* and/or fair_throughput_*) the same way.
kafka-topics --create --topic kafka_batch.fair_time_ingest --partitions 100 --replication-factor 3
#            ingest = max(50, 400/4) = 100   (tenant fairness)

kafka-topics --create --topic kafka_batch.fair_time_ready  --partitions 500 --replication-factor 3
#            ready  = 100 pods × 5 concurrency  (consumer throughput)
```

**Boot safety check:** when any worker opts into fairness, KafkaBatch checks the ingest topic's partition count at Rails boot and again when Karafka starts. It **warns** if the count is below `config.fairness_min_ingest_partitions` (default `2`; a single partition is always flagged). Set `config.validate_topics_on_boot = true` to **raise** instead of warn:

```ruby
config.fairness_min_ingest_partitions = 50  # warn/raise if the ingest topic has fewer
```

> For **strict** 1:1 tenant→partition isolation you'd supply a custom WaterDrop partitioner mapping `tenant_id → a dedicated partition`; the gem ships only the standard key-hash partitioning.

---

## Web UI

A small, dependency-free Rack dashboard (think a tiny "Sidekiq Web") for inspecting batches. It works with either store and renders self-contained HTML/CSS — no asset pipeline or extra gems.

Mount it in your routes:

```ruby
# config/routes.rb
require "kafka_batch/web"

Rails.application.routes.draw do
  mount KafkaBatch::Web => "/kafka_batch"
end
```

> **Mount it behind authentication.** The UI exposes destructive actions (cancel / delete). Wrap the mount in your admin constraint, e.g.:
>
> ```ruby
> authenticate :user, ->(u) { u.admin? } do
>   mount KafkaBatch::Web => "/kafka_batch"
> end
> ```

What it shows:

- **Summary metrics** — total batches and counts by status (running / success / complete / cancelled).
- **Batch list** — newest first, with status badge, total / done / failed / **pending** counts, a progress bar, and status filters. Paginated.
- **Batch detail** — all fields, callbacks, meta, progress, and a **Job failures** list. Failures are recorded on the **first failed attempt** (status `retrying` while retries remain, `failed` once exhausted) so problems surface immediately rather than after hours of retries. Each row shows the worker, attempt #, error class/message, and time. Upserted per job and bounded by the number of failing jobs, so it's cheap even for huge batches.
- **All failures** (`/failures`) — a cross-batch view of every failure in one place (linked from the dashboard), filterable by `retrying` / `failed`, each row linking back to its batch.
- **Live activity** (`/live`) — currently-running jobs (job, batch, worker, which consumer process, topic/partition, start time) and the live consumer processes (host, PID, last-seen), auto-refreshing every 5s. It's **approximate** (very short-lived jobs may not appear between snapshots). Choose a backend with `config.liveness_backend`:
  - **`:redis`** (default) — full per-job + per-consumer tracking in Redis (`config.redis_url`) with a short TTL (`config.liveness_ttl`, default 30s), best-effort behind a circuit breaker so it never slows jobs; crashed entries expire on their own (no sweep needed). If Redis isn't reachable, the page says the feature is unavailable. (`config.track_running_jobs = false` disables the per-job writes.)
  - **`:off`** — disabled.
- **Actions** —
  - **Cancel** (running batches): sets status to `cancelled`; with `skip_cancelled_jobs` the remaining jobs stop processing (eventually-consistent — within `cancellation_cache_ttl`).
  - **Delete**: removes the batch record (best used for finished batches).

Routes (relative to the mount point):

| Method | Path | Action |
|---|---|---|
| `GET` | `/` | Batch list + metrics (`?status=`, `?page=`) |
| `GET` | `/batches/:id` | Batch detail |
| `POST` | `/batches/:id/cancel` | Cancel batch |
| `POST` | `/batches/:id/delete` | Delete batch |

> **Note:** the batch list is backed by `kafka_batch:index:all`. Batch keys expire after `batch_ttl` (refreshed on activity); expired members are pruned lazily from the index.

---

## Reconciler

The reconciler detects and recovers two classes of stuck batches:

### 1. Stuck-running batches

**Cause:** `EventConsumer` lag or message loss — all jobs completed but the counter never reached `total_jobs` because event messages were never produced or consumed.

**Detection:** `status = "running"` and `created_at < now - reconciliation_interval`.

**Recovery:** Compares `completed_count + failed_count` against `total_jobs`. If equal, transitions status and re-produces the callback message.

### 2. Lost-callback batches

**Cause:** `EventConsumer` updated the store to `success`/`complete` but crashed before or during the produce to `kafka_batch.callbacks`. The batch is "done" in the store but the callback was never fired.

**Detection:** `status IN (success, complete)` AND `callback_dispatched_at IS NULL` AND `finished_at < now - reconciliation_interval`.

**Recovery:** Re-produces the callback message to `kafka_batch.callbacks`. The `CallbackConsumer`'s `callback_dispatched_at` claim suppresses duplicates in the normal path, so re-producing is safe even if this runs multiple times (callbacks must be idempotent).

> The reconciler indexes (`kafka_batch:index:running` and `kafka_batch:index:done`) are maintained automatically in Redis as batches move through their lifecycle.

**Distributed lock:** `Reconciler.run` acquires a Redis distributed lock (`SET NX EX` on `kafka_batch:b:reconciler_lock`) before sweeping, so concurrent runs from multiple processes are safe.

### Automatic scheduling (no cron required)

The reconciler runs automatically inside `EventConsumer` — no external cron or Whenever config needed. Each time the events topic is polled, `EventConsumer` checks whether `reconciliation_interval` seconds have elapsed since the last run. If so, it fires the reconciler in a background thread so event processing is never delayed.

The check uses a class-level timestamp shared across all event-topic partitions in the same process, so the run frequency matches `config.reconciliation_interval` regardless of partition count. Across multiple app processes the distributed lock ensures only one sweep runs at a time.

```ruby
config.reconciliation_interval = 300  # seconds between automatic reconciler runs (default)
```

The rake task is still available for manual one-off runs (e.g. during a deploy, or to force an immediate sweep):

```bash
bundle exec rake kafka_batch:reconcile
```

---

## Instrumentation

KafkaBatch emits `ActiveSupport::Notifications` events at key lifecycle points so you can wire in metrics, logging, or alerting without modifying the gem.

| Event | Payload |
|---|---|
| `job.processed.kafka_batch` | `job_id`, `batch_id`, `worker_class`, `duration` |
| `job.retried.kafka_batch` | `job_id`, `batch_id`, `worker_class`, `attempt`, `next_attempt`, `retry_after` |
| `job.failed.kafka_batch` | `job_id`, `batch_id`, `worker_class`, `attempt`, `error_class`, `error_message` |
| `job.cancelled.kafka_batch` | `job_id`, `batch_id`, `worker_class` — fired when a job is skipped because its batch was cancelled |
| `batch.completed.kafka_batch` | `batch_id`, `outcome`, `total_jobs`, `completed_count`, `failed_count` |
| `callback.invoked.kafka_batch` | `batch_id`, `callback_class`, `callback_method` |
| `callback.failed.kafka_batch` | `batch_id`, `callback_class`, `callback_method`, `error_class`, `error_message` |
| `consumer.priority_yielded.kafka_batch` | `consumer_class`, `p0_topic`, `consumer_group`, `pause_ms` — fired when a p1 consumer pauses for p0 lag |
| `reconciler.ran.kafka_batch` | `stale_count`, `lost_count`, `duration`, `triggered_by` (`:rake` or `:consumer`) |

Subscribe in an initializer:

```ruby
# config/initializers/kafka_batch_instrumentation.rb

ActiveSupport::Notifications.subscribe("job.processed.kafka_batch") do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  StatsD.increment("kafka_batch.job.processed", tags: ["worker:#{event.payload[:worker_class]}"])
  StatsD.timing("kafka_batch.job.duration_ms", event.duration)
end

ActiveSupport::Notifications.subscribe("job.failed.kafka_batch") do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  Sentry.capture_message("KafkaBatch job exhausted retries", extra: event.payload)
end

ActiveSupport::Notifications.subscribe("batch.completed.kafka_batch") do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  StatsD.increment("kafka_batch.batch.completed", tags: ["outcome:#{event.payload[:outcome]}"])
end

# Alert when p1 consumers are being throttled frequently — a sign you need
# more p0 workers or that a p0 backlog is growing faster than it's consumed.
ActiveSupport::Notifications.subscribe("consumer.priority_yielded.kafka_batch") do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  StatsD.increment("kafka_batch.priority.yield",
    tags: ["consumer:#{event.payload[:consumer_class]}", "p0_topic:#{event.payload[:p0_topic]}"])
end
```

When `ActiveSupport` is not available (non-Rails environments), all instrumentation calls are no-ops — the gem works without it.

---

## Rake tasks

| Task | Description |
|---|---|
| `kafka_batch:create_topics` | Create all configured Kafka topics (idempotent). `PARTITIONS=N` forces every topic to N partitions; `REPLICATION_FACTOR=N` (default 1) |
| `kafka_batch:topics` | Dry-run: print the full topic plan (names, partition counts, replication factor) without creating anything |
| `kafka_batch:reconcile` | Run both reconciler sweeps (stuck-running + lost-callback) |
| `kafka_batch:install_migrations` | Copy ancillary MySQL migrations (failures, pauses, tenant weights) to `db/migrate/` |
| `kafka_batch:workers` | Print all registered workers, topics, and retry config |

> **Use `bundle exec rake`, not `rails`.** The `rails` command only dispatches built-in Rails commands (like `db:migrate`) — gem-provided Rake tasks must be run with `bundle exec rake kafka_batch:*`.

### Provisioning topics (the "migration" for Kafka)

Kafka has no migration system, so `kafka_batch:create_topics` is the equivalent: it derives the full topic set from your current config and creates any that are missing. It is idempotent (existing topics are skipped, never mutated) and uses per-topic default partition counts unless you override them. The set it creates:

- **Job topics** — for any fair worker, the shared **ingest** and **ready** topics; for plain workers, **each worker's own `kafka_topic`** (the rake task eager-loads the app so all workers are discovered; if none are loaded it falls back to `config.jobs_topic`).
- **Control plane** — events, callbacks, the three retry tier topics (`…retry.short/.medium/.large`), and the dead-letter topic.

```bash
# sensible per-topic defaults (jobs=6, events=3, retry tiers=3 each, …)
bundle exec rake kafka_batch:create_topics

# force every topic to 12 partitions, replication factor 3
PARTITIONS=12 REPLICATION_FACTOR=3 bundle exec rake kafka_batch:create_topics
```

Or call it from Ruby (e.g. a deploy hook):

```ruby
KafkaBatch::Topics.create_all!(partitions: 12, replication_factor: 3)
# => { created: [...], skipped: [...], failed: [...] }
```

> Kafka can only **grow** partition counts, never shrink them, so this task never alters an existing topic. To change partitioning, manage the topic with your Kafka admin tooling.

```bash
bundle exec rake kafka_batch:workers
#  ProcessOrderWorker   → topic: orders.process   retries: 5
#  GenerateReportWorker → topic: reports.generate retries: 3
```

---

## Reliability guarantees

| Guarantee | How it's achieved |
|---|---|
| **Batch never prematurely completes** | Store record (with exact `total_jobs`) is written before the first message is produced |
| **Partial produce is cleaned up** | Any `StandardError` in `flush!` calls `delete_batch` to roll back the store record |
| **Job completion is idempotent** | Per-batch `job_id` dedup (`SADD` on `kafka_batch:b:dedup:{id}`) rejects redelivered and re-produced events |
| **Counter increment is atomic** | Redis Lua script (`HINCRBY` + finalize in one eval); EventConsumer pipelines a whole poll in one round-trip |
| **Redis `create_batch` is race-free** | Lua script uses `HSETNX` as existence sentinel — single atomic operation, no TOCTOU |
| **Callback fires at least once** | Callback is invoked, then `callback_dispatched_at` is set; duplicates are suppressed and crashes lead to safe re-invocation (callbacks must be idempotent) |
| **Lost callbacks are recovered** | Reconciler scans for `status IN (success,complete) AND callback_dispatched_at IS NULL` |
| **Reconciler runs once per cluster** | Redis distributed lock (`SET NX EX` on `kafka_batch:b:reconciler_lock`) |
| **Retries don't block partitions** | Failed jobs go to per-tier `kafka_batch.jobs.retry.*` topics; `RetryConsumer` uses Karafka `pause()` |
| **Event emission failure ≠ job failure** | Separate rescue blocks; emission retried independently before leaving offset uncommitted |
| **Malformed JSON is never silently dropped** | Unparseable messages in all consumers are forwarded to DLT before committing |
| **Cancellation stops remaining jobs** | `JobConsumer` skips jobs of cancelled batches using a per-process cancelled-id cache (eventually-consistent within `cancellation_cache_ttl`) |
| **Callback exceptions are not silently swallowed** | `StandardError` in callbacks → DLT with `dlt_type: "callback_error"` |
| **Unresolvable callbacks are not silently dropped** | Forwarded to `dead_letter_topic` with `dlt_type: "callback"` |
| **DLT publish failure causes redelivery** | If DLT produce fails, offset is left uncommitted so Karafka redelivers the message |
| **Consumer crash after callback but before commit** | `claim_callback` CAS prevents double-invocation on redelivery |
| **Worker resolution is fast** | `WORKER_CACHE` hash caches `const_get` lookups per class name; thread-safe via mutex |
| **Store and worker registry are thread-safe** | Double-checked locking with `Mutex` on both `store` and `workers` singleton accessors |

---

## Known limitations

**Workers must be idempotent.** If event emission fails after a successful `perform`, Karafka redelivers the job and `perform` runs again. Design workers to tolerate duplicate execution (upsert, guard clauses, etc.).

**No per-job audit in the batch ledger.** Completion counting stores aggregate counts only (`completed_count` / `failed_count`). Use the `/failures` dashboard, DLT, or your own logging for per-job forensics.

**Redis TTL.** Batch keys expire after `batch_ttl` seconds. The TTL is refreshed on every job completion event, but a batch with no activity for longer than `batch_ttl` will lose its state. Set `batch_ttl` well above your longest expected batch duration.

**Worker class renames after deploy.** In-flight messages carry the original class name. After removing or renaming a worker, the consumer forwards those jobs straight to the DLT (and emits a `failed` event so the batch still completes) rather than blocking the partition. Perform a rolling deploy or drain the topic before removing the class.

**No automatic metrics sink.** Instrumentation events are emitted via `ActiveSupport::Notifications` (see [Instrumentation](#instrumentation)) but nothing is sent to Prometheus/StatsD by default. Subscribe to the events to forward them to your metrics backend.

**Fair-lane in-flight slots after a hard crash.** A fair-lane slot is normally released when the `JobConsumer` finishes a job. If a consumer is hard-killed mid-job (SIGKILL / OOM / node loss), that release never runs — so each slot is instead a **lease** that expires after `fairness_lease_ttl` (default 30 min) and is reclaimed automatically on the next checkout (and by a periodic sweep). A lane therefore self-heals rather than staying wedged, **provided `fairness_lease_ttl` exceeds your longest job runtime** — set it too low and long-running jobs get their slots reclaimed early (soft concurrency overshoot, not data loss).

---

## Migrating from Sidekiq Pro Batches

| Sidekiq Pro | kafka-batch |
|---|---|
| `Sidekiq::Batch.new` | `KafkaBatch::Batch.create` |
| `batch.jobs { MyWorker.perform_async(...) }` | `b.push(MyWorker, ...)` inside block |
| `batch.on(:success, MyCallback)` | `on_success: "MyCallback"` parameter |
| `batch.on(:complete, MyCallback)` | `on_complete: "MyCallback"` parameter |
| Callback `#on_success(status)` | `#on_success(batch_hash)` |
| Callback `#on_complete(status)` | `#on_complete(batch_hash)` |
| `status.bid` | `batch["batch_id"]` |
| `status.total` | `batch["total_jobs"]` |
| `status.failures` | `batch["failed_count"]` |

**What you don't need to change:** Callback class names and method signatures are structurally the same.

**Key difference:** Workers must `include KafkaBatch::Worker` and be **idempotent** (a `kafka_topic` is optional — it defaults to `config.jobs_topic`). They are consumed by Karafka rather than Sidekiq threads.

---

## Architecture deep-dive

### Why write the batch record before producing?

If the store record were written after producing, a fast consumer could complete all N jobs and find no batch record — resulting in `not_found` and a permanently lost callback. Writing first with the exact count guarantees the store is ready before any completion event can arrive.

### Why roll back on partial produce failure?

If only M of N messages reach Kafka, the store has `total_jobs: N` but only M jobs will ever complete. Without rollback, the batch hangs in "running" indefinitely. With `delete_batch`, the caller receives a `ProducerError` and can retry the entire `Batch.create` call.

### Why Redis Lua for completion counting?

Completion dedup (`SADD job_id`) and counter increment (`HINCRBY`) run in a single Lua script per event. This avoids lost updates and double-counting without hot-row contention. The EventConsumer pipelines a whole Kafka poll into one Redis round-trip for throughput.

### Why separate `perform` and event-emission rescue blocks?

With a single `rescue`, a transient Kafka error on `emit_event` looks identical to a job failure and triggers a job retry. The work was already done — retrying the job runs it again (possibly corrupting state) and eventually sends a false "failed" event to the DLT. Separate rescue blocks mean event-emission failures are retried independently, and only a worker-raised exception triggers the job retry path.

### Why a dedicated retry topic instead of `sleep`?

A `sleep` inside `JobConsumer` blocks the entire Kafka partition for the backoff duration. The retry-topic approach forwards the message immediately and suspends only the *retry partition* (via Karafka `pause()`) — the job partition is fully unblocked. Splitting retries across per-tier topics keeps each retry partition's head-of-line pause bounded by **that tier's** delay, so a slow tier never blocks a fast one.

### Why invoke the callback first, then claim?

Callbacks are **at-least-once**: the `CallbackConsumer` invokes the callback, then sets `callback_dispatched_at`. A crash between the two re-invokes on redelivery (never a lost callback) — matching Sidekiq Pro. Duplicates in the normal path are suppressed by a pre-invocation `callback_dispatched?` check, which is reliable because callback messages are keyed by `batch_id` (one partition → one consumer → sequential). Make callbacks idempotent.

### Message flow (numbered)

```
1.  App             → Redis             CREATE batch record (total_jobs = N)
2.  App             → Kafka jobs topic  PRODUCE N job messages (idempotent producer)
3.  JobConsumer     → worker            CALL perform(payload)
4a. (success)       → Kafka events      PRODUCE {batch_id, job_id, status: success, src_*}
4b. (failure)       → Kafka retry       PRODUCE {retry_after, retry_to, attempt+1}
4c. (exhausted)     → Kafka events      PRODUCE {batch_id, status: failed, src_*}
                    → Kafka DLT         PRODUCE original message + error context
5.  RetryConsumer   pauses partition    WAIT until retry_after via Karafka pause()
                    → Kafka jobs topic  PRODUCE message back to original topic
6.  EventConsumer   → Redis             ATOMIC job_id dedup + increment + finalize check
7.  EventConsumer   → Kafka callbacks   PRODUCE callback message (if batch done)
8.  CallbackConsumer → callback class   INVOKE on_success / on_complete, then CLAIM
```

**For a fair worker** (`fairness true`), step 2 changes to: `Batch.push → Kafka ingest topic` (keyed by `tenant_id`), then `Fairness::Dispatcher` (`-dispatch` group) → Kafka ready topic (fairly ordered + throttled), then `JobConsumer` on the **ready** topic in the `-jobs-fair` group. Plain workers still use step 2 as written (`-jobs` group). Steps 3–8 are otherwise unchanged.

---

## Topic reference

| Topic (default name) | Consumer group | Produced by | Consumed by | Purpose |
|---|---|---|---|---|
| `kafka_batch.jobs` (per worker) | `{consumer_group}-jobs` | `Batch.create` / `Batch.enqueue` | `JobConsumer` | Individual job messages (plain, non-priority, non-fair workers) |
| `kafka_batch.jobs.fast_p0` | `{consumer_group}-jobs-fast` | `Batch.create` / `Batch.enqueue` | `FastP0Consumer` | Fast critical jobs (always runs; no gate) |
| `kafka_batch.jobs.fast_p1` | `{consumer_group}-jobs-fast` | `Batch.create` / `Batch.enqueue` | `FastP1Consumer` | Fast normal jobs (yields briefly when p0 has lag) |
| `kafka_batch.jobs.slow_p0` | `{consumer_group}-jobs-slow` | `Batch.create` / `Batch.enqueue` | `SlowP0Consumer` | Slow critical jobs (always runs; no gate) |
| `kafka_batch.jobs.slow_p1` | `{consumer_group}-jobs-slow` | `Batch.create` / `Batch.enqueue` | `SlowP1Consumer` | Slow normal jobs (pauses while p0 has lag) |
| `kafka_batch.jobs.retry.{short,medium,large}` | `{consumer_group}-control` | `JobConsumer` | `RetryConsumer` | Failed jobs awaiting their tier's delay |
| `kafka_batch.events` | `{consumer_group}-control` | `JobConsumer` | `EventConsumer` | Job completion signals |
| `kafka_batch.callbacks` | `{consumer_group}-control` | `EventConsumer` / `Reconciler` | `CallbackConsumer` | Batch-complete triggers |
| `kafka_batch.dead_letter` | — | `JobConsumer` / `CallbackConsumer` / `RetryConsumer` | Your consumer | Exhausted jobs + unresolvable callbacks |
| `kafka_batch.fair_time_ingest` / `kafka_batch.fair_throughput_ingest` *(fairness only)* | `{consumer_group}-dispatch-<lane>` | `Batch.push` (keyed by `tenant_id`) | `Fairness::Dispatcher` | Per-tenant intake queue per lane (durable backlog) |
| `kafka_batch.fair_time_ready` / `kafka_batch.fair_throughput_ready` *(fairness only)* | `{consumer_group}-jobs-fair-<lane>` | `Fairness::Dispatcher` | `JobConsumer` | Fairly-ordered, throttled execution queue per lane |

For a **fair worker** (`fairness true`), jobs flow `ingest → ready → JobConsumer` in the `-dispatch` / `-jobs-fair` groups instead of straight to the worker's own topic. For a **priority worker** (topic set to one of the four `fast_*/slow_*` topics), jobs flow straight to that topic and are consumed by the matching priority consumer in the `-jobs-fast` or `-jobs-slow` group. See [Priority queues](#priority-queues), [Multi-tenant fairness](#multi-tenant-fairness-wfq), and [Scaling consumer groups independently](#scaling-consumer-groups-independently).

---

## Data reference

A complete map of every Redis key and MySQL table the gem owns — useful when debugging unexpected behavior or inspecting state directly.

---

### Redis keys

All keys use the `kafka_batch:` namespace. The gem never writes outside it.

#### Batch ledger (always — `config.store` `:redis` or `:mysql`)

| Key pattern | Type | TTL | Contents |
|---|---|---|---|
| `kafka_batch:b:<batch_id>` | HASH | `batch_ttl` (refreshed on each completion) | All batch state — see fields below |
| `kafka_batch:b:dedup:<batch_id>` | SET | `batch_ttl` | Applied `job_id` members (completion dedup) |
| `kafka_batch:b:<batch_id>:failures` | HASH | `failures_ttl` | field = `job_id`, value = JSON failure record (`store: :redis` only) |
| `kafka_batch:index:all` | ZSET | none (pruned lazily) | All batch IDs; score = `created_at` epoch. Powers the UI batch list. Capped to `all_index_max_size` |
| `kafka_batch:index:running` | ZSET | none | Running batch IDs; score = `created_at` epoch. Used by the reconciler to find stuck-running batches |
| `kafka_batch:index:done` | ZSET | none | Finished-but-uncallbacked batch IDs; score = `finished_at` epoch. Used by the reconciler to find lost-callback batches |
| `kafka_batch:index:cancelled` | ZSET | none (pruned by age) | Cancelled batch IDs; score = cancellation epoch. Read by `CancellationCache` to skip remaining jobs |
| `kafka_batch:counts` | HASH | none | O(1) status counters: fields `running`, `success`, `complete`, `cancelled`. Maintained atomically with every status transition |
| `kafka_batch:b:reconciler_lock` | STRING | `reconciler_lock_ttl` | Distributed lock token (SET NX EX). Only one reconciler sweep runs cluster-wide at a time |

**`kafka_batch:b:<batch_id>` hash fields:**

| Field | Type | Description |
|---|---|---|
| `id` | String | Batch UUID |
| `total_jobs` | Integer | Total expected jobs (grows as jobs are pushed) |
| `completed_count` | Integer | Jobs that succeeded |
| `failed_count` | Integer | Jobs that failed or were counted via `complete_after_retries` |
| `status` | String | `running` → `success` / `complete` / `cancelled` |
| `on_success` | String | Callback class name (nil = no callback) |
| `on_complete` | String | Callback class name (nil = no callback) |
| `meta` | JSON | Caller-supplied metadata Hash |
| `description` | String | Human label shown in the dashboard |
| `tenant_id` | String | Tenant identifier (fairness path) |
| `created_at` | ISO8601 | When the batch was created |
| `finished_at` | ISO8601 | When all jobs completed |
| `locked_at` | ISO8601 | When the batch was sealed (block-form population finished); empty while open |
| `callback_dispatched_at` | ISO8601 | When `CallbackConsumer` claimed dispatch; nil = callback not yet fired |
| `callback_dispatched_by` | String | Consumer pod/process that ran the callbacks |

**`kafka_batch:b:<batch_id>:failures` hash fields (value is JSON):**

| JSON key | Description |
|---|---|
| `job_id` | Job UUID |
| `worker_class` | Worker class name |
| `error_class` | Exception class |
| `error_message` | Exception message |
| `attempt` | 0-based retry attempt number |
| `status` | `retrying` while retries remain; `failed` once exhausted |
| `next_retry_at` | ISO8601 timestamp of the next retry (nil once exhausted) |
| `failed_at` | ISO8601 timestamp of this failure event |

---

#### Liveness (`config.liveness_backend = :redis`)

| Key pattern | Type | TTL | Contents |
|---|---|---|---|
| `kafka_batch:live:job:<consumer_id>:<job_id>` | STRING | `liveness_ttl` (default 30s) | JSON: `job_id`, `batch_id`, `worker_class`, `consumer_id`, `topic`, `partition`, `started_at`. Written when a job starts; expires automatically if the consumer crashes |
| `kafka_batch:live:consumer:<consumer_id>` | STRING | `liveness_ttl` | JSON: `consumer_id`, `hostname`, `pid`, `topic`, `last_seen`. One key per active consumer thread |

---

#### Consumption control (pause/resume)

| Key | Type | Contents |
|---|---|---|
| `kafka_batch:consumption:topics` | SET | Members: `"<group>\x1f<topic>"` — entire topics paused via the `/lag` UI |
| `kafka_batch:consumption:partitions` | SET | Members: `"<group>\x1f<topic>\x1f<partition>"` — individual partitions paused |

---

#### Delayed-job index (`config.schedule_store = :redis`)

The `perform_in` / `perform_at` index. Only present when `config.schedule_store = :redis` (independent of `config.store`). The job **payload lives in Kafka** (`config.scheduled_topic`); these ZSETs hold only a compact pointer `"<job_id>:<partition>:<offset>"`, so their size is independent of payload size. No TTL — entries persist until dispatched (`ack`) or reclaimed.

| Key | Type | Contents |
|---|---|---|
| `kafka_batch:sched:pending` | ZSET | member = `"<job_id>:<partition>:<offset>"`, score = run-at epoch (seconds). Due members (`score <= now`) are claimed by the `SchedulePoller` |
| `kafka_batch:sched:inflight` | ZSET | member = same pointer, score = lease-expiry epoch. Holds claimed-but-not-yet-dispatched jobs; `reclaim` returns any whose lease expired (crash recovery — at-least-once) |

> ```bash
> redis-cli ZCARD kafka_batch:sched:pending                    # scheduled jobs waiting
> redis-cli ZRANGEBYSCORE kafka_batch:sched:pending -inf +inf WITHSCORES  # pointers + run-at
> redis-cli ZCARD kafka_batch:sched:inflight                   # claimed, awaiting dispatch/reclaim
> ```

---

#### Fairness Scheduler (`KafkaBatch::Fairness::Scheduler`)

These keys only exist when `config.redis_url` is set. They are **independent of `config.store`** — the Scheduler always uses Redis for its WFQ mechanics regardless of whether your store is MySQL or Redis.

Keys are namespaced **per lane**: `<ns>` is `kafka_batch:fair_time` or `kafka_batch:fair_throughput`.

| Key | Type | Contents |
|---|---|---|
| `<ns>:weight` | HASH | field = `tenant_id`, value = Float weight override. Primary store when `config.store = :redis`; mirrored from MySQL (WHERE `fairness_type` = lane) when `store = :mysql` |
| `<ns>:vtime` | HASH | field = `tenant_id`, value = Float accumulated virtual time. Persists across idle periods so a returning tenant can't exploit accrued capacity |
| `<ns>:ring` | ZSET | member = `tenant_id`, score = vtime. Only **currently active** tenants (with queued ready jobs). The Scheduler always picks the lowest score |
| `<ns>:leases` | ZSET | member = `slot_id`, score = lease-expiry epoch (`checkout-time + fairness_lease_ttl`). **Authoritative global in-flight** for the lane: the concurrency budget is the count of members whose score is still in the future. Expired members (a dead consumer's leaked slot) are dropped on the next checkout, so the budget self-heals and can never be permanently pinned. Capped by `fairness_global_concurrency` |
| `<ns>:lease:<tenant_id>` | ZSET | member = `slot_id`, score = lease-expiry epoch. **Authoritative per-tenant in-flight**, drives the dynamic fair-share and hard (`fairness_max_inflight_per_tenant`) caps. Same self-healing expiry as the global set. A completion removes the `slot_id` from both this and `<ns>:leases` |
| `<ns>:ready:<tenant_id>` | LIST | Queued job payloads (JSON strings) for this tenant. LPOP'd at checkout; RPUSH'd at enqueue. Bounded by `fairness_ready_window` |
| `<ns>:reclaim_lock` | STRING | Short-lived (`SET NX EX`) single-flight lock so only one process runs the periodic expired-lease sweep at a time |

> **Quick debug commands** (swap `fair_time` for `fair_throughput` to inspect the other lane):
> ```bash
> redis-cli HGETALL kafka_batch:fair_time:weight         # all custom weights (time lane)
> redis-cli HGETALL kafka_batch:fair_time:vtime          # accumulated vtime per tenant
> redis-cli ZRANGE  kafka_batch:fair_time:ring 0 -1 WITHSCORES  # active tenants + their vtime
> redis-cli ZCOUNT  kafka_batch:fair_time:leases "($(date +%s))" +inf  # LIVE global in-flight (what the budget uses)
> redis-cli ZCARD   kafka_batch:fair_time:leases         # global leases incl. any not-yet-pruned expired
> redis-cli ZCARD   kafka_batch:fair_time:lease:acme     # in-flight for one tenant (per-tenant cap)
> redis-cli HGETALL kafka_batch:fair_throughput:weight   # throughput-lane weights
> redis-cli HGETALL kafka_batch:counts                   # batch status summary
> redis-cli ZCARD   kafka_batch:index:running            # how many batches are running
> redis-cli ZCARD   kafka_batch:index:done               # batches done but awaiting callback
> ```

---

### MySQL tables

Created only for the backends you opt into — the batch ledger and fairness WFQ mechanics are always Redis. Two independent flags select MySQL, each copying just the migration it needs (`rails g kafka_batch:install --store mysql --schedule-store mysql`):

| Table | Created when | Purpose |
|---|---|---|
| `kafka_batch_failures` | `--store mysql` | Per-job failure log (`/failures`) |
| `kafka_batch_consumption_pauses` | `--store mysql` | Pause/resume state (`/lag`) when Redis is down |
| `kafka_batch_tenant_weights` | `--store mysql` | Per-tenant, per-lane WFQ weight overrides |
| `kafka_batch_scheduled_jobs` | `--schedule-store mysql` | Delayed-job (`perform_in`/`perform_at`) index |

#### `kafka_batch_failures` — per-job failure log

One row per failing job (upserted on each failure event). Surfaced in the dashboard immediately as `retrying`, flipped to `failed` once the retry budget is exhausted. Bounded by `max_failures_per_batch`.

| Column | Type | Description |
|---|---|---|
| `id` | BIGINT PK | Auto-increment |
| `batch_id` | VARCHAR(36) | Parent batch UUID |
| `job_id` | VARCHAR(36) | Job UUID |
| `worker_class` | VARCHAR(255) | Worker class name |
| `error_class` | VARCHAR(255) | Exception class |
| `error_message` | TEXT | Exception message |
| `attempt` | INT | 0-based retry attempt when this record was written |
| `status` | VARCHAR(20) | `retrying` while retries remain; `failed` once exhausted |
| `next_retry_at` | DATETIME | Scheduled retry time (null once exhausted) |
| `failed_at` | DATETIME | Time of this failure event |

**Indexes:** unique on `(batch_id, job_id)`, `(batch_id, failed_at)` (per-batch listing), `failed_at` (cross-batch `/failures` view).

---

#### `kafka_batch_consumption_pauses` — pause/resume state

Pause/resume fallback for the `/lag` dashboard when Redis is unavailable and `config.store = :mysql`. A row's existence means that group/topic/partition is paused. `partition_id = -1` pauses the whole topic. When Redis is up, pause state is stored in Redis sets instead.

| Column | Type | Description |
|---|---|---|
| `id` | BIGINT PK | Auto-increment |
| `consumer_group` | VARCHAR(255) | Karafka consumer group name |
| `topic_name` | VARCHAR(255) | Kafka topic name |
| `partition_id` | INT | Partition index, or `-1` for whole-topic pause |
| `created_at` | DATETIME | When the pause was applied |

**Index:** unique on `(consumer_group, topic_name, partition_id)`.

---

#### `kafka_batch_tenant_weights` — WFQ weight overrides (per lane)

Per-tenant weight overrides for the Fairness Scheduler when `config.store = :mysql`. Weights are **per fairness lane** — a tenant can carry a different weight in the `time` and `throughput` lanes — so the row key is `(tenant_id, fairness_type)`. Each weight is mirrored to `kafka_batch:fair_<type>:weight` in Redis so the WFQ checkout Lua can read it. When `store: :redis`, weights are written to the per-lane Redis hash directly.

| Column | Type | Description |
|---|---|---|
| `id` | BIGINT PK | Auto-increment |
| `tenant_id` | VARCHAR(255) | Tenant identifier (matches the `tenant_id` passed to `Batch.create`) |
| `fairness_type` | VARCHAR(16) | `time` or `throughput` — which lane this weight applies to (default `time`) |
| `weight` | DECIMAL(10,4) | Weight multiplier (e.g. `2.0` = double share). Must be > 0 |
| `updated_at` | DATETIME | Last update time |

**Index:** unique on `(tenant_id, fairness_type)`.

---

#### `kafka_batch_scheduled_jobs` — delayed-job index

The `perform_in` / `perform_at` index when `config.schedule_store = :mysql` (detached from `config.store`). Holds a **compact pointer** to the payload, which lives in Kafka (`config.scheduled_topic`) — not the payload itself. `job_id` is the primary key so cancel/ack are O(1) point deletes; `run_at` drives the due scan; `lease_until` gives crash-safe at-least-once claiming via `SELECT … FOR UPDATE SKIP LOCKED`.

| Column | Type | Description |
|---|---|---|
| `job_id` | VARCHAR(36) PK | Job UUID (primary key; no auto-increment `id`) |
| `run_at` | DATETIME(6) | When the job becomes due |
| `partition_id` | INT | Kafka partition of the payload on `scheduled_topic` |
| `kafka_offset` | BIGINT | Kafka offset of the payload on `scheduled_topic` |
| `batch_id` | VARCHAR(36) | Parent batch UUID, or null for a standalone job |
| `lease_until` | DATETIME(6) | Set while claimed (null = claimable); expired leases are reclaimed |
| `created_at` | DATETIME(6) | When the job was scheduled |

**Indexes:** unique on `job_id`; `(run_at, lease_until)` (due scan); `batch_id`.

---

## Contributing

1. Fork the repo
2. `bundle install`
3. `bundle exec rspec`
4. Submit a PR

Please add tests for new behaviour and keep the store interface (`lib/kafka_batch/stores/base.rb`) in sync if you add a new store backend.

---

## License

MIT
