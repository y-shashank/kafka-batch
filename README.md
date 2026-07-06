# kafka-batch

[![CI](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/y-shashank/kafka-batch/badges/coverage.json)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)

**Sidekiq Pro Batches on Kafka.** Same `on_success` / `on_complete` callback model, per-job retries, and batch completion counting — with Kafka as the durable job transport and Redis for coordination.

Built on [Karafka](https://karafka.io) (WaterDrop + consumers).

---

## Table of contents

- [Non-negotiable pitfalls](#non-negotiable-pitfalls)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Workers & jobs](#workers--jobs)
- [Batches & callbacks](#batches--callbacks)
- [Configuration](#configuration)
- [Karafka routing & deployment](#karafka-routing--deployment)
- [Priority queues](#priority-queues)
- [Multi-tenant fairness](#multi-tenant-fairness)
- [Delayed jobs](#delayed-jobs)
- [Unique jobs & expiration](#unique-jobs--expiration)
- [Retries & dead letter](#retries--dead-letter)
- [Scaling & partitions](#scaling--partitions)
- [Web UI & reconciler](#web-ui--reconciler)
- [Instrumentation](#instrumentation)
- [Migrating from Sidekiq Pro](#migrating-from-sidekiq-pro)
- [Reference](#reference)
- [Contributing](#contributing)

---

## Non-negotiable pitfalls

Read these **before** production. They are architectural constraints, not bugs.

### 1. Jobs are at-least-once — workers must be idempotent

Kafka redelivers. Retries re-run `#perform`. Make every worker safe to run more than once (or guard with your own idempotency keys).

### 2. Redis is always required

Batch ledger, fairness scheduler, uniqueness locks, liveness, and reconciler locks all use Redis (`config.redis_url`). There is no Redis-free mode.

### 3. Callbacks are at-least-once — make them idempotent

`on_success` / `on_complete` may run more than once after a crash between invoke and claim. Same rule as Sidekiq Pro.

### 4. Each Kafka topic belongs to exactly one consumer group

A topic in a **priority YAML** must **not** also be consumed by flat `-jobs`. Boot validation rejects overlaps — configuring the same topic in two groups would **double-process** jobs.

### 5. Partition count is fixed at topic creation

Kafka cannot shrink partitions. Size execution topics for **peak pods × concurrency** (e.g. 150 × 5 → 768). Undersized topics leave pods idle at peak; oversized topics are harmless.

### 6. Mega-batches still hit one Redis hash per batch

Completion dedup is a bitmap (~1 bit/job), but counter updates go to `kafka_batch:b:{batch_id}`. A single 10M-job batch is a deliberate burst — shard into smaller batches in app policy if needed.

### 7. `fairness_weighted_concurrency = false` ignores weight ratios under load

Default is `true` (weights → in-flight throughput share). Set `false` only if you want every active tenant to get an **equal** in-flight cap regardless of weight.

### 8. `fairness_lease_ttl` must exceed your longest job

Hard-killed consumers release fair-lane slots via TTL expiry. Set TTL below max job runtime and you get soft concurrency overshoot (harmless, not data loss).

### 9. Schedule poller is off by default — enable only on scheduler pods

`schedule_poller_enabled` defaults to `false`. Turning it on every execution pod at 100+ replicas hammers the schedule store. Use `KB_ROLE=scheduler` on 2–3 fixed pods.

### 10. Cancellation is eventually consistent

Cancelled batches are cached per process (`cancellation_cache_ttl`, default 120s). Some already-fetched jobs may still run until the cache refreshes.

### 11. Priority applies to selection, not preemption

In-flight jobs are not killed when higher-priority backlog arrives. Strict mode blocks **new** lower-rank consumes until higher topics are drained.

### 12. No built-in Prometheus/StatsD sink yet

Events fire via `ActiveSupport::Notifications` only. You must subscribe yourself today — see [Instrumentation](#instrumentation) and the metrics plan below.

---

## Quick start

### 1. Add the gem

**Worker service** (runs Karafka consumers):

```ruby
# Gemfile
gem "kafka-batch"
```

**Web-only service** (dashboard, no consumers):

```ruby
gem "kafka-batch", require: "kafka_batch/ui"
```

> Use `require: "kafka_batch/ui"` in the web Gemfile — not `require: true` (LoadError on `kafka-batch`) or `require: false` (Rake tasks won't register).

### 2. Install & configure

```bash
bundle exec rails generate kafka_batch:install
# optional: --store mysql  (failure log / pauses in MySQL; ledger still Redis)
```

Edit `config/initializers/kafka_batch.rb` — at minimum:

```ruby
KafkaBatch.configure do |config|
  config.brokers   = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")
  config.redis_url = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  config.topic_prefix = ENV["KAFKA_PREFIX"].to_s.strip  # optional namespace
end
```

### 3. Create Kafka topics

```bash
bundle exec rake kafka_batch:create_topics
# or: KAFKA_BROKERS=localhost:9092 ./bin/create_kafka_topics.sh
# dry-run: bundle exec rake kafka_batch:topics
```

Topics are derived from registered workers, priority YAML files, and fairness settings.

### 4. Wire Karafka routes

Eager-load workers **before** `draw_routes` so the registry is populated:

```ruby
# karafka.rb
class KarafkaApp < Karafka::App
  routes.draw do
    KafkaBatch.draw_routes(self)
  end
end
```

### 5. Define a worker & run

```ruby
class ProcessOrderWorker
  include KafkaBatch::Worker

  def perform(payload)
    Order.find(payload["order_id"]).process!
  end
end
```

```ruby
KafkaBatch::Batch.create(description: "nightly") do |b|
  b.on_success  "NightlyCallback"
  b.on_complete "NightlyCallback"
  orders.each { |o| b.push(ProcessOrderWorker, { "order_id" => o.id }) }
end
```

```bash
bundle exec karafka server   # dev: all consumer groups
```

### 6. Mount the dashboard (optional)

```ruby
# config/routes.rb — protect behind auth
mount KafkaBatch::Web => "/kafka_batch"
```

---

## How it works

```
App  →  Redis (batch record FIRST)
     →  Kafka worker topic(s)
          → JobConsumer#perform
               ├─ success  → events topic → EventConsumer (bitmap dedup) → callbacks topic
               ├─ retry    → retry.{short|medium|large} → RetryConsumer → worker topic
               └─ exhausted → dead_letter + failed event

CallbackConsumer: invoke callback → then claim (at-least-once)
```

| Layer | Transport | Role |
|---|---|---|
| Job execution | Kafka worker topics | `JobConsumer` / `PriorityJobConsumer` |
| Completion counting | Kafka events + Redis bitmap | `EventConsumer` |
| Callbacks | Kafka callbacks | `CallbackConsumer` |
| Batch state | Redis hash | Counters, status, callback claim |
| Fairness ordering | Kafka ingest → Redis WFQ → Kafka ready | `Dispatcher` + `Forwarder` |
| Delayed jobs | Kafka scheduled + Redis/MySQL index | `SchedulePoller` |

**Fair workers** (`fairness true`) route through ingest → scheduler → ready instead of their own topic. **Priority workers** use topics from a priority YAML group. **Plain workers** use `kafka_topic` or the shared default `kafka_batch.jobs`.

---

## Workers & jobs

```ruby
class MyWorker
  include KafkaBatch::Worker

  kafka_topic "orders.process"   # optional — defaults to config.jobs_topic
  max_retries 5
  retry_tier :large            # pin all retries to one tier (optional)
  fairness true                # opt into WFQ lane (optional)
  fairness_type :time          # :time (default) or :throughput
  uniq true                    # dedup enqueue by payload (optional)

  def perform(payload)
    # must be idempotent
  end
end
```

### Standalone jobs (no batch)

```ruby
KafkaBatch::Batch.enqueue(MyWorker, { "id" => 1 })
KafkaBatch::Batch.enqueue_at(MyWorker, { "id" => 1 }, 1.hour.from_now)
KafkaBatch::Batch.enqueue_in(MyWorker, { "id" => 1 }, 30.minutes)
```

### Shared default queue

Workers without `kafka_topic` share `config.jobs_topic` (default `kafka_batch.jobs`). Dispatch is safe — each message embeds `worker_class`. Use a dedicated `kafka_topic` when you need independent scaling, ordering, or lag isolation.

---

## Batches & callbacks

```ruby
KafkaBatch::Batch.create(
  description: "import run",
  on_success:  "ImportCallbacks",
  on_complete: "ImportCallbacks",
  meta:        { "source" => "api" }
) do |b|
  b.push_many(ProcessUserWorker, users.map { |u| { "user_id" => u.id } })
end
```

```ruby
class ImportCallbacks
  def on_success(batch)
    notify_slack("All #{batch['total_jobs']} succeeded")
  end

  def on_complete(batch)
    notify_slack("#{batch['failed_count']} failed")
  end
end
```

| Method | Purpose |
|---|---|
| `Batch.create` | New batch; block form auto-seals when block exits |
| `Batch.open(id)` | Push more jobs into a running batch (jobs-adding-jobs) |
| `Batch.find(id)` | Fetch batch hash from Redis |
| `Batch.cancel(id)` | Mark cancelled; consumers skip within cache TTL |
| `Batch.enqueue` | Single job, optional `batch_id:` for open batches |

**`complete_after_retries`** (per worker or global): after N failures, count the job toward `on_complete` while retries continue in the background.

---

## Configuration

All options live on `KafkaBatch.config`. The install generator ships enterprise-oriented defaults; library `initialize` values are dev-sized.

### Essential options

| Option | Default | Notes |
|---|---|---|
| `brokers` | `["localhost:9092"]` | Kafka bootstrap servers |
| `redis_url` | `redis://localhost:6379/0` | **Required** |
| `topic_prefix` | `""` | Namespaces all topics + consumer group (`myapp` → `myapp.kafka_batch.jobs`, `myapp.kafka-batch`) |
| `store` | `:redis` | `:mysql` moves failure log / pauses to MySQL; **ledger stays Redis** |
| `schedule_store` | `:redis` | Delayed-job index (`:mysql` for disk-backed scale) |
| `schedule_poller_enabled` | `false` | Enable on scheduler pods only |
| `max_retries` | `3` | Override per worker |
| `validate_topics_on_boot` | `false` | Raise if topics missing at boot |
| `skip_cancelled_jobs` | `true` | |
| `cancellation_cache_ttl` | `120` | seconds |
| `priority_config_paths` | `[]` | Paths to priority YAML files |
| `fairness_weighted_concurrency` | `true` | Set `false` for equal in-flight cap per tenant (weights → order only) |
| `fairness_global_concurrency` | `50` | Per-lane in-flight window (install template: `1000`) |
| `fairness_lease_ttl` | `1800` | Seconds; install template: `7200` |
| `track_running_jobs` | `true` | Set `false` at very high throughput (keeps heartbeats) |
| `uniq_enabled` | `true` | Master switch for `uniq true` workers |

### Store modes

| | `store: :redis` | `store: :mysql` |
|---|---|---|
| Batch ledger | Redis | Redis (always) |
| Failure log | Redis | MySQL `kafka_batch_failures` |
| Migrations | None | `rails db:migrate` |

### Priority config paths

Also read from environment:

- `KAFKA_BATCH_PRIORITY_CONFIG` — single YAML path
- `KAFKA_BATCH_PRIORITY_CONFIGS` — comma-separated paths

---

## Karafka routing & deployment

`KafkaBatch.draw_routes` registers consumer groups:

| Group suffix | Topics | Consumer |
|---|---|---|
| `-control` | events, callbacks, retry tiers | Event, Callback, Retry |
| `-dispatch-<lane>` | fair ingest | `Fairness::Dispatcher` |
| `-jobs-fair-<lane>` | fair ready | `JobConsumer` |
| `-<priority-suffix>` | from priority YAML | `PriorityJobConsumer` (ranked) |
| `-jobs` | plain worker topics | `JobConsumer` |

Karafka runs only the groups you include. In production, use **one Deployment per role**:

```bash
# Examples (replace kafka-batch with your config.consumer_group)
bundle exec karafka server --include-consumer-groups kafka-batch-control
bundle exec karafka server --include-consumer-groups kafka-batch-jobs
bundle exec karafka server --include-consumer-groups kafka-batch-jobs-fast   # from priority YAML suffix
bundle exec karafka server --include-consumer-groups kafka-batch-dispatch-time,kafka-batch-jobs-fair-time
```

### `KB_ROLE` wrapper

Map roles to consumer groups in `bin/kb-server` (see install docs). Typical production split:

| `KB_ROLE` | Consumer groups | Poller | Scale |
|---|---|---|---|
| `control` | `-control` | off | few pods |
| `scheduler` | `-control` (light) | **on** | 2–3 fixed pods |
| `jobs` | `-jobs` | off | autoscale on lag |
| `fair-time` | `-dispatch-time`, `-jobs-fair-time` | off | autoscale on lag |
| `jobs-fast` | `-jobs-fast` (your YAML suffix) | off | autoscale on lag |
| `all` | everything | on | dev only |

```ruby
# initializer — wire poller to role
roles = ENV.fetch("KB_ROLE", "all").split(",").map(&:strip)
config.schedule_poller_enabled =
  case ENV["KB_SCHEDULE_POLLER"]
  when "true"  then true
  when "false" then false
  else (roles & %w[all scheduler]).any?
  end
```

---

## Priority queues

Sidekiq.yml-style **ordered topics per consumer group**, loaded from YAML at boot.

### Example YAML

`config/kafka_batch/priority/jobs-fast.yml`:

```yaml
consumer_group_suffix: jobs-fast    # → {consumer_group}-jobs-fast
mode: weighted                      # weighted | strict
weighted_interleave: 4              # optional — 1-in-N lower-rank jobs while higher lag
topics:                             # highest priority first
  - kafka_batch.jobs.p0
  - kafka_batch.jobs.p1
  - kafka_batch.jobs.p2
```

`config/kafka_batch/priority/jobs-slow.yml`:

```yaml
consumer_group_suffix: jobs-slow
mode: strict
topics:
  - kafka_batch.jobs.slow_p0
  - kafka_batch.jobs.slow_p1
```

Register paths:

```ruby
config.priority_config_paths = [
  Rails.root.join("config/kafka_batch/priority/jobs-fast.yml").to_s,
  Rails.root.join("config/kafka_batch/priority/jobs-slow.yml").to_s
]
```

Topic names get `topic_prefix` applied automatically.

### Enroll workers

```ruby
class CriticalWorker
  include KafkaBatch::Worker
  kafka_topic "kafka_batch.jobs.p0"   # rank 0 — no gate
end

class NormalWorker
  include KafkaBatch::Worker
  kafka_topic "kafka_batch.jobs.p1"   # yields while p0 has lag
end
```

### Modes

| Mode | Behaviour while higher topics have lag |
|---|---|
| **`strict`** | Lower ranks do **not** start new work until all higher topics are empty |
| **`weighted`** | Lower ranks interleave — default 1-in-4 messages proceed (`priority_weighted_interleave`) |

Lag checks use the Kafka Admin API, rate-limited per `priority_lag_check_interval` (default 2s). Unreachable cluster → **fail open** (process anyway).

### Boot rules

- Each topic → **one** consumer group (validated at boot)
- `kafka_batch.jobs` (default flat queue) **cannot** appear in priority YAML
- `fairness true` on a worker bypasses priority topics

Run the priority group on dedicated pods:

```bash
bundle exec karafka server --include-consumer-groups myapp.kafka-batch-jobs-fast
```

---

## Multi-tenant fairness

Opt in per worker with `fairness true`. Two independent lanes run simultaneously:

| Lane | `fairness_type` | Shares |
|---|---|---|
| Time | `:time` (default) | Weighted **wall-clock** execution time |
| Throughput | `:throughput` | Weighted **job count** |

```
push → fair_*_ingest (keyed by tenant_id)
     → Dispatcher (-dispatch-<lane>)
     → Redis WFQ Scheduler
     → Forwarder → fair_*_ready
     → JobConsumer (-jobs-fair-<lane>)
```

Fair jobs re-enter **ready** on retry (skip scheduler). Everything downstream (events, callbacks, DLT) is identical to plain workers.

### Critical settings

```ruby
config.fairness_global_concurrency      = 1000   # in-flight window per lane
config.fairness_weighted_concurrency    = true   # default; false = equal cap per tenant
config.fairness_lease_ttl               = 7200   # must exceed longest job
config.fairness_min_ingest_partitions   = 64     # boot check
```

Tune live weights at `/kafka_batch/weights` (stored in Redis per fairness lane, regardless of `config.store`).

### Enqueue

```ruby
KafkaBatch::Batch.create(tenant_id: "acme") do |b|
  b.push(FairWorker, { "id" => 1 })   # tenant_id inherited from batch
end
```

---

## Delayed jobs

`perform_in` / `perform_at` equivalent:

```ruby
KafkaBatch::Batch.enqueue_in(MyWorker, payload, 30.minutes)
KafkaBatch::Batch.enqueue_at(MyWorker, payload, run_at)
```

- Payload stored in `kafka_batch.scheduled` topic
- Pointer index in Redis ZSET or MySQL (`schedule_store`)
- `SchedulePoller` on scheduler pods claims due jobs and re-produces to the real worker topic

Set `scheduled` topic retention ≥ `max_schedule_horizon` (default 7 days).

---

## Unique jobs & expiration

### Uniqueness (`uniq true`)

One in-flight job per `worker_class + payload` (XXHash64 digest, 8-byte Redis key):

```ruby
class ImportWorker
  include KafkaBatch::Worker
  uniq true
end
```

Duplicate enqueue → `nil` (default) or `DuplicateJobError` (`config.uniq_on_duplicate = :raise`).

### Expiration (`valid_till`)

```ruby
KafkaBatch::Batch.enqueue(MyWorker, payload, valid_till: 1.hour.from_now)
```

Consumer sends expired jobs to DLT without running `#perform`.

---

## Retries & dead letter

| Tier | Default delay | Topic |
|---|---|---|
| short | 30s | `kafka_batch.jobs.retry.short` |
| medium | 7m | `kafka_batch.jobs.retry.medium` |
| large | 20m | `kafka_batch.jobs.retry.large` |

Nth retry walks the progression (override with `retry_tier` on the worker). `RetryConsumer` pauses only the retry partition — job partitions stay unblocked.

Exhausted jobs → dead letter topic + failed completion event.

Subscribe your own consumer to `config.dead_letter_topic` for alerting.

---

## Scaling & partitions

**Rule:** `partitions ≥ peak_pods × concurrency` on every execution topic you intend to scale.

Default partition targets (`KafkaBatch::Topics::DEFAULT_PARTITIONS`):

| Category | Partitions | Examples |
|---|---|---|
| Execution | 768 | jobs, priority, fair ready |
| Events | 48 | completion events |
| Fair ingest | 64 | per lane |
| Scheduled | 48 | delayed payloads |
| Retry | 12 | per tier |

Autoscale execution Deployments on **consumer lag** (KEDA / HPA). Partitions are fixed — pods are elastic.

### High-volume checklist

1. Size execution topics for peak pods × concurrency
2. Tune tenant weights at `/kafka_batch/weights` (weighted concurrency is on by default)
3. `fairness_lease_ttl` > longest job runtime
4. `schedule_poller_enabled` only on 2–3 scheduler pods
5. `track_running_jobs = false` at 50M+ jobs/day if `/live` per-job detail isn't needed
6. Split `KB_ROLE=control` from execution swarms
7. Cap batch size or shard mega-batches in application code

---

## Web UI & reconciler

Mount `KafkaBatch::Web` at `/kafka_batch`:

| Page | Shows |
|---|---|
| `/` | Batch list, status, cancel |
| `/lag` | Per-group/topic lag, pause/resume |
| `/live` | Running jobs & consumer heartbeats |
| `/weights` | Tenant fairness weights |
| `/failures` | Failure log |

**Reconciler** (inside `EventConsumer`, periodic): recovers stuck `running` batches and re-dispatches lost callbacks. Manual: `rake kafka_batch:reconcile`.

---

## Instrumentation

Events publish via `ActiveSupport::Notifications` when Rails/AS is loaded:

| Event | When |
|---|---|
| `job.processed.kafka_batch` | Success |
| `job.retried.kafka_batch` | Scheduled retry |
| `job.failed.kafka_batch` | Exhausted |
| `job.cancelled.kafka_batch` | Batch cancelled |
| `job.uniq_skipped.kafka_batch` | Duplicate uniq enqueue |
| `job.expired.kafka_batch` | Past `valid_till` |
| `job.emit_retried.kafka_batch` | Completion-event produce retry |
| `scheduled.enqueued.kafka_batch` | Delayed job indexed |
| `scheduled.enqueued_bulk.kafka_batch` | Bulk delayed jobs indexed |
| `scheduled.dispatched.kafka_batch` | Delayed job fired |
| `batch.created.kafka_batch` | Batch persisted |
| `batch.sealed.kafka_batch` | Block-form batch sealed |
| `batch.completed.kafka_batch` | Terminal state |
| `callback.invoked.kafka_batch` | Callback ran |
| `callback.failed.kafka_batch` | Callback error |
| `dlt.published.kafka_batch` | Dead letter (all paths via `KafkaBatch::Dlt`) |
| `consumer.priority_yielded.kafka_batch` | Priority gate paused |
| `reconciler.ran.kafka_batch` | Sweep finished |

### Subscribe today (DIY metrics)

```ruby
# config/initializers/kafka_batch_metrics.rb
ActiveSupport::Notifications.subscribe("job.processed.kafka_batch") do |*args|
  event = ActiveSupport::Notifications::Event.new(*args)
  StatsD.increment("kafka_batch.job.processed", tags: ["worker:#{event.payload[:worker_class]}"])
  StatsD.timing("kafka_batch.job.duration", event.duration * 1000, tags: ["worker:#{event.payload[:worker_class]}"]) if event.payload[:duration]
end

ActiveSupport::Notifications.subscribe(/\.kafka_batch\z/) do |name, *rest|
  event = ActiveSupport::Notifications::Event.new(name, *rest)
  StatsD.increment("kafka_batch.events", tags: ["event:#{name.sub('.kafka_batch', '')}"])
end
```

> **Built-in Prometheus/StatsD export is planned** — see plan below. Until then, AS::Notifications is the integration point.

---

## Migrating from Sidekiq Pro

| Sidekiq Pro | kafka-batch |
|---|---|
| `Sidekiq::Batch.new` | `KafkaBatch::Batch.create` |
| `batch.jobs { perform_async }` | `b.push(Worker, payload)` |
| `batch.on(:success, …)` | `on_success: "CallbackClass"` |
| `batch.on(:complete, …)` | `on_complete: "CallbackClass"` |
| `status.bid` | `batch["batch_id"]` |
| `status.total` | `batch["total_jobs"]` |
| `status.failures` | `batch["failed_count"]` |

Workers use `include KafkaBatch::Worker` and run under Karafka instead of Sidekiq threads. Callback signatures are structurally the same — keep them idempotent.

---

## Reference

### Consumer groups (default prefix)

| Suffix | Purpose |
|---|---|
| `{cg}-control` | Events, callbacks, retries |
| `{cg}-dispatch-time` | Fair time-lane ingest |
| `{cg}-jobs-fair-time` | Fair time-lane execution |
| `{cg}-dispatch-throughput` | Fair throughput-lane ingest |
| `{cg}-jobs-fair-throughput` | Fair throughput-lane execution |
| `{cg}-<priority-suffix>` | Priority YAML group |
| `{cg}-jobs` | Plain workers |

### Key Redis keys

| Key | Purpose |
|---|---|
| `kafka_batch:b:{id}` | Batch hash (counters, status, callbacks) |
| `kafka_batch:b:bitmap:{id}` | Completion dedup (~1 bit / `batch_seq`) |
| `kafka_batch:b:seq:{id}` | Monotonic `batch_seq` allocator |
| `kafka_batch:uniq:{digest}` | Uniqueness lock (8-byte binary suffix) |
| `kafka_batch:index:running` | Reconciler — stuck batches |
| `kafka_batch:index:done` | Reconciler — lost callbacks |
| `kafka_batch:sched:pending` | Delayed-job pointers (ZSET) |
| `kafka_batch:fair_time:*` | Time-lane WFQ state |
| `kafka_batch:fair_throughput:*` | Throughput-lane WFQ state |

### Rake tasks

```bash
bundle exec rake kafka_batch:create_topics
bundle exec rake kafka_batch:topics          # dry-run
bundle exec rake kafka_batch:reconcile
```

### Reliability summary

| Guarantee | Status |
|---|---|
| Job execution | At-least-once (idempotent workers required) |
| Completion counting | Exactly-once per `batch_seq` (bitmap dedup) |
| Callback delivery | At-least-once (idempotent callbacks required) |
| Batch create atomicity | Redis record before produce; rollback on partial produce failure |

---

## Contributing

Issues and PRs welcome at [github.com/y-shashank/kafka-batch](https://github.com/y-shashank/kafka-batch).
