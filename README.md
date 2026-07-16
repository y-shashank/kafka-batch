# kafka-batch

[![CI](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/y-shashank/kafka-batch/badges/coverage.json)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)

**Sidekiq Pro Batches on Kafka.** Same `on_success` / `on_complete` callback model, per-job retries, and batch completion counting — with Kafka as the durable job transport and Redis for coordination.

Built on [Karafka](https://karafka.io) (WaterDrop + consumers). **Go runtime:** [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go).

---

## Table of contents

- [Non-negotiable pitfalls](#non-negotiable-pitfalls)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Workers & jobs](#workers--jobs)
- [Ruby + Go handlers (mixed runtime)](#ruby--go-handlers-mixed-runtime)
- [Batches & callbacks](#batches--callbacks)
- [Configuration](#configuration)
- [Three-tier architecture](#three-tier-architecture)
- [Deployment](#deployment)
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
- [TCO vs Sidekiq at scale](#tco-vs-sidekiq-at-scale)
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

Kafka cannot shrink partitions. Size execution topics for **peak pods × effective in-flight** (Karafka `concurrency` × `super_fetch_concurrency`, default often `N × 1`). Undersized topics leave pods idle at peak; oversized topics are harmless.

Ruby job consumers are **SuperFetch always-on** on every execution group (plain, priority, fair-ready; scheduled jobs hit those topics after the poller). Claim in Redis → Kafka mark → thread-pool `#perform`. **EventConsumer** (and other control consumers) stay sync. Crash recovery: Ruby control runs the same workset **reclaim** loop as Go `kbatch daemon` (either control plane is enough on shared Redis). Keep **one topic = one runtime** (Ruby or Go), never both.

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

### 12. Metrics need a client you supply

A built-in StatsD/Datadog/proc bridge exists (`config.metrics_enabled`), but it emits through a client **you** provide — the gem has no metrics dependency and ships no dashboard. See [Metrics export](#metrics-export-statsd--datadog--prometheus).

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
# optional: --store mysql           (failure log / pauses in MySQL; ledger still Redis)
# optional: --schedule-store mysql  (delayed-job index in MySQL)
# optional: --audit                 (copy the Web UI audit-log migration)
```

The generator copies only the migrations the chosen stores/features need, then run `rails db:migrate`. See [Web UI audit log](#web-ui-audit-log) for `--audit`.

Edit `config/initializers/kafka_batch.rb` — at minimum:

```ruby
KafkaBatch.configure do |config|
  config.brokers   = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")
  config.redis_url = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  config.topic_prefix = ENV["KAFKA_PREFIX"].to_s.strip  # optional namespace

  # Required when enqueueing Go handlers — see [Ruby + Go handlers](#ruby--go-handlers-mixed-runtime)
  # config.handler_manifest_path = Rails.root.join("config/kafka_batch_handlers.yml").to_s
end
```

### 3. Create Kafka topics

```bash
bundle exec rake kafka_batch:create_topics
# or: KAFKA_BROKERS=localhost:9092 ./bin/create_kafka_topics.sh
# dry-run: bundle exec rake kafka_batch:topics
```

Topics are derived from registered workers, priority YAML files, and fairness settings. The task is **idempotent** — existing topics are skipped, never altered (Kafka cannot shrink partitions or change replication factor in place). It prints a `created / skipped / failed` summary and exits non-zero on any failure.

Two env vars tune what gets created (both also apply to the `:topics` dry-run):

| Env var | Default | Effect |
|---------|---------|--------|
| `REPLICATION_FACTOR` | `config.topics_replication_factor` (**3**) | Replication factor for **every** topic. Set to `1` on a single-broker cluster (local/dev/CI) — the default `3` fails there with `NOT_ENOUGH_REPLICAS`. |
| `PARTITIONS` | per-category [defaults](#scaling--partitions) | Forces **every** topic to exactly N partitions, overriding the per-category sizing. Omit to keep the tuned per-category defaults. |

```bash
# Local single-broker Kafka — replication factor MUST be 1:
REPLICATION_FACTOR=1 bundle exec rake kafka_batch:create_topics

# Force every topic to 12 partitions (e.g. a small staging cluster):
PARTITIONS=12 bundle exec rake kafka_batch:create_topics

# Both together — 24 partitions, single replica:
PARTITIONS=24 REPLICATION_FACTOR=1 bundle exec rake kafka_batch:create_topics

# Preview the exact plan (name / partitions / rf) without creating anything:
PARTITIONS=24 REPLICATION_FACTOR=1 bundle exec rake kafka_batch:topics
```

> Prefer setting per-topic partition counts by sizing your workers/priority topics (see [Scaling & partitions](#scaling--partitions)); reach for `PARTITIONS=N` only when you want a uniform override across the whole set (e.g. dev/staging).

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

  job_type "orders.process"   # stable wire ID (defaults from class name)
  kafka_topic "orders.process"

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
bundle exec karafka server   # dev: all tiers — see [Deployment](#deployment)
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
          → Execution tier (Karafka JobConsumer)
               ├─ success  → events topic → EventConsumer (bitmap dedup) → callbacks topic
               ├─ retry    → retry.{short|medium|large} → RetryConsumer → worker topic
               └─ exhausted → dead_letter + failed event

Control tier (Karafka control): fair ingest→forward, schedule poller, events/retry/callbacks consumers.

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

**Fair workers** (`fairness_type :time` or `:throughput`) route through ingest → scheduler → ready instead of their own topic. **Priority workers** use topics from a priority YAML group. **Plain workers** use `kafka_topic` or the shared default `kafka_batch.jobs`.

---

## Workers & jobs

```ruby
class MyWorker
  include KafkaBatch::Worker

  job_type "orders.process"    # stable cross-language ID (optional — see below)
  kafka_topic "orders.process" # optional — defaults to config.jobs_topic;
                               # topic_prefix applied automatically (see below)
  max_retries 5
  retry_tier :large            # pin all retries to one tier (optional)
  fairness_type :time          # :time or :throughput — opts into WFQ lane (optional)
  uniq true                    # dedup enqueue by payload (optional)

  retries_exhausted do |job, error|
    # job: job_id, batch_id, payload, attempt, job_type, …
    Alert.notify(job["job_id"], error.message)
  end

  def perform(payload)
    # must be idempotent
  end
end
```

### `job_type` and the handler registry

Every produced job carries a **`job_type`** string (alongside legacy `worker_class`). `JobConsumer` resolves handlers through `KafkaBatch::HandlerRegistry`:

1. Lookup by `job_type` when registered
2. Fall back to `worker_class` (`const_get` + auto-register)
3. Unknown handler → DLT (poison-pill safe)

**Default `job_type`** is derived from the class name (`ProcessOrderWorker` → `"process_order"`). Override explicitly when the wire ID must match a handler manifest entry or a Go `kbatch.Register` name:

```ruby
job_type "campaign.fast_p0"
```

Each Kafka **execution** topic belongs to exactly one runtime (`ruby` or `go`). See **[Ruby + Go handlers (mixed runtime)](#ruby--go-handlers-mixed-runtime)** for manifest setup, enqueue examples, and deployment.

**Wire envelope** (excerpt):

```json
{
  "job_type": "orders.process",
  "worker_class": "ProcessOrderWorker",
  "job_id": "...",
  "batch_id": "...",
  "payload": {},
  "attempt": 0
}
```

Legacy messages with only `worker_class` still work.

---

## Ruby + Go handlers (mixed runtime)

The **client tier** (`KafkaBatch::Batch`) can enqueue **both** Ruby and Go jobs from one Rails app. Each job carries a stable `job_type`; routing picks the Kafka topic; **execution happens on the matching runtime**.

| Runtime | Declared in | Executed by |
|---|---|---|
| `ruby` | Ruby `Worker` class (+ optional manifest entry) | Karafka `JobConsumer` (this gem) |
| `go` | Handler manifest YAML only | [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go) `kbatch worker` |

Both runtimes share the same **Kafka topics**, **Redis** fairness state, **batch** completion counting, and **control plane** (events, retry, callbacks on Ruby Karafka `-control` groups).

### 1. Create the handler manifest

`config/kafka_batch_handlers.yml`:

```yaml
handlers:
  # Plain Go job — client produces directly to this topic
  segment.export:
    runtime: go
    topic: segment.exports
    max_retries: 25

  # Plain Ruby job — worker_class must match a loaded Worker
  orders.process:
    runtime: ruby
    worker_class: Orders::ProcessWorker
    topic: orders.process

  # Fair Go job — client produces to shared ingest; forwarder routes to .go ready
  reports.rebuild:
    runtime: go
    fairness_type: time
    max_retries: 10

  # Fair Ruby job — same ingest lane; forwarder routes to .ruby ready
  campaigns.dispatch:
    runtime: ruby
    worker_class: Campaigns::DispatchWorker
    fairness_type: time
```

**Rules:**

- The YAML key (`segment.export`, `orders.process`, …) is the wire **`job_type`** — it must match `job_type` on the Ruby Worker and `kbatch.Register("…")` in kafka-batch-go.
- Each Kafka **execution** topic belongs to **one** runtime only.
- Fair handlers on both runtimes share the **ingest** topic per lane; the control-tier forwarder splits to `fair_*_ready.ruby` vs `fair_*_ready.go` (enabled by default).

### 2. Configure every process that enqueues

```ruby
# config/initializers/kafka_batch.rb
KafkaBatch.configure do |config|
  config.brokers   = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")
  config.redis_url = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")

  config.handler_manifest_path =
    Rails.root.join("config/kafka_batch_handlers.yml").to_s
  # Or: ENV["KAFKA_BATCH_HANDLER_MANIFEST"]=/path/to/handlers.yml
end
```

Karafka processes load the same manifest automatically when `draw_routes` runs.

### 3. Define Ruby workers (match manifest `job_type`)

```ruby
class Orders::ProcessWorker
  include KafkaBatch::Worker

  job_type "orders.process"       # must match manifest
  kafka_topic "orders.process"

  def perform(payload)
    Order.find(payload["order_id"]).process!
  end
end

class Campaigns::DispatchWorker
  include KafkaBatch::Worker

  job_type "campaigns.dispatch"
  fairness_type :time              # must match manifest

  def perform(payload)
    Campaign.dispatch!(payload)
  end
end
```

Go handlers are **not** Ruby classes. Register them in kafka-batch-go with the same `job_type` string (see [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go)).

### 4. Enqueue from the client

**Standalone jobs**

```ruby
# Ruby — by Worker class
KafkaBatch::Batch.enqueue(Orders::ProcessWorker, "order_id" => 42)

# Go — by manifest job_type
KafkaBatch::Batch.enqueue_job("segment.export", "segment_id" => 99)

# Delayed Go job
KafkaBatch::Batch.enqueue_job_at(1.hour.from_now, "segment.export", "segment_id" => 99)
KafkaBatch::Batch.enqueue_job_in(30.minutes, "segment.export", "segment_id" => 99)

# Fair job (Ruby or Go) — tenant_id drives WFQ partitioning
KafkaBatch::Batch.enqueue_job(
  "reports.rebuild",
  { "report_id" => 1 },
  tenant_id: "acme"
)
```

**Mixed batch** — Ruby + Go jobs in one batch; `on_success` / `on_complete` fire when **all** jobs finish:

```ruby
KafkaBatch::Batch.create(
  description: "nightly export",
  on_success:  "NightlyCallback",
  on_complete: "NightlyCallback"
) do |b|
  b.push(Orders::ProcessWorker, "order_id" => 1)    # Ruby
  b.push_job("segment.export", "segment_id" => 42)  # Go
  b.push_job("campaigns.dispatch", "campaign_id" => 7, tenant_id: "acme")
end
```

### 5. Where each job lands

| Handler | `fairness_type` | Client produces to | Executed by |
|---|---|---|---|
| Plain Ruby | — | worker `kafka_topic` | Ruby Karafka `-jobs` |
| Plain Go | — | manifest `topic` | `kbatch worker` |
| Fair Ruby | `:time` / `:throughput` | `fair_*_ingest` → `fair_*_ready.ruby` | Ruby Karafka `-jobs-fair-*` |
| Fair Go | `:time` / `:throughput` | `fair_*_ingest` → `fair_*_ready.go` | `kbatch worker` |

### 6. Provision topics & run both runtimes

```bash
bundle exec rake kafka_batch:create_topics   # Ruby workers + fairness lanes
# Also create Go handler topics (e.g. segment.exports) — not auto-discovered from the manifest yet
```

| Deployment | Gem / repo | What runs |
|---|---|---|
| API (tier 1) | kafka-batch | `daemon_mode: true` — enqueue only, no Karafka |
| Control (tier 2) | kafka-batch | Karafka `-control`, `-dispatch-*` (fair forwarder, events, retry, callbacks) |
| Ruby execution (tier 3) | kafka-batch | Karafka `-jobs`, `-jobs-fair-*` |
| Go execution (tier 3) | kafka-batch-go | `kbatch worker` on Go topics + `fair_*_ready.go` |

**Dev — Ruby only:** `KB_ROLE=all bundle exec karafka server` + `rails server` is enough.

**Dev / prod — mixed:** run Ruby Karafka **and** `kbatch worker` alongside the API. See [Deployment](#deployment) for consumer-group commands.

---

## Three-tier architecture

KafkaBatch splits into **three independent tiers**. This gem implements all three in **Ruby** (Karafka). The companion repo [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go) implements the same tiers in Go. Pick one language per tier; tiers talk only through **Kafka + Redis**.

### The three parts (this gem)

| Tier | Responsibility | Ruby implementation |
|------|----------------|---------------------|
| **1 — Client** | Produce jobs & batches (`push`, `enqueue`, `perform_in`, cancel) | `KafkaBatch::Batch` |
| **2 — Control** | Fairness ingest→forward, events, retry, callbacks, schedule poller | Karafka control consumer groups |
| **3 — Execution** | Consume ruby job topics, run `#perform`, publish events | Karafka `JobConsumer` |

**Client (tier 1):** Reads the handler manifest. Enqueues `runtime: ruby` or `runtime: go` jobs — routing picks the correct Kafka topic (plain, priority, or fair **ingest**).

**Control (tier 2):** Does **not** run `#perform`. Fair WFQ (ingest → `.go` / `.ruby` ready), batch event counting, tiered retries, batch callbacks, delayed-job poller, reconciler.

**Execution (tier 3):** Ruby handlers run in Karafka `JobConsumer`. Go handlers run in [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go) `kbatch worker`. See [Ruby + Go handlers](#ruby--go-handlers-mixed-runtime).

### Data flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Tier 1 — Client (KafkaBatch::Batch)  — enqueues Ruby + Go via job_type     │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ produce
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Kafka + Redis                                                              │
└───────────────┬───────────────────────────────┬───────────────────────────┘
                │                               │
                ▼                               ▼
┌───────────────────────────────┐   ┌──────────────────────┐  ┌─────────────────┐
│  Tier 2 — Karafka control     │   │ Tier 3 — Ruby exec   │  │ Tier 3 — Go exec│
│  • fair ingest → WFQ forward  │   │ JobConsumer #perform │  │ kbatch worker   │
│  • events / retry / callbacks │   │ plain + fair .ruby   │  │ plain + fair .go│
│  • schedule poller            │   └──────────────────────┘  └─────────────────┘
└───────────────────────────────┘
```

For **Go-only control** (optional), kafka-batch-go also provides `kbatch daemon` for tier 2. Most mixed stacks use Ruby Karafka for control and share events/retry/callback topics.

**Run workset reclaim on at least one control plane** (Ruby Karafka control / `Workset::ReclaimScheduler`, or Go `kbatch daemon`) against shared Redis — orphans after a crash are re-produced from `kafka_batch:work:*`. Both may run together (NX lock).

### Topic rules

- **One execution topic = one runtime.** Never register Ruby and Go handlers on the same plain or priority topic.
- **Fairness:** one **ingest** topic per lane (shared by both runtimes); separate **ready** topics per runtime (`.ruby` / `.go`). The forwarder routes by manifest `runtime`.
- **Consumer groups:** `-jobs-fair-*` (Ruby), Go worker groups in kafka-batch-go.

Handler manifest format and enqueue examples: **[Ruby + Go handlers](#ruby--go-handlers-mixed-runtime)**.

See **[Deployment](#deployment)** for how to run all three tiers together, standalone on one host, or split into separate processes.

---

### Standalone jobs (no batch)

```ruby
# Ruby handler
KafkaBatch::Batch.enqueue(MyWorker, { "id" => 1 })
KafkaBatch::Batch.enqueue_at(MyWorker, { "id" => 1 }, 1.hour.from_now)
KafkaBatch::Batch.enqueue_in(MyWorker, { "id" => 1 }, 30.minutes)

# Go handler (manifest job_type)
KafkaBatch::Batch.enqueue_job("segment.export", "segment_id" => 42)
KafkaBatch::Batch.enqueue_job_at(1.hour.from_now, "segment.export", "segment_id" => 42)
```

### Shared default queue

Workers without `kafka_topic` share `config.jobs_topic` (default `kafka_batch.jobs`). Dispatch is safe — each message embeds `worker_class`. Use a dedicated `kafka_topic` when you need independent scaling, ordering, or lag isolation.

---

## Batches & callbacks

Callbacks are **jobs on a queue** (Sidekiq-style), not in-process Ruby methods. When a batch finalizes, kafka-batch **enqueues a normal job** to the topic you choose — Ruby `JobConsumer` or Go `kbatch worker` runs it.

### Job callbacks (recommended)

Register callback handlers in the manifest (same `job_type` dotted naming as work jobs):

```yaml
# config/kafka_batch_handlers.yml
handlers:
  segment.export:
    runtime: go
    topic: segment.exports

  segment.export.on_success:
    runtime: go
    topic: segment.exports.callbacks

  segment.export.on_complete:
    runtime: go
    topic: segment.exports.callbacks

  import.on_complete:
    runtime: ruby
    worker_class: Import::OnCompleteWorker
    topic: kafka_batch.callbacks.ruby
```

```ruby
KafkaBatch::Batch.create(
  description: "export run",
  on_success:  KafkaBatch::Callback.job("segment.export.on_success", topic: "segment.exports.callbacks"),
  on_complete: KafkaBatch::Callback.job("segment.export.on_complete", topic: "segment.exports.callbacks"),
  meta:         { "source" => "api" },                    # batch metadata (dashboard, APIs — not sent to callbacks)
  callback_args: { "run_id" => "42", "channel" => "#ops" } # only passed to on_success / on_complete handlers
) do |b|
  b.push_job("segment.export", "segment_id" => 42)          # Go work job
  b.push(ImportRowWorker, "row_id" => 1)                   # Ruby work job (optional)
end
```

**`meta` vs `callback_args`**

| Option | Stored on batch | In callback payload |
|--------|-----------------|---------------------|
| `meta` | Yes | No — use for batch labels, audit, `Batch.find` |
| `callback_args` | Yes | Yes — custom args for your callback job / legacy class only |

Work jobs never receive `callback_args`; only callback handlers see them in `perform(payload)` / `ctx.Payload`.

Go worker registers the callback handler the same way as a work job:

```go
kbatch.Register("segment.export.on_success", func(ctx *kbatch.Context) error {
    // ctx.Payload has batch_id, outcome, total_jobs, completed_count, failed_count, callback_args, …
    return notifySlack(ctx.Payload)
})
```

Ruby callback worker:

```ruby
class Import::OnCompleteWorker
  include KafkaBatch::Worker
  job_type "import.on_complete"
  kafka_topic "kafka_batch.callbacks.ruby"

  def perform(payload)
    channel = payload.dig("callback_args", "channel") || "#imports"
    notify_slack(channel, "#{payload['failed_count']} failed in batch #{payload['batch_id']}")
  end
end
```

Or pass a Ruby worker directly:

```ruby
on_complete: KafkaBatch::Callback.worker(Import::OnCompleteWorker)
```

| Field | Meaning |
|---|---|
| `job_type` | Stable handler id — matches manifest + `kbatch.Register` |
| `topic` | **Your** execution topic (Go queue or Ruby queue); omit when manifest `topic` is enough |
| Payload | Batch summary: `batch_id`, `outcome`, `total_jobs`, `completed_count`, `failed_count`, `callback_args`, `callback_kind` (`on_success` / `on_complete`) |
| `job_id` | Deterministic `#{batch_id}:on_success` / `:on_complete` (idempotent redelivery) |

**Rules**

- Pick a **Go topic** for Go callbacks → `kbatch worker` executes.
- Pick a **Ruby topic** for Ruby callbacks → Karafka `JobConsumer` executes.
- Fair handlers need an **explicit callback topic** (callbacks never use fair ingest).
- Legacy Ruby class strings still work (below) but job callbacks are preferred.

### Legacy Ruby class callbacks

```ruby
KafkaBatch::Batch.create(
  on_success:  "ImportCallbacks",
  on_complete: "ImportCallbacks",
  callback_args: { "slack_channel" => "#imports" }
) do |b|
  b.push_many(ProcessUserWorker, users.map { |u| { "user_id" => u.id } })
end
```

```ruby
class ImportCallbacks
  def on_success(batch)
    channel = batch.dig("callback_args", "slack_channel") || "#imports"
    notify_slack(channel, "All #{batch['total_jobs']} succeeded")
  end

  def on_complete(batch)
    notify_slack("#{batch['failed_count']} failed")
  end
end
```

Legacy callbacks still route through `kafka_batch.callbacks` and `CallbackConsumer` (Ruby control tier).

| Method | Purpose |
|---|---|
| `Batch.create` | New batch; block form auto-seals when block exits (`meta`, `callback_args`, callbacks) |
| `Batch.open(id)` | Push more jobs into a running batch (jobs-adding-jobs) |
| `Batch.find(id)` | Fetch batch hash from Redis |
| `Batch.cancel(id)` | Mark cancelled; consumers skip within cache TTL |
| `Batch.enqueue` | Single job, optional `batch_id:` for open batches |
| `KafkaBatch::Callback.job` | Build a job callback (`job_type` + optional `topic`) |
| `KafkaBatch::Callback.worker` | Build a job callback from a Ruby `Worker` class |

**Sidekiq-style completion:** the first job finish (success or fail-then-retry) **touches** the batch. `on_complete` fires when every job has been touched (retries may still be running). `on_success` fires when every job has succeeded. Terminal fail (DLT/exhaust) increments `failed_count`; status becomes `complete` when `completed + failed >= total`.

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
| `consumption_control_refresh_interval` | `30` | How often consumers re-read pause state from Redis/MySQL |
| `priority_config_paths` | `[]` | Paths to priority YAML files |
| `handler_manifest_path` | `""` | YAML for Go handlers (and optional Ruby routing) — **required for Go jobs** |
| `fairness_weighted_concurrency` | `true` | Set `false` for equal in-flight cap per tenant (weights → order only) |
| `fairness_global_concurrency` | `50` | Per-lane in-flight window (install template: `1000`) |
| `fairness_lease_ttl` | `1800` | Seconds; install template: `7200` |
| `track_running_jobs` | `true` | Set `false` at very high throughput (keeps heartbeats) |
| `uniq_enabled` | `true` | Master switch for `uniq true` workers |
| `super_fetch_concurrency` | `1` | Concurrent `#perform`s per consumer process (thread pool). Default `1` (MRI GVL); see [SuperFetch concurrency (Ruby)](#superfetch-concurrency-ruby) |
| `super_fetch_claim_window` | `0` → `2×` SF | Max Claimed∨Queued∨Performing; Claim+ack gated here so the listener is not blocked on `#perform` |
| `redis_pool_size` | `≥16` (auto) | Default scales with SF + claim window + Karafka floor; raise before raising SF |
| `fairness_dynamic_tenant_partitions` | `true` | Exclusive ingest partitions for hot tenants (static `fairness_tenant_partitions` still wins). Set `false` to use murmur2 key-hash only. |
| `super_fetch_lease_ttl` | `120` | Redis workset job key TTL (renewed during long jobs) |
| `super_fetch_orphan_grace` | `40` | Seconds before a dead owner's job is stealable / reclaimable |
| `super_fetch_reclaim_enabled` | `true` | Control-plane orphan reclaim loop (parity with Go daemon) |
| `super_fetch_reclaim_interval` | `30` | Seconds between reclaim sweeps |
| `super_fetch_reclaim_limit` | `100` | Max orphans processed per sweep |

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

## Deployment

KafkaBatch registers **every** consumer group in `draw_routes`, but each process runs only the groups you include. Tiers talk through **Kafka + Redis** only — no sockets or shared memory between pods.

Throughout this section, **`CG`** means `KafkaBatch.config.consumer_group` (default `kafka-batch`; with `topic_prefix = "myapp"` → `myapp.kafka-batch`).

### Consumer groups

| Group | Tier | Topics | Consumer |
|---|---|---|---|
| `{CG}-control` | 2 — Control | events, callbacks, retry tiers | Event, Callback, Retry |
| `{CG}-dispatch-<lane>` | 2 — Control | fair ingest (`time`, `throughput`) | `Fairness::Dispatcher` |
| `{CG}-jobs-fair-<lane>` | 3 — Execution | fair ready (ruby) | `JobConsumer` |
| `{CG}-<priority-suffix>` | 3 — Execution | from priority YAML | `PriorityJobConsumer` |
| `{CG}-jobs` | 3 — Execution | plain worker topics | `JobConsumer` |

List every group your install registers:

```ruby
KafkaBatch.consumer_groups
# => ["myapp.kafka-batch-control", "myapp.kafka-batch-dispatch-time", ...]
```

### Choose a layout

| Layout | When | Processes |
|---|---|---|
| **All tiers together** | Local dev, smoke tests | Rails (client) + Karafka (control + execution) |
| **Standalone** | Small production, single host | One Karafka (all groups) + optional Rails API |
| **Split** | Production scale | Separate deployments per tier |

---

### Development — all 3 tiers on one machine

Run the **client** (enqueue + dashboard) and **Karafka** (control + execution) side by side. This is the fastest way to iterate locally.

**Prerequisites**

```bash
export KAFKA_PREFIX=dev
export REDIS_URL=redis://localhost:6379/0
export KAFKA_BROKERS=localhost:9092
bundle exec rake kafka_batch:create_topics
```

**Terminal 1 — Tier 2 + 3 (all Karafka consumer groups)**

```bash
KB_ROLE=all bundle exec karafka server
```

`KB_ROLE=all` turns on the schedule poller (generated initializer default). Omitting `--include-consumer-groups` runs **every** group `draw_routes` registers.

**Terminal 2 — Tier 1 (Rails client)**

```bash
bundle exec rails server
```

Enqueue from controllers or console:

```ruby
KafkaBatch::Batch.enqueue(MyWorker, { "id" => 1 })
```

**Optional — dashboard**

```ruby
# config/routes.rb
mount KafkaBatch::Web => "/kafka_batch"
```

**Minimal dev (Karafka only)** — skip Rails if you enqueue from `rails console` or a script while Karafka runs in another terminal:

```bash
KB_ROLE=all bundle exec karafka server
```

---

### Standalone — one deployment, all tiers running

Use when a **single host** or **one Kubernetes Deployment** should own the full pipeline: produce jobs, run control logic, and execute workers.

| Component | Tier | What to run |
|---|---|---|
| Karafka | 2 + 3 | All consumer groups (no filter) |
| Rails / Puma | 1 | HTTP API + `Batch.create` / `enqueue` |
| Dashboard | — | `mount KafkaBatch::Web` (optional) |

**Karafka (control + execution + schedule poller)**

```bash
KB_ROLE=all bundle exec karafka server
```

**Rails API (client — produces jobs, does not consume)**

Keep `daemon_mode` **false** if Karafka is a separate process (normal). The API only needs `require "kafka_batch"` and a configured producer:

```bash
bundle exec puma
```

If Rails and Karafka accidentally share one process, set `config.daemon_mode = true` on the web tier so Puma never registers consumers — Karafka still runs them in its own process.

**All-in-one on a single VM (no separate API)** — Karafka alone is enough; enqueue from console, cron, or an external producer.

---

### Split deployment — run one tier per process

Use in production to scale execution independently, keep API pods consumer-free, and isolate control-plane load.

#### Tier 1 — Client only

**Purpose:** HTTP API, batch creation, optional dashboard. **No Kafka consumers.**

```ruby
# config/initializers/kafka_batch.rb
config.daemon_mode = true
```

Or via environment:

```bash
KAFKA_BATCH_DAEMON_MODE=1 bundle exec puma
```

- Do **not** run `karafka server` in this deployment.
- Gemfile can use `require: "kafka_batch/ui"` for dashboard-only services that never enqueue.
- Full enqueue API needs `require "kafka_batch"` (loads `Batch` + `Producer`).

#### Tier 2 — Control only

**Purpose:** batch lifecycle (events, retry, callbacks), fair ingest → WFQ forwarder, delayed-job poller. Does **not** run `#perform`.

```bash
CG=myapp.kafka-batch   # must match KafkaBatch.config.consumer_group

bundle exec karafka server \
  --include-consumer-groups "${CG}-control,${CG}-dispatch-time,${CG}-dispatch-throughput"
```

**Schedule poller** — enable on 2–3 dedicated pods (not on every execution replica):

```bash
KB_ROLE=scheduler bundle exec karafka server \
  --include-consumer-groups "${CG}-control"
```

Or combine control + scheduler on the same groups with `KB_ROLE=control,scheduler`.

#### Tier 3 — Execution only

**Purpose:** consume job topics, run `#perform`, publish completion events. Requires **at least one control pod** somewhere for retries, callbacks, and fair ingest.

```bash
bundle exec karafka server \
  --include-consumer-groups "${CG}-jobs,${CG}-jobs-fair-time,${CG}-jobs-fair-throughput"
```

Add priority groups from your YAML (example suffix `jobs-fast`):

```bash
bundle exec karafka server \
  --include-consumer-groups "${CG}-jobs,${CG}-jobs-fast,${CG}-jobs-fair-time"
```

Keep the schedule poller **off** on execution swarms:

```ruby
config.schedule_poller_enabled = false
```

```bash
KB_ROLE=jobs bundle exec karafka server --include-consumer-groups "${CG}-jobs"
```

#### Tier cheat sheet

| Tier | Process | `daemon_mode` | Karafka `--include-consumer-groups` | Schedule poller |
|---|---|---|---|---|
| 1 — Client | Puma / API | `true` | *(none — no Karafka)* | off |
| 2 — Control | Karafka | `false` | `{CG}-control`, `{CG}-dispatch-*` | on scheduler pods |
| 3 — Execution | Karafka | `false` | `{CG}-jobs`, `{CG}-jobs-fair-*`, priority | off |
| All (dev / standalone) | Karafka | `false` | *(omit flag — all groups)* | on (`KB_ROLE=all`) |

---

### `KB_ROLE` and schedule poller

The install generator maps `KB_ROLE` → `schedule_poller_enabled` (see `config/initializers/kafka_batch.rb`):

```ruby
roles = ENV.fetch("KB_ROLE", "all").split(",").map(&:strip)
config.schedule_poller_enabled =
  case ENV["KB_SCHEDULE_POLLER"]
  when "true"  then true
  when "false" then false
  else (roles & %w[all scheduler]).any?
  end
```

| `KB_ROLE` | Karafka groups (typical) | Poller | Scale |
|---|---|---|---|
| `all` | everything | **on** | dev / standalone only |
| `control` | `{CG}-control`, `{CG}-dispatch-*` | off | few pods |
| `scheduler` | `{CG}-control` (light) | **on** | 2–3 fixed pods |
| `jobs` | `{CG}-jobs` | off | autoscale on lag |
| `fair-time` | `{CG}-dispatch-time`, `{CG}-jobs-fair-time` | off | autoscale on lag |
| `jobs-fast` | `{CG}-jobs-fast` (your YAML suffix) | off | autoscale on lag |

`KB_ROLE` does **not** filter Karafka groups by itself — it only gates the schedule poller. Use `--include-consumer-groups` (or run without it for all groups) to select which consumers a process runs.

---

### Production Kubernetes (split)

| Deployment | Tier | Replicas | Entrypoint |
|---|---|---|---|
| `kafka-batch-api` | 1 | N | `KAFKA_BATCH_DAEMON_MODE=1 bundle exec puma` |
| `kafka-batch-control` | 2 | 2–5 | `karafka server --include-consumer-groups ${CG}-control,${CG}-dispatch-time,...` |
| `kafka-batch-scheduler` | 2 | 2–3 | `KB_ROLE=scheduler karafka server --include-consumer-groups ${CG}-control` |
| `kafka-batch-worker` | 3 | autoscale | `karafka server --include-consumer-groups ${CG}-jobs,${CG}-jobs-fair-time,...` |

**Go handlers:** tier 2/3 for `runtime: go` jobs live in [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go) (`kbatch worker` on Go topics + `fair_*_ready.go`). Ruby and Go tiers share Kafka topics and Redis — deploy them as separate pods. Full setup: [Ruby + Go handlers](#ruby--go-handlers-mixed-runtime).

### Mixed Ruby + Go (production)

Typical layout when the API enqueues both runtimes:

```bash
# Tier 1 — API (enqueue Ruby + Go jobs; manifest loaded in initializer)
KAFKA_BATCH_DAEMON_MODE=1 bundle exec puma

# Tier 2 — Control (fair forwarder, events, retry, callbacks)
KB_ROLE=scheduler bundle exec karafka server \
  --include-consumer-groups ${CG}-control,${CG}-dispatch-time

# Tier 3 — Ruby execution
bundle exec karafka server \
  --include-consumer-groups ${CG}-jobs,${CG}-jobs-fair-time

# Tier 3 — Go execution (separate repo / Deployment)
# kbatch worker --manifest config/kafka_batch_handlers.yml ...
```

The client never chooses a runtime at enqueue time beyond the `job_type` — manifest `runtime` drives topic routing and which worker fleet picks up the job.

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

Topic names in priority YAML **and** worker `kafka_topic` declarations get `config.topic_prefix` applied automatically (same as `resolve_topic`). Use base names in workers; or pass the full prefixed name to pin it. `apply_prefix: false` opts out.

### Enroll workers

```ruby
class CriticalWorker
  include KafkaBatch::Worker
  kafka_topic "kafka_batch.jobs.p0"   # → myapp.kafka_batch.jobs.p0 when prefix set
end

class NormalWorker
  include KafkaBatch::Worker
  kafka_topic "kafka_batch.jobs.p1"   # yields while p0 has lag
end

# Explicit full name (no double-prefix):
# kafka_topic "myapp.kafka_batch.jobs.p0"
# Literal override (ignore global prefix):
# kafka_topic "legacy.queue", apply_prefix: false
```

### Modes

| Mode | Behaviour while higher topics have lag |
|---|---|
| **`strict`** | Lower ranks do **not** start new work until all higher topics are empty |
| **`weighted`** | Lower ranks interleave — default 1-in-4 messages proceed (`priority_weighted_interleave`) |

A higher topic **paused via `/lag`** is treated as inactive for the gate — lower ranks (p1) keep processing.

Lag checks use the Kafka Admin API, rate-limited per `priority_lag_check_interval` (default 2s). Unreachable cluster → **fail open** (process anyway).

### Boot rules

- Each topic → **one** consumer group (validated at boot)
- `kafka_batch.jobs` (default flat queue) **cannot** appear in priority YAML
- `fairness_type` on a worker bypasses priority topics

Run the priority group on dedicated pods:

```bash
bundle exec karafka server --include-consumer-groups myapp.kafka-batch-jobs-fast
```

---

## Multi-tenant fairness

Opt in per worker with `fairness_type :time` or `:throughput`. Two independent lanes run simultaneously:

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
config.fairness_min_ingest_partitions   = 300    # boot check (match ingest topic size)
```

Tune live weights at `/kafka_batch/weights` (stored in Redis per fairness lane, regardless of `config.store`).

### Tenant → ingest partition

Fair jobs should land on **one partition per tenant** so the Dispatcher processes each tenant serially on ingest. Three modes:

| Mode | Config | Behavior |
|---|---|---|
| Dynamic (default) | `fairness_dynamic_tenant_partitions = true` | On first enqueue, checkout a free partition from Redis (per lane); cached 30s in-process |
| Pinned | `fairness_tenant_partitions` | Static map; always wins over dynamic |
| Hash | `fairness_dynamic_tenant_partitions = false` | `tenant_id` keyed via murmur2 — many tenants can collide on one partition |

```ruby
# Default — automatic exclusive partitions (no big YAML map):
# config.fairness_dynamic_tenant_partitions = true  # already the default
config.fairness_tenant_partition_cache_ttl = 30

# Pin VIP tenants; everyone else still gets dynamic checkout:
config.fairness_tenant_partitions = { "acme" => 0 }

# Opt out — murmur2 key-hash only:
# config.fairness_dynamic_tenant_partitions = false
```

On boot the system reads each lane's ingest topic partition count and seeds a Redis free-pool (`kafka_batch:tenant_partitions:time`, etc.). When all partitions are assigned, new tenants log a warning and fall back to hash until you add partitions.

Lookup a tenant's partition on `/kafka_batch/fairness/time` (partition lookup widget).

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

**Rule:** `partitions ≥ peak_pods × karafka_concurrency × super_fetch_concurrency` on every execution topic you intend to scale. Karafka `concurrency` sizes how many partitions a process can hold; SuperFetch sizes concurrent `#perform`s **inside** that process.

### SuperFetch concurrency (Ruby)

These are **different knobs**:

| Knob | Controls |
|---|---|
| Karafka `concurrency` | How many partitions / consumer instances one process works on |
| `super_fetch_concurrency` | How many `#perform` threads may run after Claim+ack (default **`1`**) |

MRI Ruby threads share the GVL — they do **not** run Ruby CPU work in parallel. The default `super_fetch_concurrency = 1` keeps behavior close to sync consume. Raising it only helps when `#perform` spends time waiting on IO (HTTP, DB, Redis) so another thread can run while one is blocked.

**Production recommendation:** keep

```text
Karafka concurrency × super_fetch_concurrency ≤ 10
```

per process (e.g. Karafka `5` × SF `2`, or Karafka `10` × SF `1`). Higher products add thread/GC pressure without true parallelism.

**How to test higher values** (staging / load tests):

```ruby
# config/initializers/kafka_batch.rb
KafkaBatch.configure do |config|
  config.super_fetch_concurrency = 4  # try 2, 4, 8 while measuring lag + p99
end
# or: KAFKA_BATCH_SUPER_FETCH_CONCURRENCY=4
```

Watch consumer lag, Redis pool checkout wait (`redis_pool_size` should be ≥ SF + renewers + Karafka), RSS, and GC. If CPU-bound jobs dominate, prefer more pods (or Go workers) over raising SF.

Default partition targets (`KafkaBatch::Topics::DEFAULT_PARTITIONS`):

| Category | Partitions | Examples |
|---|---|---|
| Execution | 768 | jobs, priority, fair ready |
| Events | 48 | completion events |
| Fair ingest | 300 | per lane |
| Scheduled | 48 | delayed payloads |
| Retry | 12 | per tier |

Autoscale execution Deployments on **consumer lag** (KEDA / HPA). Partitions are fixed — pods are elastic.

### High-volume checklist

1. Size execution topics for peak pods × Karafka concurrency × `super_fetch_concurrency`; keep Ruby or Go control-plane reclaim on the same Redis
2. Keep `karafka_concurrency × super_fetch_concurrency ≤ 10` in prod; A/B higher SF only for IO-heavy handlers
3. Tune tenant weights at `/kafka_batch/weights` (weighted concurrency is on by default)
4. `fairness_lease_ttl` > longest job runtime
5. `schedule_poller_enabled` only on 2–3 scheduler pods
6. `track_running_jobs = false` at 50M+ jobs/day if `/live` per-job detail isn't needed
7. Split control from execution — see [Deployment](#split-deployment--run-one-tier-per-process)
8. Cap batch size or shard mega-batches in application code

---

## Web UI & reconciler

Mount `KafkaBatch::Web` at `/kafka_batch`. The dashboard is a **React + Material UI** SPA (assets shipped in the gem) that loads all data from JSON under `/kafka_batch/api/*`.

| Page | Shows |
|---|---|
| `/` | Batch list, status, cancel / delete / bulk |
| `/batches/:id` | Batch detail + paginated job failures |
| `/lag` | Per-group/topic lag, pause/resume (consumers see changes within `consumption_control_refresh_interval`, default **30s**) |
| `/live` | Running jobs & consumer heartbeats |
| `/weights/time` · `/weights/throughput` | Tenant fairness weights |
| `/fairness/time` · `/fairness/throughput` | Ingest / ready lag by fairness lane |
| `/failures` | Failure log |
| `/dead_letter` | Kafka dead-letter topic (paginated, newest first) |
| `/scheduled` | Delayed jobs in the schedule store |
| `/reconciler` | Last reconciler sweep + recovery counts |
| `/system` | Masked configuration snapshot |
| `/audit` | Web UI audit log (when enabled) |

**Live refresh:** the toolbar Live toggle (localStorage key `kafka_batch_live`) refetches API data every 5s.

**Building UI assets** (only needed when changing the frontend; built files are committed under `lib/kafka_batch/web/public/`):

```bash
cd frontend && npm ci && npm run build
```

**Reconciler** (inside `EventConsumer`, periodic): recovers stuck `running` batches and re-dispatches lost callbacks. Summary is persisted in Redis for `/reconciler`. Manual: `rake kafka_batch:reconcile`.

### JSON API (same mount)

All mutating calls require the `_kb_csrf` cookie **and** a matching `X-CSRF-Token` header (or body `_csrf`). Obtain the token from `GET /api/bootstrap` (or the SPA shell bootstrap).

| Method | Path |
|---|---|
| GET | `/api/bootstrap`, `/api/dashboard`, `/api/batches`, `/api/batches/:id` |
| POST / DELETE | `/api/batches/:id/cancel`, `/api/batches/:id`, `/api/batches/bulk` |
| GET | `/api/failures`, `/api/live`, `/api/lag`, `/api/scheduled`, `/api/system`, `/api/reconciler`, `/api/dead_letter`, `/api/audit` |
| GET | `/api/fairness/:type`, `/api/weights/:type` (`time` \| `throughput`) |
| POST | `/api/lag/pause`, `/api/lag/resume` |
| PUT / DELETE | `/api/weights/:type`, `/api/weights/:type/:tenant_id` |

### Securing the dashboard

The dashboard exposes destructive actions (cancel/delete, pause/resume, weight edits) and config/dead-letter payloads, so it **must** sit behind authentication. Always wrap the mount in your app's auth:

```ruby
authenticate :admin do
  mount KafkaBatch::Web => "/kafka_batch"
end
```

Built-in defences:

- **CSRF** — double-submit cookie (`SameSite=Strict`, `HttpOnly`, `Secure` on HTTPS); SPA clients send `X-CSRF-Token` (token from `/api/bootstrap`), and comparison is constant-time.
- **Credential masking** — the Redis URL (and other secrets) are masked wherever displayed (`/system`, `/lag`).
- **DoS caps** — POST bodies are capped (1 MiB), bulk cancel/delete enumeration is bounded, and the weights page caps rendered tenants.
- **Optional authenticator** (defence-in-depth, not a replacement for host auth):

  ```ruby
  config.web_authenticator = ->(env) {
    # return truthy to allow, falsey to reject with 401
    ActionController::HttpAuthentication::Basic.with_credentials(env) { |u, p| Rack::Utils.secure_compare(p, ENV["KB_WEB_PASS"]) && u == "admin" }
  }
  ```


### Web UI audit log

Every mutating dashboard action (cancel, delete, pause/resume, weight set/reset) can be persisted to a `kafka_batch_audit_logs` table for an operator trail. It's **off by default** and backed by ActiveRecord (independent of `config.store`).

**1. Copy the migration** (the installer only emits it when asked):

```bash
bundle exec rails generate kafka_batch:install --audit
bundle exec rails db:migrate
```

Or, without re-running the generator, copy the migration the gem already ships:

```bash
cp "$(bundle show kafka-batch)/db/migrate/20240101000004_create_kafka_batch_audit_logs.rb" db/migrate/
bundle exec rails db:migrate
```

**2. Enable it** in `config/initializers/kafka_batch.rb`:

```ruby
config.audit_enabled = true
# optional: a dedicated DB connection (AR model class, database.yml name, or connection Hash)
# config.audit_database_connection = :kafka_batch_audit
# optional: attribute the action to a user — a Proc(env) → String, or a static string.
# Falls back to request headers when unset.
config.audit_actor = ->(env) { env["HTTP_X_FORWARDED_USER"] }
```

Each row records `action`, `path`, `method`, `actor`, `node_id`, `status` (`ok`/`error`), a `metadata` JSON blob, and `created_at`. With `audit_enabled = false` (default) no table is required and nothing is written.

### Pause / resume (`/lag`)

Pause state is written to Redis (or MySQL when `store = :mysql`) **immediately** when you click Pause. Karafka consumers **cache** that state and re-read it at most every `consumption_control_refresh_interval` seconds (default **30**). Until the cache refreshes, jobs may still run and lag can keep falling — wait up to one interval after pausing.

```ruby
config.consumption_control_refresh_interval = 30  # seconds; lower = faster pause, more Redis reads
```

The `/lag` page shows a tooltip on Pause/Resume buttons explaining the delay. Pause is keyed on **consumer group + topic** (use the row’s group/topic, not the topic name alone).

**Priority queues:** pausing a higher topic (e.g. p0) via `/lag` stops draining that topic but does **not** block lower ranks (p1 keeps processing). Strict priority only applies while higher topics are actively consuming.

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

These events are the integration point for the built-in metrics bridge below.

### Metrics export (StatsD / Datadog / Prometheus)

kafka-batch ships an opt-in bridge from `ActiveSupport::Notifications` to a metrics client **you supply** (no metrics gem dependency). Per event it emits `#{prefix}.<event>.count` (increment) and `#{prefix}.<event>.duration` (timing, when the client supports it), tagged from the event payload (`worker_class`, `outcome`, `dlt_type`, …).

**StatsD / Datadog** — bring a tag-capable client. Use [`dogstatsd-ruby`](https://rubygems.org/gems/dogstatsd-ruby) (works for both); the classic `statsd-ruby` `Statsd` has no `tags:` kwarg, so its emits would be dropped.

```ruby
# Gemfile: gem "dogstatsd-ruby"
KafkaBatch.configure do |config|
  config.metrics_enabled = true
  config.metrics_adapter = :statsd                       # or :datadog (same wire API)
  config.metrics_client  = Datadog::Statsd.new("localhost", 8125)
  config.metrics_prefix  = "kafka_batch"                 # default
end
```

The client must respond to `#increment(name, tags:)`; `#timing` is optional. `config.validate!` fails fast if `metrics_enabled` is set without a client.

**Prometheus / custom sink** — use the `:proc` adapter:

```ruby
config.metrics_enabled = true
config.metrics_adapter = :proc
config.metrics_proc = ->(name, payload, duration_ms) {
  MY_PROMETHEUS.counter(name.tr(".", "_"), labels: payload.slice(:worker_class)).inc
}
```

**Wiring:** in Rails the railtie calls `KafkaBatch::Metrics.install!` on boot when `metrics_enabled` is true. In a non-Rails process, call `KafkaBatch::Metrics.install!` yourself after `configure` — in **every** tier-3 Ruby execution process (`karafka server` job consumers).

For Go metrics, see [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go) (`instrument.SetHandler` in daemon/worker binaries).

Example series: `kafka_batch.job_processed.count`, `kafka_batch.job_processed.duration`, `kafka_batch.job_failed.count`, `kafka_batch.batch_completed.count` (`outcome:` tag), `kafka_batch.dlt_published.count` (`dlt_type:` tag).

### Where to visualize

- **Live operational state** — mount `KafkaBatch::Web` (batches, `/failures`, `/dead_letter`, `/live`, `/lag`, `/fairness`, `/audit`). This is current state, not time-series.
- **The metrics above** render wherever your client's pipeline terminates — **Grafana** (Prometheus via `statsd_exporter`, or Graphite) or **Datadog** (via the Agent). The gem ships no dashboard; build panels on the `kafka_batch.*` series, paired with Kafka consumer-lag from your broker's exporter.

### DIY (no bridge)

Prefer to wire it by hand? Subscribe to the notifications directly:

```ruby
# config/initializers/kafka_batch_metrics.rb
ActiveSupport::Notifications.subscribe(/\.kafka_batch\z/) do |name, *rest|
  event = ActiveSupport::Notifications::Event.new(name, *rest)
  StatsD.increment("kafka_batch.events", tags: ["event:#{name.sub('.kafka_batch', '')}"])
end
```

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
| `sidekiq_retries_exhausted` | `retries_exhausted` (alias: `sidekiq_retries_exhausted`) |

Workers use `include KafkaBatch::Worker` and run under Karafka instead of Sidekiq threads. Set an explicit `job_type` when you plan to run Ruby and Go handlers on the same Kafka topic (fair/priority lanes). Callback signatures are structurally the same — keep them idempotent.

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

## TCO vs Sidekiq at scale

Rough **full annual TCO** (AWS infrastructure + Sidekiq license) for large **pending backlogs**. Intended for capacity planning — not a quote.

### Scope & assumptions

| Assumption | Value |
|---|---|
| **Pending jobs** | 100M or 200M jobs **waiting** in the queue (not yet consumed) |
| **Payload size** | ~800 bytes average JSON per job (class + args + metadata) |
| **Region** | us-east-1, on-demand list pricing |
| **Excluded** | Worker/compute pods (Sidekiq processes **and** Karafka consumers), data-transfer egress, support/SRE headcount |
| **Sidekiq Enterprise threads** | 1,500 production threads (e.g. 150 pods × concurrency 10) — [volume pricing](https://billing.contribsys.com/sent/new.cgi) at 13+ packs ($189/mo per 100 threads) |
| **kafka-batch license** | MIT — **$0** (Karafka OSS; Karafka Pro is separate if needed) |

**Sidekiq** stores every pending job in **Redis (RAM)**. **kafka-batch** stores job payloads in **Kafka (disk)**; Redis holds coordination only (batch ledger, fairness, uniq locks).

**Feature parity:** Batches alone maps to **Sidekiq Pro**. Unique jobs, rate limiting, periodic jobs, and weighted queues map to **Sidekiq Enterprise** — closer to kafka-batch’s full feature set.

### Licensing (annual)

| Product | Annual license | Notes |
|---|---|---|
| **Sidekiq Pro** | **$995/yr** | Batches + reliable scheduler; unlimited processes per org |
| **Sidekiq Enterprise** | **~$34,020/yr** | Pro + unique jobs, rate limiting, periodic jobs, etc.; priced per 100 **production** threads |
| **kafka-batch** | **$0** | MIT |

Enterprise unlimited license is **$79,500/yr** — typically cheaper above ~3,500 production threads. See the [Commercial FAQ](https://github.com/sidekiq/sidekiq/wiki/Commercial-FAQ).

### AWS infrastructure (annual)

| Stack | 100M pending | 200M pending |
|---|---|---|
| **Sidekiq (ElastiCache Redis)** | ~**$30k–$60k**/yr | ~**$60k–$120k**/yr |
| **kafka-batch (MSK + Redis ± RDS)** | ~**$13k–$30k**/yr | ~**$14k–$34k**/yr |

Sidekiq infra scales ~linearly with backlog (RAM for every job). kafka-batch infra is dominated by **MSK broker baseline**; disk grows sub-linearly thanks to compression — doubling backlog does not double cost.

### Full annual TCO (infra + license)

#### 100M pending jobs

| | License / yr | AWS infra / yr | **Total / yr** | **Total / mo** |
|---|---|---|---|---|
| **Sidekiq Pro** | $995 | ~$30k–$60k | **~$31k–$61k** | **~$2.6k–$5.1k** |
| **Sidekiq Enterprise** | ~$34k | ~$30k–$60k | **~$64k–$94k** | **~$5.3k–$7.8k** |
| **kafka-batch** | $0 | ~$13k–$30k | **~$13k–$30k** | **~$1.1k–$2.5k** |

#### 200M pending jobs

| | License / yr | AWS infra / yr | **Total / yr** | **Total / mo** |
|---|---|---|---|---|
| **Sidekiq Pro** | $995 | ~$60k–$120k | **~$61k–$121k** | **~$5.1k–$10.1k** |
| **Sidekiq Enterprise** | ~$34k | ~$60k–$120k | **~$94k–$154k** | **~$7.8k–$12.8k** |
| **kafka-batch** | $0 | ~$14k–$34k | **~$14k–$34k** | **~$1.2k–$2.8k** |

At 200M pending with Enterprise-equivalent features and ~1,500 Sidekiq threads, kafka-batch is roughly **~$80k–$120k/yr** cheaper all-in — mostly Redis RAM vs Kafka disk, plus ~$34k/yr license.

### What moves these numbers

| Factor | Effect |
|---|---|
| **Larger payloads (2 KB)** | Sidekiq Redis cost **~2.5×**; kafka-batch Kafka disk **~2×** |
| **Fewer Sidekiq threads** | Enterprise license drops (5 packs ≈ $13.7k/yr); infra unchanged |
| **Reserved Instances / Savings Plans** | 30–40% off AWS; Sidekiq license unchanged |
| **Long pending duration** | Sidekiq holds RAM until processed; Kafka retention can grow disk if jobs sit for weeks |
| **200M jobs in one batch** | Sidekiq batch dedup pain; kafka-batch still hits one Redis hash hotspot per batch ([pitfall #6](#non-negotiable-pitfalls)) |

> **Reality check:** 100–200M jobs pending in Redis is uncommon — most Sidekiq fleets shard queues or cap backlog. kafka-batch is designed for durable, partition-parallel backlogs on Kafka.

---

## Contributing

Issues and PRs welcome at [github.com/y-shashank/kafka-batch](https://github.com/y-shashank/kafka-batch).
