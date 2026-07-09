# kafka-batch

[![CI](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/y-shashank/kafka-batch/badges/coverage.json)](https://github.com/y-shashank/kafka-batch/actions/workflows/ci.yml)

**Sidekiq Pro Batches on Kafka.** Same `on_success` / `on_complete` callback model, per-job retries, and batch completion counting — with Kafka as the durable job transport and Redis for coordination.

Built on [Karafka](https://karafka.io) (WaterDrop + consumers). **Go runtime:** [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go). Go runtime: [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go).

---

## Table of contents

- [Non-negotiable pitfalls](#non-negotiable-pitfalls)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Workers & jobs](#workers--jobs)
- [Batches & callbacks](#batches--callbacks)
- [Configuration](#configuration)
- [Three-tier architecture](#three-tier-architecture)
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

Each Kafka **execution** topic belongs to exactly one runtime (`ruby` or `go`). The client routes jobs from the handler manifest; Ruby enqueues both runtimes. Go execution uses [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go).

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

Ruby workers run in-process via Karafka `JobConsumer`. Go workers use [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go). Never mix runtimes on the same execution topic or consumer group.

**Go handlers** — declare `runtime: go` in the manifest; execution lives in kafka-batch-go:

```yaml
handlers:
  segment.export:
    runtime: go
    topic: segment.exports
```

Enqueue from Ruby:

```ruby
batch.push_job("segment.export", "segment_id" => 42)
KafkaBatch::Batch.enqueue_job("segment.export", "segment_id" => 42)
```

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

**Execution (tier 3):** Consumes **ruby** plain/priority topics and `fair_*_ready.ruby` only. Go execution topics are consumed by `kbatch worker` in kafka-batch-go.

### Data flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Tier 1 — Client (KafkaBatch::Batch)                                        │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ produce
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Kafka + Redis                                                              │
└───────────────┬───────────────────────────────┬───────────────────────────┘
                │                               │
                ▼                               ▼
┌───────────────────────────────┐   ┌─────────────────────────────────────────┐
│  Tier 2 — Karafka control     │   │  Tier 3 — Karafka JobConsumer (ruby)    │
│  • fair ingest → WFQ          │   │  • Worker#perform                       │
│  • events / retry / callbacks │   │  • publish success/failed → events      │
│  • schedule poller            │   │                                         │
└───────────────────────────────┘   └─────────────────────────────────────────┘
```

For **Go handlers**, use kafka-batch-go for tier 2 (`kbatch daemon`) and/or tier 3 (`kbatch worker`) on go topics + `fair_*_ready.go`.

### Topic rules

- **One execution topic = one runtime.** Manifest validation rejects a topic shared by `ruby` and `go` handlers.
- **Fairness:** one **ingest** topic per lane (shared); two **ready** topics per lane (`.go` / `.ruby`). Forwarder routes by manifest `runtime`.
- **Consumer groups** on ready topics: `-jobs-fair-*` (Ruby), `-go-worker-fair-ready-*` (Go worker).

### Handler manifest

```yaml
handlers:
  segment.export:
    runtime: go
    topic: segment.exports
  orders.process:
    runtime: ruby
    worker_class: Orders::ProcessWorker
    topic: kafka_batch.jobs
```

### API / client pods (produce only)

```ruby
config.daemon_mode = true   # skips all Karafka consumers in this process
```

Use dedicated Karafka deployments for tier 2 (control) and tier 3 (jobs).

### Development (local, Ruby-only)

```bash
export KAFKA_PREFIX=dev
export REDIS_URL=redis://localhost:6379/0
bundle exec karafka server
```

Split across terminals with `KB_ROLE` or `--include-consumer-groups` when you want control and execution as separate processes.

### Production (Kubernetes, Ruby stack)

| Deployment | Tier | Command | Kafka consumers |
|------------|------|---------|-----------------|
| `kafka-batch-api` | 1 | Puma + gem | **none** (`daemon_mode: true`) |
| `kafka-batch-control` | 2 | `karafka server` (control groups) | ingest, events, retry, callbacks, schedule |
| `kafka-batch-worker` | 3 | `karafka server` (job groups) | ruby plain, priority, `fair_*_ready.ruby` |

**Go stack:** see [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go).

---

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
| `consumption_control_refresh_interval` | `30` | How often consumers re-read pause state from Redis/MySQL |
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
| Hash (default) | neither set | `tenant_id` keyed via murmur2 — many tenants can collide on one partition |
| Pinned | `fairness_tenant_partitions` | Static map; always wins |
| Dynamic | `fairness_dynamic_tenant_partitions = true` | On first enqueue, checkout a free partition from Redis (per lane); cached 30s in-process |

```ruby
# Option A — pin VIP tenants, dynamic for the rest:
config.fairness_tenant_partitions = { "acme" => 0 }
config.fairness_dynamic_tenant_partitions = true

# Option B — fully automatic (no big YAML map):
config.fairness_dynamic_tenant_partitions = true
config.fairness_tenant_partition_cache_ttl = 30
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

**Rule:** `partitions ≥ peak_pods × concurrency` on every execution topic you intend to scale.

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
| `/lag` | Per-group/topic lag, pause/resume (consumers see changes within `consumption_control_refresh_interval`, default **30s**) |
| `/live` | Running jobs & consumer heartbeats |
| `/weights` | Tenant fairness weights |
| `/failures` | Failure log |
| `/dead_letter` | Kafka dead-letter topic (paginated, newest first) |
| `/reconciler` | Last reconciler sweep + recovery counts |

**Reconciler** (inside `EventConsumer`, periodic): recovers stuck `running` batches and re-dispatches lost callbacks. Summary is persisted in Redis for `/reconciler`. Manual: `rake kafka_batch:reconcile`.

### Securing the dashboard

The dashboard exposes destructive actions (cancel/delete, pause/resume, weight edits) and config/dead-letter payloads, so it **must** sit behind authentication. Always wrap the mount in your app's auth:

```ruby
authenticate :admin do
  mount KafkaBatch::Web => "/kafka_batch"
end
```

Built-in defences:

- **CSRF** — double-submit cookie (`SameSite=Strict`, `HttpOnly`, `Secure` on HTTPS); the token rides a hidden form field (never the URL), and comparison is constant-time.
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
