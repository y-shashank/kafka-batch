# kafka-batch

Drop-in replacement for **Sidekiq Pro Batches** using Apache Kafka as the transport layer. Provides the same `on_success` / `on_complete` callback semantics, per-job retry with backoff, and idempotent completion tracking — at a fraction of the cost.

Built on the [Karafka](https://karafka.io) ecosystem: **WaterDrop** for producing, **Karafka consumers** for processing.

---

## Table of Contents

- [How it works](#how-it-works)
- [Installation](#installation)
- [Configuration](#configuration)
  - [MySQL store](#mysql-store)
  - [Redis store](#redis-store)
  - [Karafka routing](#karafka-routing)
- [Defining workers](#defining-workers)
- [Creating batches](#creating-batches)
  - [Standalone jobs (no batch)](#standalone-jobs-no-batch)
- [Callbacks](#callbacks)
- [Retry behaviour](#retry-behaviour)
- [Dead Letter Topic](#dead-letter-topic)
- [Reconciler](#reconciler)
- [Rake tasks](#rake-tasks)
- [Reliability guarantees](#reliability-guarantees)
- [Known limitations](#known-limitations)
- [Migrating from Sidekiq Pro Batches](#migrating-from-sidekiq-pro-batches)
- [Architecture deep-dive](#architecture-deep-dive)
- [Topic reference](#topic-reference)
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
│    b.push(MyWorker, { id: 3 })  ──┘                              │
│                                                                   │
│  BatchRecord written to MySQL/Redis BEFORE first produce          │
└──────────────────────────────────────────────────────────────────┘
                │ (jobs topic)
   ┌────────────▼────────────┐
   │    Karafka: JobConsumer  │
   │                         │
   │  worker.perform(payload) │
   │    ├─ success ──────────┼──► kafka_batch.events
   │    │                    │
   │    └─ failure           │
   │        ├─ retriable ────┼──► kafka_batch.jobs.retry
   │        └─ exhausted ────┼──► kafka_batch.dead_letter
   └─────────────────────────┘         +events (failed)
                │ (events topic)
   ┌────────────▼────────────┐
   │  Karafka: EventConsumer  │
   │                         │
   │  store.record_job_       │
   │  completion(...)         │
   │    ├─ running ──► skip  │
   │    └─ done ─────────────┼──► kafka_batch.callbacks
   └─────────────────────────┘
                │ (callbacks topic)
   ┌────────────▼────────────┐
   │ Karafka: CallbackConsumer│
   │                         │
   │  store.claim_callback() ─┼── atomic CAS; only one
   │    ├─ won ──────────────┼──  process fires callbacks
   │    │  on_success(batch)  │
   │    │  on_complete(batch) │
   │    └─ lost ──► skip     │
   └─────────────────────────┘

   ┌─────────────────────────┐
   │  Karafka: RetryConsumer  │  ◄── kafka_batch.jobs.retry
   │                         │
   │  retry_after in future? │
   │    ├─ yes ──► pause()   │  (Karafka partition pause –
   │    │         then retry  │   zero thread blocking)
   │    └─ no  ──► produce   │
   │              to retry_to │──► original worker topic
   └─────────────────────────┘
```

---

## Installation

Add to your `Gemfile`:

```ruby
gem "kafka-batch"
```

Run the installer:

```bash
bundle exec rails generate kafka_batch:install
# or with Redis store:
bundle exec rails generate kafka_batch:install --store redis
```

This creates:
- `config/initializers/kafka_batch.rb`
- Database migrations (MySQL store only)

Run migrations if using the MySQL store:

```bash
bundle exec rails db:migrate
```

Create the required Kafka topics (adjust partitions to your throughput):

```bash
kafka-topics.sh --create --topic kafka_batch.jobs       --partitions 6
kafka-topics.sh --create --topic kafka_batch.events     --partitions 3
kafka-topics.sh --create --topic kafka_batch.callbacks  --partitions 1
kafka-topics.sh --create --topic kafka_batch.jobs.retry --partitions 3
kafka-topics.sh --create --topic kafka_batch.dead_letter --partitions 1
```

---

## Configuration

Edit `config/initializers/kafka_batch.rb`:

```ruby
KafkaBatch.configure do |config|
  # ── State store ────────────────────────────────────────────────────
  # :mysql  – persistent, survives Redis restarts, queryable via SQL
  # :redis  – lower latency, no schema migration needed
  config.store = :mysql

  # ── Kafka brokers ───────────────────────────────────────────────────
  config.brokers = ENV.fetch("KAFKA_BROKERS", "localhost:9092").split(",")

  # ── Topic names ─────────────────────────────────────────────────────
  config.jobs_topic        = "kafka_batch.jobs"
  config.events_topic      = "kafka_batch.events"
  config.callbacks_topic   = "kafka_batch.callbacks"
  config.retry_topic       = "kafka_batch.jobs.retry"   # dedicated retry topic
  config.dead_letter_topic = "kafka_batch.dead_letter"

  # ── Consumer group ──────────────────────────────────────────────────
  config.consumer_group = "kafka-batch"

  # ── Retry behaviour (global defaults; override per Worker class) ────
  config.max_retries   = 3   # max attempts before dead letter
  config.retry_backoff = 5   # seconds; sleep = attempt × retry_backoff

  # ── Redis (only when store: :redis) ─────────────────────────────────
  config.redis_url       = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  config.redis_pool_size = 5
  config.batch_ttl       = 7 * 24 * 3600  # seconds until Redis keys expire

  # ── Reconciliation ───────────────────────────────────────────────────
  config.reconciliation_interval = 300  # seconds

  # ── Advanced rdkafka / WaterDrop config overrides ───────────────────
  # config.producer_config = { "compression.type" => "snappy" }
  # config.consumer_config = { "fetch.min.bytes"  => "1024"   }
end
```

### MySQL store

Requires three migrations (two tables + one column addition):

| Migration | What it creates |
|---|---|
| `create_kafka_batch_records` | Batch state table with counters and status |
| `create_kafka_batch_job_completions` | Per-job completion dedup table (unique constraint on `batch_id, job_id`) |
| `add_callback_tracking_to_kafka_batch_records` | `callback_dispatched_at` column for at-most-once callback enforcement |

```bash
bundle exec rails db:migrate
```

### Redis store

No migrations needed. Batch state is stored as a Redis Hash at `kafka_batch:b:{batch_id}`. Completed job IDs are tracked in a Set at `kafka_batch:b:{batch_id}:done_jobs`. Both keys expire after `config.batch_ttl` seconds and are refreshed on every job completion event.

> **Note:** The Redis store's reconciler methods (`stale_batches`, `done_batches_without_callback`) are no-ops. Maintain a separate sorted set of batch IDs keyed by `created_at` / `finished_at` score if you need reconciliation on Redis.

### Karafka routing

Wire up KafkaBatch routes inside your `karafka.rb`. Call `KafkaBatch.draw_routes` **after** all worker classes are loaded so the registry is fully populated:

```ruby
class KarafkaApp < Karafka::App
  setup do |config|
    config.kafka = { "bootstrap.servers" => ENV["KAFKA_BROKERS"] }
    config.client_id = "my-app"
  end

  routes.draw do
    # Your own routes
    topic "my_app.events" do
      consumer MyEventsConsumer
    end

    # KafkaBatch: registers internal topics + one job route per worker
    KafkaBatch.draw_routes(self)
  end
end
```

`draw_routes` registers four consumer groups:

| Group | Topic(s) | Consumer |
|---|---|---|
| `kafka-batch-jobs` | Each worker's `kafka_topic` | `JobConsumer` |
| `kafka-batch-retry` | `kafka_batch.jobs.retry` | `RetryConsumer` |
| `kafka-batch-events` | `kafka_batch.events` | `EventConsumer` |
| `kafka-batch-callbacks` | `kafka_batch.callbacks` | `CallbackConsumer` |

---

## Defining workers

Include `KafkaBatch::Worker` and implement `#perform`:

```ruby
class ProcessOrderWorker
  include KafkaBatch::Worker

  kafka_topic   "orders.process"   # required – Kafka topic to consume from
  max_retries   5                  # optional – overrides config.max_retries
  retry_backoff 10                 # optional – overrides config.retry_backoff (seconds)

  # payload is the Hash passed to Batch#push or Batch.enqueue
  def perform(payload)
    order = Order.find(payload["order_id"])
    order.process!
  end
end
```

> **Workers must be idempotent.** If `perform` succeeds but the subsequent event-emission fails, the job message is redelivered and `perform` runs again. Design your workers so running twice produces the same result (upsert, check-before-write, etc.).

---

## Creating batches

```ruby
batch_id = KafkaBatch::Batch.create(
  on_success:  "BatchSuccessCallback",   # called if ALL jobs succeed
  on_complete: "BatchCompleteCallback",  # called when ALL jobs finish (any status)
  meta: { report_id: 42, user_id: 99 }  # arbitrary data forwarded to callbacks
) do |b|
  Order.find_each do |order|
    b.push(ProcessOrderWorker, order_id: order.id)
  end
end

puts batch_id  # => "550e8400-e29b-41d4-a716-446655440000"
```

The batch record is written to the store with the exact job count **before** the first Kafka message is produced. If `produce_sync` fails mid-way through, the store record is rolled back via `delete_batch` so the batch doesn't linger in "running" with an unreachable total.

An optional explicit `job_id` can be passed for tracing:

```ruby
b.push(ProcessOrderWorker, { order_id: 1 }, job_id: "order-1-#{Time.now.to_i}")
```

### Standalone jobs (no batch)

```ruby
KafkaBatch::Batch.enqueue(ProcessOrderWorker, order_id: 99)
```

The job goes through the same retry / DLT flow but no batch completion tracking occurs.

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

**At-most-once guarantee:** Before invoking any callback, `CallbackConsumer` performs an atomic compare-and-swap on `callback_dispatched_at` in the store. Whichever consumer process wins the claim is the only one that fires the callbacks — even if the callback message is redelivered due to a consumer crash.

**Unresolvable class names:** If the callback class doesn't exist (typo, rename after deploy), the message is forwarded to `dead_letter_topic` with `dlt_type: "callback"` instead of being silently dropped.

---

## Retry behaviour

When a job raises an exception, `JobConsumer` catches it and takes one of two paths based on the current attempt count:

**Retriable (attempt < max_retries):**
The message is produced to `kafka_batch.jobs.retry` with two extra fields:
- `retry_after` — ISO8601 timestamp of when to re-enqueue (now + `attempt × retry_backoff` seconds)
- `retry_to` — the original worker topic to re-enqueue to

The `JobConsumer` partition is immediately freed for the next message. No thread blocking occurs.

**RetryConsumer** picks up the message. If `retry_after` is still in the future, it calls Karafka's `pause(offset, ms)` to suspend that partition for up to `MAX_PAUSE_SECONDS` (30s) at a time, then checks again. When the message is due it re-enqueues to `retry_to` and commits.

**Exhausted (attempt == max_retries):**
A `failed` event is emitted to the events topic (so the batch counter is updated) and the message is forwarded to the dead-letter topic.

Backoff is linear: `sleep = attempt × retry_backoff`.
With `max_retries: 3, retry_backoff: 5` — delays are 5s, 10s, 15s.

Override per worker:

```ruby
class CriticalWorker
  include KafkaBatch::Worker
  kafka_topic   "critical.jobs"
  max_retries   10
  retry_backoff 30   # 30s, 60s, 90s, ...
end
```

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

For unresolvable callback classes:

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

Subscribe a consumer in your `karafka.rb` to alert, log, or trigger manual replay:

```ruby
topic KafkaBatch.config.dead_letter_topic do
  consumer DeadLetterConsumer
end
```

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

**Recovery:** Re-produces the callback message to `kafka_batch.callbacks`. The `CallbackConsumer`'s atomic claim (`callback_dispatched_at` CAS) ensures the actual callback fires exactly once even if this runs multiple times.

```bash
bundle exec rake kafka_batch:reconcile
```

Schedule with cron or Whenever:

```ruby
# config/schedule.rb
every 5.minutes do
  rake "kafka_batch:reconcile"
end
```

---

## Rake tasks

| Task | Description |
|---|---|
| `kafka_batch:reconcile` | Run both reconciler sweeps (stuck-running + lost-callback) |
| `kafka_batch:install_migrations` | Copy all migrations to `db/migrate/` |
| `kafka_batch:workers` | Print all registered workers, topics, and retry config |

```bash
bundle exec rake kafka_batch:workers
#  ProcessOrderWorker   → topic: orders.process   retries: 5  backoff: 10s
#  GenerateReportWorker → topic: reports.generate retries: 3  backoff: 5s
```

---

## Reliability guarantees

| Guarantee | How it's achieved |
|---|---|
| **Batch never prematurely completes** | Store record (with exact `total_jobs`) is written before the first message is produced |
| **Partial produce is cleaned up** | If `produce_sync` fails mid-batch, `delete_batch` rolls back the store record |
| **Job completion is idempotent** | Unique constraint on `(batch_id, job_id)` (MySQL) or `SADD` (Redis) deduplicates event messages |
| **Counter increment is atomic** | MySQL `SELECT FOR UPDATE` + `UPDATE field = field + 1`; Redis Lua script |
| **Callback fires at most once** | `claim_callback` does `UPDATE WHERE callback_dispatched_at IS NULL` — only the winner invokes |
| **Lost callbacks are recovered** | Reconciler scans for `status IN (success,complete) AND callback_dispatched_at IS NULL` |
| **Retries don't block partitions** | Failed jobs go to `kafka_batch.jobs.retry`; `RetryConsumer` uses Karafka `pause()` |
| **Event emission failure ≠ job failure** | Separate rescue blocks; emission retried independently before leaving offset uncommitted |
| **Unresolvable callbacks are not silently dropped** | Forwarded to `dead_letter_topic` with `dlt_type: "callback"` |
| **Consumer crash after callback but before commit** | `claim_callback` CAS prevents double-invocation on redelivery |

---

## Known limitations

**Workers must be idempotent.** If event emission fails after a successful `perform`, Karafka redelivers the job and `perform` runs again. Design workers to tolerate duplicate execution (upsert, guard clauses, etc.).

**Redis reconciliation is a no-op.** The Redis store cannot range-scan hash field values. For Redis-backed reconciliation, maintain a sorted set of batch IDs by `created_at`/`finished_at` in your application and pass them to a custom sweep.

**Redis TTL.** Batch keys expire after `batch_ttl` seconds. The TTL is refreshed on every job completion event, but a batch with no activity for longer than `batch_ttl` will lose its state. Set `batch_ttl` well above your longest expected batch duration.

**Worker class renames after deploy.** In-flight messages carry the original class name. After removing or renaming a worker, those jobs will retry until exhausted and land in the DLT. Perform a rolling deploy or drain the topic before removing the class.

**No built-in metrics.** There is no Prometheus/StatsD instrumentation. Query the store directly or subscribe to `ActiveSupport::Notifications` if you add your own instrumentation hooks.

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

**Key difference:** Workers must `include KafkaBatch::Worker`, define a `kafka_topic`, and be **idempotent**. They are consumed by Karafka rather than Sidekiq threads.

---

## Architecture deep-dive

### Why write the batch record before producing?

If the store record were written after producing, a fast consumer could complete all N jobs and find no batch record — resulting in `not_found` and a permanently lost callback. Writing first with the exact count guarantees the store is ready before any completion event can arrive.

### Why roll back on partial produce failure?

If only M of N messages reach Kafka, the store has `total_jobs: N` but only M jobs will ever complete. Without rollback, the batch hangs in "running" indefinitely. With `delete_batch`, the caller receives a `ProducerError` and can retry the entire `Batch.create` call.

### Why `FOR UPDATE` and not `LOCK IN SHARE MODE`?

Share locks (`LOCK IN SHARE MODE`) allow concurrent readers. Two `EventConsumer` threads processing the last two jobs can both enter the transaction simultaneously, both increment, both reload and see `completed >= total`, and both publish the callback — double-firing `on_success`. `FOR UPDATE` (`.lock`) serialises access: the second thread blocks until the first commits, sees the already-finalised status, and returns `:duplicate`.

### Why separate `perform` and event-emission rescue blocks?

With a single `rescue`, a transient Kafka error on `emit_event` looks identical to a job failure and triggers a job retry. The work was already done — retrying the job runs it again (possibly corrupting state) and eventually sends a false "failed" event to the DLT. Separate rescue blocks mean event-emission failures are retried independently, and only a worker-raised exception triggers the job retry path.

### Why a dedicated retry topic instead of `sleep`?

A `sleep` inside `JobConsumer` blocks the entire Kafka partition for the duration. With `max_retries: 3, retry_backoff: 5`, a single exhausted job stalls its partition for 30 seconds. Under load, multiple failing jobs on the same partition compound this. The retry topic approach forwards the message immediately and suspends only the *retry partition* (via Karafka `pause()`) — the job partition is fully unblocked.

### Why `claim_callback` as a CAS instead of checking first?

A read-before-write ("check if claimed, then claim") has a race: two `CallbackConsumer` processes read `callback_dispatched_at = nil` simultaneously, both conclude they should proceed, both claim, and both fire. A conditional `UPDATE WHERE callback_dispatched_at IS NULL` is atomic at the database level — exactly one process gets `rows_affected = 1`.

### Message flow (numbered)

```
1.  App             → MySQL/Redis       CREATE batch record (total_jobs = N)
2.  App             → Kafka jobs topic  PRODUCE N job messages
3.  JobConsumer     → worker            CALL perform(payload)
4a. (success)       → Kafka events      PRODUCE {batch_id, job_id, status: success}
4b. (failure)       → Kafka retry       PRODUCE {retry_after, retry_to, attempt+1}
4c. (exhausted)     → Kafka events      PRODUCE {batch_id, job_id, status: failed}
                    → Kafka DLT         PRODUCE original message + error context
5.  RetryConsumer   pauses partition    WAIT until retry_after via Karafka pause()
                    → Kafka jobs topic  PRODUCE message back to original topic
6.  EventConsumer   → MySQL/Redis       ATOMIC increment + check (FOR UPDATE / Lua)
7.  EventConsumer   → Kafka callbacks  PRODUCE callback message (if batch done)
8.  CallbackConsumer → MySQL/Redis      CLAIM via callback_dispatched_at CAS
                    → callback class    CALL on_success / on_complete
```

---

## Topic reference

| Topic (default name) | Produced by | Consumed by | Purpose |
|---|---|---|---|
| `kafka_batch.jobs` (per worker) | `Batch.create` / `Batch.enqueue` | `JobConsumer` | Individual job messages |
| `kafka_batch.jobs.retry` | `JobConsumer` | `RetryConsumer` | Failed jobs awaiting backoff |
| `kafka_batch.events` | `JobConsumer` | `EventConsumer` | Job completion signals |
| `kafka_batch.callbacks` | `EventConsumer` / `Reconciler` | `CallbackConsumer` | Batch-complete triggers |
| `kafka_batch.dead_letter` | `JobConsumer` / `CallbackConsumer` / `RetryConsumer` | Your consumer | Exhausted jobs + unresolvable callbacks |

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
