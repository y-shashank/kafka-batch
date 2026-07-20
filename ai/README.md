# kafka-batch AI knowledge base

> **Purpose.** Canonical corpus for the kafka-batch Web UI assistant (RAG). Covers architecture, every major subsystem, Redis/Kafka contracts, configuration knobs (Ruby gem + Go companion), SuperFetch, fairness, batches, retries, delayed jobs, **recurring cron**, and how each piece preserves **atomicity** and **idempotency**.
>
> **Assistant safety rule.** Answers must be derived from this knowledge base, `ai/FAQ.md`, and the **live configuration snapshot** (`config:live` / `topic_inventory`) when present. The assistant must **never** read, write, or mutate live Redis keys used by the batch ledger, fairness scheduler, workset, uniqueness locks, schedule index, **recurring leader lock**, liveness, consumption pause, retry-cancel, reconciler, or performance counters. Separate assistant Redis keys (API-key ciphertext, knowledge chunks, chat history) are not operational kafka-batch state. For live partition counts use `live_broker_partitions` from the snapshot — not DEFAULT_PARTITIONS documentation.

Companion repos:

| Repo | Role |
|------|------|
| [kafka-batch](https://github.com/y-shashank/kafka-batch) | Ruby client, Karafka control + Ruby execution, Web UI |
| [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go) | Go client, `kbatch daemon` (control), `kbatch worker` (Go execution) |

Both runtimes share **Kafka topics** and **Redis** contracts. Pick one language per tier; tiers talk only through Kafka + Redis.

**Critical deployment rule:** never run Go `kbatch daemon` and Ruby `{CG}-control` / `{CG}-dispatch-*` on the **same** events/retry/fair-ingest topics — that double-consumes. One control plane per cluster (or carefully partitioned topic sets).

---

## Table of contents

1. [What kafka-batch is](#1-what-kafka-batch-is)
2. [Non-negotiable pitfalls](#2-non-negotiable-pitfalls)
3. [Three-tier architecture](#3-three-tier-architecture)
4. [End-to-end data flow](#4-end-to-end-data-flow)
5. [Workers, job_type, HandlerRegistry, HandlerManifest](#5-workers-job_type-handlerregistry-handlermanifest)
6. [Ruby + Go mixed runtime](#6-ruby--go-mixed-runtime)
7. [Batches — create, open, seal, push, cancel](#7-batches--create-open-seal-push-cancel)
8. [EventConsumer — completion bitmaps and callback gates](#8-eventconsumer--completion-bitmaps-and-callback-gates)
9. [Callbacks — job callbacks vs legacy CallbackConsumer](#9-callbacks--job-callbacks-vs-legacy-callbackconsumer)
10. [SuperFetch — Claim, Mark, Perform](#10-superfetch--claim-mark-perform)
11. [Workset reclaim](#11-workset-reclaim)
12. [JobConsumer pipeline](#12-jobconsumer-pipeline)
13. [Fairness — Dispatcher, Scheduler, Forwarder, TenantPartitions](#13-fairness--dispatcher-scheduler-forwarder-tenantpartitions)
14. [Uniqueness](#14-uniqueness)
15. [Job expiry (`valid_till`)](#15-job-expiry-valid_till)
16. [Delayed jobs — SchedulePoller](#16-delayed-jobs--schedulepoller)
17. [Recurring (cron) scheduler](#17-recurring-cron-scheduler)
18. [Retries, RetryCancel, dead letter](#18-retries-retrycancel-dead-letter)
19. [Priority queues](#19-priority-queues)
20. [Cancellation and CancellationCache](#20-cancellation-and-cancellationcache)
21. [Consumption pause / resume](#21-consumption-pause--resume)
22. [Liveness](#22-liveness)
23. [Reconciler](#23-reconciler)
24. [Instrumentation, Metrics, PerformanceMetrics](#24-instrumentation-metrics-performancemetrics)
25. [Stores — Redis vs MySQL](#25-stores--redis-vs-mysql)
26. [Topics, partitions, create_all](#26-topics-partitions-create_all)
27. [Deployment, consumer groups, KB_ROLE, daemon_mode](#27-deployment-consumer-groups-kb_role-daemon_mode)
28. [Web UI and JSON API](#28-web-ui-and-json-api)
29. [Redis key namespace catalog](#29-redis-key-namespace-catalog)
30. [Kafka topic catalog](#30-kafka-topic-catalog)
31. [Ruby configuration reference](#31-ruby-configuration-reference)
32. [Go configuration reference](#32-go-configuration-reference)
33. [Ruby ↔ Go parity gaps](#33-ruby--go-parity-gaps)
34. [Atomicity and idempotency matrix](#34-atomicity-and-idempotency-matrix)
35. [Scaling and tuning](#35-scaling-and-tuning)
36. [Operator cheat sheet](#36-operator-cheat-sheet)
37. [Document maintenance](#37-document-maintenance)

---

## 1. What kafka-batch is

**Sidekiq Pro Batches on Kafka.** Same `on_success` / `on_complete` callback model, per-job retries, and batch completion counting — with Kafka as the durable job transport and Redis for coordination.

| Layer | Transport | Role |
|---|---|---|
| Job execution | Kafka worker topics | Ruby `JobConsumer` / Go `kbatch worker` |
| Completion counting | Kafka events + Redis bitmaps | `EventConsumer` / Go events processor |
| Callbacks | Kafka callbacks or app callback topics | `CallbackConsumer` or job callbacks |
| Batch state | Redis hash + bitmaps | Counters, status, callback claim |
| Fairness ordering | Kafka ingest → Redis WFQ → Kafka ready | Dispatcher + Forwarder |
| Delayed jobs | Kafka scheduled + Redis/MySQL index | SchedulePoller |
| Recurring cron | MySQL schedules + fire ledger | Ruby `Recurring::Ticker` / Go `pkg/cron` |
| In-flight execution ledger | Redis workset | SuperFetch claim / renew / complete / reclaim |

**Redis is always required.** There is no Redis-free mode.

**Guarantee slogan:** the framework makes **coordination** atomic and **counting** idempotent; the application makes **business effects** idempotent under at-least-once delivery.

---

## 2. Non-negotiable pitfalls

### 2.1 Jobs are at-least-once — workers must be idempotent

Kafka redelivers. Retries re-run handlers. SuperFetch reclaim re-produces orphans with `_reclaim: true`. Application handlers must tolerate duplicates.

### 2.2 Redis is always required

`config.redis_url` (Ruby) / `redis_url` (Go) is mandatory.

### 2.3 Callbacks are at-least-once

Dispatch claim is atomic (`HSETNX`), but the callback job/message is at-least-once. Deterministic callback `job_id` (`#{batch_id}:on_success` / `:on_complete`) helps application dedup.

### 2.4 Each Kafka topic belongs to exactly one consumer group

Priority YAML topics must not also be in flat `-jobs`. Boot validation rejects overlaps.

### 2.5 Partition count is fixed at topic creation

Size execution topics for peak pods × members/concurrency × SuperFetch concurrency.

### 2.6 One execution topic = one runtime

Never put Ruby and Go handlers on the same plain or priority topic. Fairness shares ingest; ready splits `.ruby` / `.go`.

### 2.7 One control plane per shared topic set

Do not run Go `kbatch daemon` and Ruby control/dispatch groups on the same events/retry/ingest topics.

### 2.8 Mega-batches still hit one Redis hash per batch

Shard huge batches in application policy if needed.

### 2.9 `fairness_weighted_concurrency = false` ignores weight ratios under load

Default `true` maps weights to in-flight share.

### 2.10 `fairness_lease_ttl` must exceed longest job

Expired leases free slots while jobs may still run → soft concurrency overshoot.

### 2.11 Schedule poller and recurring ticker off by default

Enable only on a few scheduler/control pods. Both `schedule_poller_enabled` and `recurring_scheduler_enabled` default **false**. Recurring also needs MySQL tables (§17) and Redis `kafka_batch:cron:leader_lock`.

### 2.12 Cancellation is eventually consistent

`cancellation_cache_ttl` (default 120s) across pods.

### 2.13 Priority is selection, not preemption

In-flight jobs are not killed.

### 2.14 Ruby SuperFetch default 1; Go default 10

MRI GVL vs Go true parallelism. Do not copy Go YAML SF=10 into Ruby blindly.

### 2.15 Uniqueness fails open on Redis errors

If Redis is down at claim time, enqueue proceeds without dedup (prefer availability).

### 2.16 Event emit failure does not schedule a job retry

If completion-event produce fails after perform, the offset stays uncommitted → job redelivery. Design handlers accordingly.

### 2.17 Legacy callbacks need Ruby CallbackConsumer

Go daemon **produces** legacy callback messages to `callbacks_topic` but does **not** consume them. Job-style callbacks run on execution topics instead.

---

## 3. Three-tier architecture

| Tier | Responsibility | Ruby | Go |
|------|----------------|------|-----|
| **1 — Client** | Produce jobs & batches | `KafkaBatch::Batch` | `pkg/client` |
| **2 — Control** | Fair forward, events, retry, callbacks produce, schedule, **recurring cron**, reclaim, reconcile | Karafka `-control`, `-dispatch-*` (+ optional Ruby recurring ticker) | `kbatch daemon` |
| **3 — Execution** | Run handlers, emit events | Karafka `JobConsumer` | `kbatch worker` |

```
┌─────────────────────────────────────────────────────────────────┐
│  Tier 1 — Client (enqueue Ruby + Go via job_type)               │
└───────────────────────────────┬─────────────────────────────────┘
                                │ produce
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Kafka + Redis (shared contracts — do not fork)                 │
└───────────────┬───────────────────────────────┬─────────────────┘
                ▼                               ▼
┌───────────────────────────┐   ┌──────────────┐  ┌──────────────┐
│ Tier 2 — Control          │   │ Tier 3 Ruby  │  │ Tier 3 Go    │
│ fair forward, events,     │   │ JobConsumer  │  │ kbatch worker│
│ retry, schedule, cron,    │   │ SuperFetch   │  │ SuperFetch   │
│ reclaim                   │   │              │  │              │
└───────────────────────────┘   └──────────────┘  └──────────────┘
```

Go CLI:

| Command | Role |
|---------|------|
| `kbatch daemon --config` | Control plane |
| `kbatch worker --config` | Go execution |
| `kbatch reconcile --config` | One-shot reconciler |
| `kbatch topics create/validate` | Topic provisioning |
| `kbatch version` | Version |

---

## 4. End-to-end data flow

```
App  →  Redis batch record FIRST (when batched)
     →  Kafka (plain topic | fair ingest | scheduled)
          → [fair path] Dispatcher → Redis WFQ → Forwarder → ready[.ruby|.go]
          → Execution SuperFetch: Claim → Kafka mark → perform pool
               ├─ success  → events → EventConsumer (bitmaps) → callbacks
               ├─ failure (retries left) → touch "executed" once → retry.{tier}
               ├─ exhausted → DLT + failed event
               └─ cancelled / expired → skip or DLT (+ failed event when batched)
Recurring cron (MySQL) → enqueue_job → same plain/fair/priority paths as above
SchedulePoller (delayed) → claim pointer → produce real topic → ack
```

| Worker kind | Client produces to | Executed from |
|-------------|-------------------|---------------|
| Plain Ruby | `kafka_topic` or `jobs_topic` | `{CG}-jobs` |
| Plain Go | manifest `topic` | `{CG}-go-worker-jobs` |
| Fair Ruby | ingest → `fair_*_ready.ruby` | `{CG}-jobs-fair-*` |
| Fair Go | ingest → `fair_*_ready.go` | `{CG}-go-worker-fair-ready-*` |
| Priority | YAML topics | `{CG}-<suffix>` or `{CG}-go-worker-<suffix>` |
| Delayed | `scheduled_topic` + index | re-produced when due |
| Fair retry | **ready** (skip ingest/WFQ) | same ready consumers |

---

## 5. Workers, job_type, HandlerRegistry, HandlerManifest

### Ruby Worker DSL

```ruby
class MyWorker
  include KafkaBatch::Worker

  job_type "orders.process"
  kafka_topic "orders.process"
  max_retries 5
  retry_tier :large
  fairness_type :time   # :time | :throughput
  uniq true

  retries_exhausted { |job, error| Alert.notify(job["job_id"], error.message) }

  def perform(payload)
    # must be idempotent
  end
end
```

### Handler resolution (JobConsumer)

1. Lookup by `job_type` in `HandlerRegistry`
2. Fall back to `worker_class` (`const_get` + auto-register)
3. Unknown → DLT + failed event (poison-pill safe)
4. Go runtime job on Ruby consumer → explicit error / DLT path

Default `job_type` derives from class name (`ProcessOrderWorker` → `"process_order"`).

### Manifest YAML

```yaml
handlers:
  segment.export:
    runtime: go
    topic: segment.exports
    max_retries: 25
    uniq: true
  orders.process:
    runtime: ruby
    worker_class: Orders::ProcessWorker
    topic: orders.process
  reports.rebuild:
    runtime: go
    fairness_type: time
```

Rules: YAML key = wire `job_type`; duplicate job_type → boot error; fair Go+Ruby hybrid requires split ready topics; Go worker validates every `runtime: go` entry is `kbatch.Register`ed.

### Wire envelope (excerpt)

```json
{
  "job_type": "orders.process",
  "worker_class": "ProcessOrderWorker",
  "job_id": "...",
  "batch_id": "...",
  "batch_seq": 1,
  "payload": {},
  "attempt": 0,
  "max_retries": 7,
  "tenant_id": "acme",
  "_uniq_fp": "...",
  "_reclaim": false,
  "valid_till": null,
  "batch_counted": false
}
```

Go handlers often use `worker_class: "go:{job_type}"` when manifest omits `worker_class`.

---

## 6. Ruby + Go mixed runtime

| Runtime | Declared in | Executed by |
|---------|-------------|-------------|
| `ruby` | Ruby Worker (+ optional manifest) | Karafka JobConsumer |
| `go` | Manifest only | `kbatch worker` |

Enqueue:

```ruby
KafkaBatch::Batch.enqueue(Orders::ProcessWorker, "order_id" => 42)
KafkaBatch::Batch.enqueue_job("segment.export", "segment_id" => 99)
KafkaBatch::Batch.create(...) do |b|
  b.push(Orders::ProcessWorker, "order_id" => 1)
  b.push_job("segment.export", "segment_id" => 42)
end
```

Configure `handler_manifest_path` / `KAFKA_BATCH_HANDLER_MANIFEST` on every process that enqueues or routes.

Partition hashing (murmur2) must agree across WaterDrop and franz-go for fairness co-partitioning — guarded by cross-runtime matrix tests.

---

## 7. Batches — create, open, seal, push, cancel

### Key files

Ruby: `lib/kafka_batch/batch.rb`, `stores/redis_store.rb`  
Go: `pkg/client`, `pkg/store`

### Lifecycle

1. **Create** — `CREATE_BATCH_LUA` / `HSETNX` on `id`; `total_jobs=0`; block form leaves unsealed (`locked_at` empty)
2. **Reserve / push** — `ADD_JOBS_LUA` increments `total_jobs`, allocates contiguous 1-based `batch_seq` via `kafka_batch:b:seq:{id}`
3. **Produce** to Kafka; on failure: rollback uniq + `add_jobs(-n)`
4. **Seal** — `SEAL_BATCH_LUA` sets `locked_at`, pre-sizes bitmaps; may early-complete
5. **Complete** via events (section 8)
6. **Cancel** — status `cancelled`; `CANCELLED_INDEX`; CancellationCache optimistic add

### Sidekiq-style completion semantics

| Concept | Meaning |
|---------|---------|
| Touch (`executed`) | First finish attempt — retry may still run |
| Success bitmap | Terminal success |
| Fail bitmap | Terminal failure only (not retry touch) |
| `on_complete` | `touched_count >= total` (may fire while retries run) |
| `on_success` | `completed_count >= total` |
| Terminal status | `completed + failed >= total` |

### Push / push_many (inside a batch)

- `push_many` / bulk: single `add_jobs`, chunked produce (`push_many_chunk_size` default 500) preserves gap-free `batch_seq`
- Push into completed/cancelled → `BatchClosedError`
- Block that raises still seals so already-pushed jobs can finalize

### Standalone bulk — `enqueue_many` / `perform_bulk` (no batch ledger)

For fire-and-forget throughput (no Redis batch hash, no completion events, no callbacks):

| API | Role |
|-----|------|
| `Batch.enqueue_many(Worker, payloads, tenant_id:, valid_till:)` | Chunked produce of N standalone jobs |
| `Worker.perform_bulk(payloads, …)` | Same, Sidekiq-style alias on the Worker |
| `Batch.enqueue_many_in` / `enqueue_many_at` | Delayed standalone bulk via schedule index |
| Go `EnqueueMany` / `EnqueueManyJobs` | Same shapes in kafka-batch-go |

**Do not confuse with** `Batch.create { b.push_many(...) }` — that creates a batch ledger and emits completion events. Use `enqueue_many` when you only need Kafka delivery (e.g. load tests, HelloWorker benches).

Optional `tenant_id:` applies to fair workers (ingest partitioning / WFQ). Chunk size follows `push_many_chunk_size`.

### meta vs callback_args

| Option | Stored | Passed to callback handler |
|--------|--------|----------------------------|
| `meta` | Yes | No (dashboard/API labels) |
| `callback_args` | Yes | Yes |

### Config

| Knob | Library default |
|------|-----------------|
| `batch_ttl` | 7 days |
| `all_index_max_size` | 200_000 |
| `push_many_chunk_size` | 500 |

### Atomicity notes

- Create-once via `HSETNX`
- Counters exactly-once per `batch_seq` (bitmaps)
- Kafka produce + schedule index are **not** a single atomic transaction — schedule reclaim covers poller crashes

---

## 8. EventConsumer — completion bitmaps and callback gates

### Purpose

Consume `events_topic`; apply `BATCH_DONE_JOB_LUA`; trigger callback dispatch when gates fire.

### Event payload

```json
{
  "batch_id": "...",
  "job_id": "...",
  "status": "success|failed|executed",
  "batch_seq": 1,
  "src_topic": "...",
  "src_partition": 0,
  "src_offset": 123,
  "occurred_at": "..."
}
```

Missing required fields → DLT (`incomplete_event`), not silent skip. Malformed JSON → DLT.

### Three bitmaps

| Key | Role |
|-----|------|
| `kafka_batch:b:bitmap:{id}` | Touch (first-execution) dedup |
| `kafka_batch:b:okbit:{id}` | Success dedup |
| `kafka_batch:b:failbit:{id}` | Terminal-fail dedup |

Bit index = `batch_seq - 1`.

### Lua return codes (conceptual)

- duplicate / not_found / invalid → no-op
- fire callbacks with outcome `success` | `success_only` | `complete`
- early `on_complete` only
- continue (no fire yet)

Callback pre-claim inside Lua: `HSETNX` on `complete_callback_dispatched_at` / `success_callback_dispatched_at`.

### Offset commit

Whole poll commits after store apply. Redelivery → bitmap dedup → may retry undispatched callbacks.

### Reconciler hook

EventConsumer hosts a background reconciler timer (`reconciliation_interval`).

### Idempotency

Exactly-once **counting** per `batch_seq`. Source offset fields are observability — primary dedup is bitmaps (legacy `kafka_batch:offsets:*` unused for counting).

---

## 9. Callbacks — job callbacks vs legacy CallbackConsumer

### Job callbacks (recommended)

`Callbacks::Dispatcher` produces a normal job to a user/manifest topic:

- Deterministic `job_id = "#{batch_id}:on_success"` or `:on_complete`
- Executed by Ruby JobConsumer or Go worker
- Fair work jobs need an **explicit** callback topic (never fair ingest)

### Legacy class callbacks

String class name → message on `callbacks_topic` → Ruby `CallbackConsumer`.

**Claim-before-invoke** via `CLAIM_CALLBACK_LUA` unless message has `preclaimed: true` (Go/Ruby events path already claimed in Lua).

### Outcome firing

| Outcome | Fires |
|---------|-------|
| `complete` / early complete | `on_complete` only |
| `success` | `on_success` + `on_complete` |
| `success_only` | `on_success` only |

### Go control note

Go daemon produces preclaimed legacy callback messages but **does not run CallbackConsumer**. Deploy Ruby control for legacy class callbacks, or use job callbacks exclusively.

### Errors

Callback errors → DLT (`callback`, `callback_error`, `malformed_callback`). At-least-once side effects if crash between claim and invoke.

---

## 10. SuperFetch — Claim, Mark, Perform

Always-on for Ruby and Go job execution (plain, priority, fair-ready). Control consumers stay synchronous.

### Protocol

```
Kafka poll
  → acquire claim_window slot
  → CLAIM_LUA (workset + fence)
  → claim lost? ack duplicate, release window
  → claim won → start lease renewer
  → Kafka mark/ack  (offsets advance at delivery rate)
  → acquire perform pool slot
  → #perform / Go handler
  → COMPLETE_LUA (fence-checked) OR leave for reclaim
  → release perform + claim_window
```

### Two limits

| Knob | Controls |
|------|----------|
| `super_fetch_concurrency` | Parallel perform slots |
| `super_fetch_claim_window` | Max Claimed∨Queued∨Performing (0 → 2× concurrency) |

Claim+ack gated on claim window so a full perform pool does not block rebalance forever. Renew starts at Claim so lease cannot expire while waiting for a perform slot.

### Fence token

Renew/complete require matching `consumer_id` + `fence`. Stolen/reclaimed jobs invalidate old fence → old performer must skip complete.

### Defaults

| Setting | Ruby | Go |
|---------|------|-----|
| `super_fetch_concurrency` | **1** | **10** |
| `super_fetch_claim_window` | 0 → 2× | 0 → 2× |
| `super_fetch_lease_ttl` | 120s | 120s |
| `super_fetch_orphan_grace` | 40s | 40s |
| `super_fetch_drain_timeout` | 30s | 30s |

Ruby production guidance: keep `karafka_concurrency × SF ≤ 10` unless load-tested. Raise SF only for IO-bound MRI work.

Go: members × SF = max concurrent performs per lane. ~50 SF per CPU core for long IO; 1–2 for CPU-heavy.

### Ruby SF knobs + env

| Option | Default | Env |
|--------|---------|-----|
| `super_fetch_concurrency` | 1 | `KAFKA_BATCH_SUPER_FETCH_CONCURRENCY` |
| `super_fetch_claim_window` | 0 | `KAFKA_BATCH_SUPER_FETCH_CLAIM_WINDOW` |
| `super_fetch_lease_ttl` | 120 | `KAFKA_BATCH_SUPER_FETCH_LEASE_TTL` |
| `super_fetch_orphan_grace` | 40 | `KAFKA_BATCH_SUPER_FETCH_ORPHAN_GRACE` |
| `super_fetch_reclaim_enabled` | true | disable via `KAFKA_BATCH_SUPER_FETCH_RECLAIM_DISABLED` |
| `super_fetch_reclaim_interval` | 30 | `KAFKA_BATCH_SUPER_FETCH_RECLAIM_INTERVAL` |
| `super_fetch_reclaim_limit` | 100 | `KAFKA_BATCH_SUPER_FETCH_RECLAIM_LIMIT` |
| `super_fetch_drain_timeout` | 30 | `KAFKA_BATCH_SUPER_FETCH_DRAIN_TIMEOUT` |
| `redis_pool_size` | auto ≥16 | `KAFKA_BATCH_REDIS_POOL_SIZE` |

Go exposes SF concurrency/claim window via env; lease/grace/reclaim interval often YAML-only.

---

## 11. Workset reclaim

Shared Redis contract (Ruby `Workset` ↔ Go `pkg/workset`) — do not fork.

| Key | Role |
|-----|------|
| `kafka_batch:work:job:{job_id}` | Claimed JSON + fence + TTL |
| `kafka_batch:work:by_consumer:{id}` | Job set per consumer |
| `kafka_batch:work:index` | ZSET by claim time |
| `kafka_batch:work:reclaiming:{job_id}` | Reclaim lock |
| `kafka_batch:work:produced:{job_id}` | Idempotent re-produce marker |
| `kafka_batch:live:consumer:{id}` | Heartbeat EXISTS for steal/reclaim |

**Run reclaim on ≥1 control plane** (Ruby `Workset::ReclaimScheduler` or Go daemon). Both may run (NX lock).

Path: find orphans (no live heartbeat past grace) → re-produce once with `_reclaim: true` → Finish. Reclaimed messages still Claim → mark → perform (at-least-once).

---

## 12. JobConsumer pipeline

Highlights after SuperFetch delivers a message to perform:

1. Resolve handler (`HandlerRegistry`)
2. Check cancellation cache → skip without event (unless retry-cancel path)
3. Check `valid_till` → expired handler (DLT + failed event if batched)
4. Fair slot: claim execution dedup; renew lease during long jobs; `complete(tenant, duration)` in ensure
5. `#perform` / handler
6. Success → emit success event (retries with `event_emit_retries`)
7. Failure with retries left → emit `executed` once (`batch_counted`), produce to retry tier topic with `retry_after`, `retry_to`
8. Exhausted → DLT + failed event; run `retries_exhausted` hook
9. Non-StandardError (e.g. fatal) → DLT backstop

**Event emit failure after success:** does **not** enqueue a job retry — leaves offset uncommitted for redelivery.

PriorityJobConsumer: rank 0 unconditional; lower ranks use `PriorityGate`; per-message yield.

ConsumptionGate prepended on consumers for pause/resume.

---

## 13. Fairness — Dispatcher, Scheduler, Forwarder, TenantPartitions

### Lanes

| Lane | vtime advances | Best for |
|------|----------------|----------|
| `:time` | at completion by `actual_seconds / weight` | Uneven runtimes |
| `:throughput` | at checkout by `1 / weight` | Similar runtimes |

### Flow

```
push → fair_*_ingest (tenant partition; one ingest topic per lane, both runtimes)
     → Dispatcher ({CG}-dispatch-{lane})
     → ENQUEUE_LUA (bounded ready list)
     → Forwarder checkout (lease ZSETs)
     → produce ready (.go / .ruby by handler runtime) with _fair_slot / _fair_type / _fair_slot_id
     → confirm forwarding HASH
     → JobConsumer (.ruby) / kbatch worker (.go)
```

Ready topics are **always runtime-split** (`fair_*_ready.go` / `fair_*_ready.ruby`). There is **no combined/non-suffixed `fair_*_ready` topic** — the Forwarder routes every job to the `.go` or `.ruby` ready topic by its handler runtime (unknown → `.ruby`). Ingest stays a single topic per lane (the Dispatcher is runtime-agnostic).

### Redis per lane (`kafka_batch:fair_{time|throughput}:*`)

| Suffix | Type | Role |
|--------|------|------|
| `ring` | ZSET | Ready tenants by vtime |
| `vtime` | HASH | Remembered virtual time |
| `weight` | HASH | Tenant weights (UI) |
| `ready:{tenant}` | LIST | Bounded window |
| `leases` | ZSET | Global in-flight leases |
| `lease:{tenant}` | ZSET | Per-tenant leases |
| `forwarding` | HASH | Staged until produce confirmed |
| `forwarding_meta` | HASH | slot_id → tenant |
| `slot_dedup:{slot_id}` | key | Ready redelivery dedup |
| `reclaim_lock` | lock | Forwarding recovery |

### Backpressure

Enqueue full → Dispatcher pauses ingest partition briefly; durable backlog stays in Kafka.

### Checkout

Lease ZSETs are authoritative in-flight count (self-healing on TTL). Weighted cap ≈ `floor(budget * w_t / Σw)`. Work-conserving two-pass selection. Complete removes lease by slot id (idempotent).

### Virtual-time (vtime) fairness & idle reset

`vtime` is the durable per-tenant fairness ledger: checkout always picks the ready tenant with the smallest vtime. Two safeguards keep it fair over time:

- **Min-vtime re-admission floor** (`ENQUEUE_LUA`): a tenant returning after being idle is re-admitted at `max(its stored vtime, current min ring vtime)`, so it enters at the active frontier and **cannot burst ahead** of tenants that stayed busy while it was gone.
- **Idle vtime reset** (`fairness_reset_vtime_when_idle`, default **true**): once a lane is fully quiescent — empty `ring`, no live `leases`, empty `forwarding`, and zero ingest lag — held for `fairness_vtime_idle_reset_debounce` (default **15s**), the Forwarder clears the `vtime` hash (weights preserved) via `RESET_VTIME_IF_QUIESCENT_LUA`. This gives **fresh per-active-period fairness** (a busy period never carries vtime debt/credit into the next) and **bounds unbounded vtime growth**. The DEL is atomic under a ring-empty guard, so a tenant re-enqueuing mid-check is never wiped. It never fires mid-run (no fixed-interval reset). Go: `Scheduler.ResetVtimeIfQuiescent` on the forwarder loop; Ruby: `Scheduler#reset_vtime_if_quiescent!` from `Forwarder#maybe_reset_vtime_idle`.

### TenantPartitions

| Mode | Behavior |
|------|----------|
| Dynamic (default) | Checkout exclusive partition from Redis free-pool |
| Pinned | `fairness_tenant_partitions` wins |
| Hash | Dynamic off → murmur2 (collisions possible) |

Keys: `kafka_batch:tenant_partitions:{lane}`, `:free`, `:partition_count`.

### Critical settings

| Setting | Ruby default | Go default | Notes |
|---------|--------------|------------|-------|
| `fairness_global_concurrency` | 50 | 50 | Install template often 1000 |
| `fairness_ready_window` | **500** | **100** | ⚠️ set Go to 500 for parity |
| `fairness_lease_ttl` | 1800 | 1800 | Install often 7200 |
| `fairness_weighted_concurrency` | true | true | |
| `fairness_dynamic_tenant_partitions` | true | true | |
| `fairness_min_ingest_partitions` | 2 | (internal) | README/ops often target 300 |
| `fairness_enabled` (Go only) | — | false | Must enable on Go daemon |

Time lane requires `fairness_global_concurrency > 0` **or** `fairness_max_inflight_per_tenant > 0` (Ruby boot validate).

Weights live in Redis per lane regardless of `config.store`. UI: `/weights/time`, `/weights/throughput`. Cache TTL `fairness_weight_cache_ttl` (60s).

Idle vtime reset: `fairness_reset_vtime_when_idle` (default true, both runtimes; Ruby env `KAFKA_BATCH_FAIRNESS_RESET_VTIME_WHEN_IDLE`, Go YAML `fairness_reset_vtime_when_idle`) and `fairness_vtime_idle_reset_debounce` (default 15s).

---

## 14. Uniqueness

`uniq true` on Worker / manifest (`uniq_enabled` master switch default true).

- Key: `kafka_batch:uniq:` + XXHash64 digest (16-byte dual hash; legacy 8-byte also released)
- Material: `worker_class + "\x00" + canonical JSON payload` (deep-sorted keys; HTML escape disabled for Oj/Go parity)
- Wire: `_uniq_fp` hex for release without re-hash
- Claim SETNX; release compare-and-delete Lua
- Duplicate → `nil` (`:skip`) or `DuplicateJobError` (`:raise`)
- TTL `uniq_lock_ttl` default 7 days
- **Fail-open** if Redis errors on claim

Cross-runtime matrix tests guard fingerprint parity including `<`, `>`, `&`, non-ASCII.

---

## 15. Job expiry (`valid_till`)

Optional ISO8601 `valid_till` on the message. Checked at JobConsumer, RetryConsumer, Fairness Dispatcher/Forwarder.

- Expired → DLT (`dlt_type: expired`) + failed completion event if batched
- Unparseable → treat as expired (poison-safe)

**Not the same as `enqueue_at`.** Expiry is a consumption-time gate on already-produced jobs; schedule poller is delayed dispatch.

---

## 16. Delayed jobs — SchedulePoller

### Flow

1. Payload → `scheduled_topic`
2. Pointer scored by run-at → `schedule_store` (`:redis` ZSET or `:mysql` table)
3. Control poller claims due (lease) → read Kafka by partition/offset → `route_for` → produce real topic → ack
4. Idle backoff to `schedule_poll_max_interval` with jitter; snap back when work appears
5. Reclaim expired leases periodically

### Redis schedule keys

| Key | Role |
|-----|------|
| `kafka_batch:sched:pending` | ZSET score=run_at, member=`job_id:partition:offset` |
| `kafka_batch:sched:inflight` | Lease until ack |
| `kafka_batch:sched:read_miss` | Poison offset counter |

MySQL: `kafka_batch_scheduled_jobs` when `schedule_store: :mysql`.

### Config

| Setting | Ruby default | Go default |
|---------|--------------|------------|
| `schedule_poller_enabled` | false | false |
| `schedule_poll_interval` | 5s | 5s |
| `schedule_poll_max_interval` | 60s | 60s |
| `schedule_poll_jitter` | **0.1** | **0** unless set | ⚠️ |
| `schedule_batch_size` | 100 | 100 |
| `schedule_lease_seconds` | 60 | 60 |
| `max_schedule_horizon` | 7 days | — |
| `schedule_index_write_retries` | 3 | — |

### Failure modes

- Payload missing (retention) → drop + ack
- Many read misses → drop
- Produce failure → leave leased → reclaim → duplicate-safe dispatch
- Cancelled batch → skip via CancellationCache

Enable only on few pods (`KB_ROLE=scheduler` / control with poller on).

**Not the same as recurring cron (§17).** Delayed jobs are one-shot `perform_in` / `perform_at` via `scheduled_topic` + pointer index. Recurring schedules live in MySQL cron tables and fire through the normal `enqueue_job` path.

---

## 17. Recurring (cron) scheduler

Whenever-style **repeating cron** that enqueues a **manifest `job_type`** on a schedule. Shared by Ruby (`KafkaBatch::Recurring::*`) and Go (`pkg/cron`) against the same MySQL tables and Redis leader lock.

### What it is / is not

| | Recurring (§17) | Delayed jobs (§16) |
|--|-----------------|-------------------|
| Trigger | Cron expression + timezone | Absolute `run_at` / relative delay |
| Storage | `kafka_batch_recurring_schedules` + `_fires` | `scheduled_topic` + Redis/MySQL pointer index |
| Dispatch | `Batch.enqueue_job` / Go `EnqueueJob` | Poller reads Kafka payload by offset |
| Idempotency | PK `(schedule_id, fire_at)` + deterministic `job_id` | Lease + reclaim on schedule index |
| Accuracy | Within one `recurring_window` (default 30s) | Poll interval + lease |

Does **not** run arbitrary code — only registered handlers (plain / fair / priority / uniq / retries / DLT all apply).

### Architecture

```
Dashboard Recurring::Store ──upsert──► kafka_batch_recurring_schedules
                                              ▲
Ruby Recurring::Ticker ──┐                    │
                         ├── Redis leader ──► Ledger claim_and_advance ──► enqueue_job
Go pkg/cron.Ticker ──────┘   lock             kafka_batch_recurring_fires
```

| Piece | Ruby | Go |
|-------|------|-----|
| Loop | `Recurring::Ticker` | `pkg/cron.Ticker` |
| Misfire plan | `Recurring::Planner` | `cron.PlanFires` |
| Claim + ledger | `Recurring::Ledger` | `Store.ClaimAndAdvance` |
| Leader lock | `Recurring::Lock` | `cron.Lock` |
| List / stale health | `Recurring::Reader` | `Store.List` + heartbeat |
| Dashboard CRUD | `Recurring::Store` + Web API | (UI is Ruby; Go daemon only fires) |
| Boot | Railtie when `recurring_scheduler_enabled` | `StartRecurringScheduler` when YAML true |

### Double-fire prevention (Go + Ruby together)

Safe to run **both** tickers against one cluster:

1. **Leader lock (optimization):** Redis `kafka_batch:cron:leader_lock` — SET NX EX + token-checked release. TTL = `recurring_lock_ttl` (default **60s**). Brief split-brain is OK.
2. **Fire ledger (correctness):** `kafka_batch_recurring_fires` PRIMARY KEY `(schedule_id, fire_at)`. `INSERT IGNORE` → second emit of the same instant is a no-op. Status: `pending` → `dispatched`.
3. **Deterministic job id:** `sched-{schedule_id}-{fire_at_unix_utc}` — recovery re-enqueue + uniq handlers stay idempotent. Go treats uniq-skip (`ErrJobSkipped`) as successful dispatch.

Enable on **few scheduler/control pods**, not every execution replica.

### Tick lifecycle (leader only, each `recurring_window`)

1. **`dispatch_due`** — `claim_and_advance(now, batch_size, planner)`:
   - `SELECT … WHERE enabled=1 AND next_run_at <= now FOR UPDATE SKIP LOCKED LIMIT recurring_batch_size` (default **100**)
   - Planner → `{fires[], new_next}` per schedule
   - INSERT IGNORE each fire; enqueue only **newly inserted** rows; update `next_run_at` / `last_fire_at`
   - **Poison** cron/tz: disable schedule (`enabled=0`), log error
2. **`recover`** (every `recurring_recover_every`, default **300s**): re-enqueue `pending` rows older than `recurring_recover_grace` (default **120s**) with the same `job_id`
3. **`prune`** (every `recurring_prune_every`, default **3600s**): delete `dispatched` rows older than `recurring_prune_retention` (default **7 days**)
4. **`heartbeat`** (every `recurring_heartbeat_every`, default **60s**): emit `cron.stale` / `cron.heartbeat` for enabled schedules past the stale threshold

Enqueue failure → row stays `pending` → recover retries. Uniq duplicate on enqueue → treat as dispatched.

### Misfire policies

| Policy | Behavior |
|--------|----------|
| **`fire_once`** (default) | Fire one instant at `next_run_at`, then advance to first instant **strictly after now** |
| **`skip`** | If lag ≤ `misfire_grace` → fire; else no fires; always advance past now |
| **`backfill`** | Fire every missed instant while `≤ now`, capped `recurring_max_backfill` per tick (default **1000**); remainder drains later (ledger dedups) |

`misfire_grace` default **60s**. Go `applyDefaults`: if grace ≤ 0 at runtime, uses `2 × window`.

### Stale detection / health

- Interval = gap between next two cron instants
- Threshold = `recurring_stale_factor × interval` (default factor **2.0**)
- Reference = `last_fire_at` else `next_run_at`; idle = `now - reference`
- **Stale** = enabled AND idle > threshold → UI health `stale`; else `ok` / `paused`
- Ruby interval needs **fugit**; without it, staleness is not computed (health ok/paused only)
- Go uses built-in 5-field cron parser

### Cron expressions

| Runtime | Parser |
|---------|--------|
| Ruby | **Fugit** (`Fugit::Cron.parse`, timezone appended) |
| Go | **5-field** min hour dom month dow + `@hourly` / `@daily` / `@weekly` / `@monthly` / `@yearly` |

Invalid cron on register → API 400; poison on tick → schedule disabled.

### MySQL schema

**Install:** `rails g kafka_batch:install --recurring && rails db:migrate`  
Migration: `db/migrate/20240101000005_create_kafka_batch_recurring_schedules.rb`  
Connection: `config.schedule_store_database_connection` (Ruby) / `recurring_mysql_dsn` falling back to `schedule_mysql_dsn` (Go).

Go `Store.EnsureSchema` can auto-create on boot; prefer explicit SQL in production (Go README appendix **block C**).

**`kafka_batch_recurring_schedules`:** `name` (unique upsert key, `\A[a-zA-Z0-9_.:-]{1,191}\z`), `cron_expr`, `timezone` (default UTC), `job_type` (manifest key), `args_json`, `tenant_id`, `enabled`, `misfire_policy`, `next_run_at`, `last_fire_at`, indexes `uq_name`, `idx_due(enabled, next_run_at)`.

**`kafka_batch_recurring_fires`:** PK `(schedule_id, fire_at)`, `status` (`pending`|`dispatched`), `job_id`, `created_at`, `dispatched_at`, index `idx_pending(status, created_at)`.

### Ruby config + env

| Knob | Default | Env |
|------|---------|-----|
| `recurring_scheduler_enabled` | false | `KAFKA_BATCH_RECURRING_SCHEDULER_ENABLED` (`1`/`true`/`yes`) |
| `recurring_window` | 30.0 s | `KAFKA_BATCH_RECURRING_WINDOW` |
| `recurring_lock_ttl` | 60 | `KAFKA_BATCH_RECURRING_LOCK_TTL` |
| `recurring_batch_size` | 100 | `KAFKA_BATCH_RECURRING_BATCH_SIZE` |
| `recurring_misfire_grace` | 60.0 | `KAFKA_BATCH_RECURRING_MISFIRE_GRACE` |
| `recurring_max_backfill` | 1000 | `KAFKA_BATCH_RECURRING_MAX_BACKFILL` |
| `recurring_recover_every` | 300.0 | `KAFKA_BATCH_RECURRING_RECOVER_EVERY` |
| `recurring_recover_grace` | 120.0 | `KAFKA_BATCH_RECURRING_RECOVER_GRACE` |
| `recurring_prune_every` | 3600.0 | `KAFKA_BATCH_RECURRING_PRUNE_EVERY` |
| `recurring_prune_retention` | 604800 (7d) | `KAFKA_BATCH_RECURRING_PRUNE_RETENTION` |
| `recurring_heartbeat_every` | 60.0 | `KAFKA_BATCH_RECURRING_HEARTBEAT_EVERY` |
| `recurring_stale_factor` | 2.0 | `KAFKA_BATCH_RECURRING_STALE_FACTOR` |

**Boot:** Karafka railtie starts an embedded thread when enabled. **Dedicated pod:** `rake kafka_batch:recurring:run` (loop) or `rake kafka_batch:recurring:tick` (one leader-gated pass).

### Go config

Same YAML key names and defaults. Extra: `recurring_mysql_dsn` (falls back to `schedule_mysql_dsn`). **No `KAFKA_BATCH_RECURRING_*` env overrides in Go `applyEnv` today** — YAML only. Enable with `recurring_scheduler_enabled: true` on the daemon.

### Web UI + API

Page: `/kafka_batch/recurring` (nav “Recurring”). Without migrated tables → `available: false`.

| Method | Path | Action |
|--------|------|--------|
| GET | `/api/recurring` | List + summary `{total,enabled,stale}` + health |
| POST | `/api/recurring` | Upsert by `name`: cron, job_type, timezone, args, tenant_id, misfire_policy, enabled |
| PATCH/POST | `/api/recurring/:name` | `{enabled: true\|false}` |
| DELETE | `/api/recurring/:name` | Delete |
| POST | `/api/recurring/:name/run` | Immediate one-shot enqueue (**no** ledger row) |

`job_type` accepts manifest key **or** Ruby worker class → canonicalized via `Manifest.ResolveJobType` / Go equivalent. Dashboard writes only mutate MySQL; ticker picks up within ≤ one window.

### Instrumentation

| Event | When |
|-------|------|
| `cron.fired` | Successful enqueue |
| `cron.enqueue_failed` | Enqueue error; left pending |
| `cron.stale` | Enabled schedule past stale threshold |
| `cron.heartbeat` | Sweep pulse `{enabled_count, stale_count, max_stale_seconds}` |

Mirrored in Go `pkg/instrument` for Datadog parity.

### Deployment

- Enable on 1–few control/scheduler pods (same pattern as §16 schedule poller)
- Needs **Redis** (leader lock) + **MySQL** (tables)
- Go-control clusters typically enable only the Go ticker; Ruby-control without Go enables the Ruby ticker; both together are safe
- Handlers must exist in the shared manifest on the workers that will execute them

---

## 18. Retries, RetryCancel, dead letter

### Tiers

| Tier | Default delay | Topic |
|------|---------------|-------|
| short | 30s | `{retry_topic}.short` |
| medium | 7m | `.medium` |
| large | 20m | `.large` |

Progression walks tiers by attempt; worker `retry_tier` can pin. Jitter ±`retry_jitter` (Ruby; Go code default 0.1 but may not be fully wired in scheduling — prefer explicit YAML awareness).

### RetryConsumer invariants

- Process in offset order; **stop** at first not-due message (never skip head of partition)
- Pause partition up to `retry_max_pause_seconds` / Go `retry_max_pause`
- Cancel/skip checked before pause
- Unroutable (no `retry_to`) → failed event + DLT

### RetryCancel (UI delete)

| Key | Role |
|-----|------|
| `kafka_batch:retry:cancel` | SET of job_ids |
| `kafka_batch:retry:skip` | HASH `topic:partition` → max offset watermark |

TTL `retry_cancel_ttl` default 7 days.

### DLT

Central `Dlt.publish` → `dead_letter_topic` (30-day retention at creation). Types include: `malformed_event`, `incomplete_event`, `callback*`, `retry_routing`, `expired`, `malformed_ingest`, `schedule_route_error`, poison jobs, etc. Optional Redis `kafka_batch:dlt:stats`.

`max_retries` library default **7** (install template often **3** — README essential table may match install, not library).

---

## 19. Priority queues

YAML groups (Sidekiq-style):

```yaml
consumer_group_suffix: jobs-fast
mode: weighted   # or strict
weighted_interleave: 4
topics: [p0, p1, p2]  # highest first
```

| Setting | Default |
|---------|---------|
| `priority_config_paths` | [] (+ env) |
| `priority_lag_check_interval` | 2s |
| `priority_weighted_interleave` | 4 |

Boot: topic in exactly one group; default `jobs_topic` forbidden in priority YAML; `fairness_type` workers bypass priority.

Gate uses Admin lag; **fail-open** if cluster unreachable. Topic-level pause excludes topic from lag gate so lower ranks keep flowing. **Not preemption.**

Go groups: `{CG}-go-worker-{suffix}` with `priority_consumer_concurrency` members (default 4).

---

## 20. Cancellation and CancellationCache

`Batch.cancel(id)` → status cancelled + cancelled index. Consumers with `skip_cancelled_jobs` (default true) skip when ID is in process-local cache.

| Setting | Default |
|---------|---------|
| `skip_cancelled_jobs` | true |
| `cancellation_cache_ttl` | 120s |
| `retry_cancel_ttl` | 7 days |

`CancellationCache`: refresh at most every TTL from Redis cancelled index; web cancel calls `add(id)` for immediate same-process effect. SchedulePoller also drops scheduled jobs for cancelled batches.

Eventually consistent across pods up to TTL.

---

## 21. Consumption pause / resume

Redis:

| Key | Members |
|-----|---------|
| `kafka_batch:consumption:topics` | `group\x1ftopic` |
| `kafka_batch:consumption:partitions` | `group\x1ftopic\x1fpartition` |

Consumers (`ConsumptionGate`) refresh every `consumption_control_refresh_interval` (30s). UI reads fresh. MySQL fallback table when `store: :mysql` and Redis down. Go mirrors via franz-go PauseFetch so paused topics do not advance offsets.

---

## 22. Liveness

| Key | Role |
|-----|------|
| `kafka_batch:live:consumer:{id}` | Heartbeat JSON + TTL |
| `kafka_batch:live:job:{consumer}:{job}` | Running job detail |

| Setting | Default |
|---------|---------|
| `liveness_backend` (Ruby) | `:redis` or `:off` |
| `liveness_ttl` | 180s |
| `liveness_heartbeat_interval` | 20s |
| `liveness_stats_interval` (Ruby) | 15s (0 disables RSS/CPU) |
| `track_running_jobs` | true |

Circuit breaker on Redis errors; best-effort; never fails hot path. Feeds SuperFetch orphan detection.

Go: SuperFetch forces Redis heartbeats even if `liveness_enabled: false`; HTTP `/health`+`/live` only when enabled (`:8080` default). HTTP stale ≈ `3 × heartbeat_interval`.

---

## 23. Reconciler

Recovers:

1. Stuck **running** — sealed, `completed+failed >= total`, status still running
2. **Lost callback** — terminal status, callback timestamps null

| Setting | Default |
|---------|---------|
| `reconciliation_interval` | 300s |
| `reconciler_lock_ttl` | 600s |
| `max_reconcile_per_run` | 100 |

Lock: `kafka_batch:b:reconciler_lock`. Summary: `kafka_batch:reconciler:last`, `:last_skip`. Skips open (unsealed) and genuinely in-progress batches.

Triggers: EventConsumer/daemon background; `rake kafka_batch:reconcile`; `kbatch reconcile`.

---

## 24. Instrumentation, Metrics, PerformanceMetrics

### Instrumentation

ActiveSupport::Notifications pattern `*.kafka_batch` (Ruby). Go `pkg/instrument` mirrors events: `job.processed/retried/failed`, `batch.*`, `callback.*`, `dlt.published`, `workset.reclaimed`, `super_fetch.drained`, `scheduled.*`, `reconciler.ran`, etc.

### Metrics export

Opt-in `metrics_enabled`. Adapters StatsD/Datadog/proc (Ruby) or StatsD (Go). You supply the client. Prefix `kafka_batch`.

### Performance dashboard metrics

Opt-in Redis minute buckets shared by Ruby + Go for Web UI `/performance`:

`kafka_batch:perf:min:{epoch}:{processed|failed|retried|reclaimed}`

Fields: `_all`, per `job_type`, `_other` overflow.

| Knob | Default |
|------|---------|
| `performance_metrics_enabled` | false |
| `performance_metrics_retention` | 86400 |
| `performance_metrics_max_job_types` | 50 |
| `performance_metrics_bucket_seconds` | 60 |
| `performance_metrics_sample_rate` | 1.0 |

Best-effort; never raises into hot path.

---

## 25. Stores — Redis vs MySQL

### `config.store`

| Concern | `:redis` | `:mysql` |
|---------|----------|----------|
| Batch ledger / bitmaps | Redis | **Still Redis** (MysqlStore delegates ledger) |
| Failure log | none (DLT + retry topics) | `kafka_batch_failures` |
| Pause state | Redis | MySQL fallback |
| Migrations | none | required |

### `config.schedule_store` (independent)

| `:redis` | `:mysql` |
|----------|----------|
| ZSET index | `kafka_batch_scheduled_jobs` |

### Recurring cron tables (independent of `store`; MySQL required)

| Table | Role |
|-------|------|
| `kafka_batch_recurring_schedules` | Cron definitions (see §17) |
| `kafka_batch_recurring_fires` | Fire ledger PK `(schedule_id, fire_at)` |

Ruby binds via `schedule_store_database_connection`. Go uses `recurring_mysql_dsn` (fallback `schedule_mysql_dsn`). Install: `rails g kafka_batch:install --recurring` or Go README SQL block C / `EnsureSchema`.

Hot batch counters are **never** in SQL by design.

---

## 26. Topics, partitions, create_all

Ruby `KafkaBatch::Topics` / rake `kafka_batch:create_topics` / `kafka_batch:topics` (dry-run).

Go: `kbatch topics create|validate`.

### DEFAULT_PARTITIONS (Ruby)

These are **create_topics defaults only** — not necessarily what exists on a live cluster.
Kafka cannot shrink partitions; ops often create smaller topics for local/dev (e.g. 10).

| Category | Create default |
|----------|----------------|
| jobs / priority | 16 |
| events | 16 |
| callbacks | 16 |
| retry per tier | 16 |
| scheduled | 16 |
| dead_letter | 16 |
| fair ingest per lane | 64 |
| fair ready per lane (`.go` / `.ruby`) | 64 |

Every category defaults to **16** partitions except the fairness **ingest** and **ready** lanes, which default to **64**. Env: `REPLICATION_FACTOR` (default **1**), `PARTITIONS` uniform override. Fair ready topics are always runtime-split (`.go` / `.ruby`) — there is no combined `fair_*_ready` topic to create.

Scheduled retention ≥ `max_schedule_horizon` (+1 day buffer). DLT retention 30 days at creation. Existing topics skipped, never altered.

**Manifest plain Go topics** are not always auto-discovered by Ruby `create_topics` — create them explicitly or via Go topics CLI.

### Live broker inventory (AI + ops)

On AI knowledge sync (NX-locked, at most every 24h with the config snapshot), `KafkaBatch::Topics.inventory` merges:

| Field | Meaning |
|-------|---------|
| `live_broker_partitions` / `broker_partitions` | Actual count from `Karafka::Admin.cluster_info` |
| `create_default_partitions` / `configured_partitions` | `DEFAULT_PARTITIONS` / create_topics intent |
| `status` | `matches_default` / `differs_from_default` / `missing_on_broker` / `broker_unavailable` |

Always includes both fairness lanes’ ingest + ready (`.ruby` / `.go`) even when Worker classes are not loaded (UI-only pods). Stored in Redis `kafka_batch:ai:knowledge:config` → `topic_inventory` and the `config:live` RAG chunk.

**Assistant rule:** when asked “how many partitions does topic X have?”, answer from `live_broker_partitions`. Never report 768 / DEFAULT_PARTITIONS as the live count unless broker metadata is unavailable (then say so).

---

## 27. Deployment, consumer groups, KB_ROLE, daemon_mode

`CG` = `consumer_group` (default `kafka-batch`; with prefix `myapp` → `myapp.kafka-batch`).

### Ruby groups (`draw_routes`)

| Group | Tier | Consumers |
|-------|------|-----------|
| `{CG}-control` | Control | Event, Callback, Retry |
| `{CG}-dispatch-{lane}` | Control | Fairness::Dispatcher |
| `{CG}-jobs-fair-{lane}` | Execution | JobConsumer on ruby ready |
| `{CG}-{priority-suffix}` | Execution | PriorityJobConsumer |
| `{CG}-jobs` | Execution | plain + non-priority manifest topics |

`daemon_mode` / `KAFKA_BATCH_DAEMON_MODE` skips `draw_routes` entirely (API enqueue-only).

### Go groups

| Group | Role |
|-------|------|
| `{CG}-events` / retry / dispatch (daemon) | Control |
| `{CG}-go-worker-jobs` | Plain Go jobs |
| `{CG}-go-worker-fair-ready-{lane}` | Fair Go ready |
| `{CG}-go-worker-{priority-suffix}` | Priority Go |

### KB_ROLE (schedule poller / recurring helper — does not filter Karafka groups by itself)

| Role | Poller / recurring pattern |
|------|----------------------------|
| `all`, `scheduler` | schedule poller on (generated initializer); also set `recurring_scheduler_enabled` on those pods when using cron |
| other | poller off unless `KB_SCHEDULE_POLLER=true`; recurring off unless `KAFKA_BATCH_RECURRING_SCHEDULER_ENABLED=true` / YAML |

Filter groups explicitly: `karafka server --include-consumer-groups ...`.

### Layouts

All-in-one (dev), standalone (small prod), split (scale). Mixed: Ruby or Go control **xor** per topic set; Ruby exec + Go exec in parallel on different topics.

---

## 28. Web UI and JSON API

Mount `KafkaBatch::Web` at `/kafka_batch` behind host auth.

### Pages

`/`, `/batches/:id`, `/lag`, `/live`, `/weights/*`, `/fairness/*`, `/failures`, `/dead_letter`, `/scheduled`, `/recurring`, `/reconciler`, `/system`, `/audit`, `/performance`, `/ai`

Live refresh: localStorage `kafka_batch_live`, 5s.

### Dashboard metrics (`GET /api/dashboard`)

| Field | Meaning |
|-------|---------|
| `counts` / `total` | Batch status counters from Redis ledger |
| `pending_jobs` | Untouched jobs in **running batches** (ledger) — UI label “Pending in batches” |
| `topic_pending` | Sum of Kafka consumer-group lag across gem topics **excluding** scheduled log-archive rows — UI label “Jobs pending” (links to `/lag`) |
| `liveness` | Live consumers + running jobs when liveness is on |

Do not conflate `pending_jobs` (batch ledger) with `topic_pending` (Kafka lag). Fair ingest + ready can both count the same logical job mid-pipeline in the lag sum.

### System page

Read-only `KafkaBatch::SystemInfo` sections: Overview, Kafka, Redis, MySQL (if store), SuperFetch, Uniqueness, Liveness, Fairness, Scheduled jobs, Retry, Reconciliation, Cancellation, Priority, Retention, Performance metrics, Instrumentation metrics, Audit, AI, optional rdkafka overrides. Secrets masked. Recurring schedules are managed on `/recurring` (not a dedicated SystemInfo block today).

### AI assistant

Settings + shared chat at `/ai`. RAG over packaged `knowledge_chunks.json` + live config snapshot (`config:live`). OpenRouter key encrypted in Redis (`kafka_batch:ai:settings`). Never touches operational ledger/fairness/workset/cron-lock keys.

### Mutating API (CSRF cookie `_kb_csrf` + `X-CSRF-Token`)

Batch cancel/delete/bulk; lag pause/resume; weights set/reset; retries delete/delete_all; recurring upsert/pause/resume/delete/run-now (§17); AI settings/chat/history.

Optional `web_authenticator`, `audit_enabled` (MySQL audit table). Secrets masked on `/system`.

---

## 29. Redis key namespace catalog

| Prefix / pattern | Subsystem |
|------------------|-----------|
| `kafka_batch:b:{id}` | Batch hash |
| `kafka_batch:b:seq:{id}` | batch_seq allocator |
| `kafka_batch:b:bitmap:{id}` | Touch bitmap |
| `kafka_batch:b:okbit:{id}` | Success bitmap |
| `kafka_batch:b:failbit:{id}` | Fail bitmap |
| `kafka_batch:b:reconciler_lock` | Reconciler lock |
| `kafka_batch:index:running` | Running ZSET |
| `kafka_batch:index:done` | Done / callback-pending ZSET |
| `kafka_batch:index:all` | UI listing ZSET |
| `kafka_batch:index:cancelled` | Cancelled ZSET |
| `kafka_batch:counts` | Status counters HASH |
| `kafka_batch:uniq:{digest}` | Uniqueness locks |
| `kafka_batch:sched:pending` | Schedule index |
| `kafka_batch:sched:inflight` | Schedule leases |
| `kafka_batch:sched:read_miss` | Schedule read failures |
| `kafka_batch:cron:leader_lock` | Recurring ticker leader lease (§17) |
| `kafka_batch:fair_time:*` / `fair_throughput:*` | WFQ per lane |
| `kafka_batch:tenant_partitions:{lane}` (+ `:free`, `:partition_count`) | Dynamic partitions |
| `kafka_batch:work:job:*` | SuperFetch workset |
| `kafka_batch:work:by_consumer:*` | Workset by consumer |
| `kafka_batch:work:index` | Workset ZSET |
| `kafka_batch:work:reclaiming:*` | Reclaim state |
| `kafka_batch:work:produced:*` | Re-produce dedup |
| `kafka_batch:live:job:*` / `live:consumer:*` | Liveness |
| `kafka_batch:consumption:topics` / `partitions` | Pause state |
| `kafka_batch:retry:cancel` / `retry:skip` | Retry UI cancel |
| `kafka_batch:reconciler:last` / `last_skip` | Sweep summary |
| `kafka_batch:perf:min:*` | Performance metrics |
| `kafka_batch:dlt:stats` | DLT stats |

Assistant must not touch these operational keys.

---

## 30. Kafka topic catalog

Defaults with empty prefix:

| Topic | Group / reader | Consumer |
|-------|----------------|----------|
| `kafka_batch.jobs` | `{CG}-jobs` | JobConsumer |
| `kafka_batch.events` | `{CG}-control` / Go events | EventConsumer |
| `kafka_batch.callbacks` | `{CG}-control` | CallbackConsumer (Ruby) |
| `kafka_batch.jobs.retry.{tier}` | control | RetryConsumer |
| `kafka_batch.scheduled` | SchedulePoller (no CG) | poller |
| `kafka_batch.dead_letter` | UI reader | — |
| `kafka_batch.fair_*_ingest` | `{CG}-dispatch-*` | Dispatcher |
| `kafka_batch.fair_*_ready.ruby` | `{CG}-jobs-fair-*` | JobConsumer |
| `kafka_batch.fair_*_ready.go` | Go fair-ready | kbatch worker |
| Custom / priority / manifest | respective groups | Job/Priority/Go |

---

## 31. Ruby configuration reference

All on `KafkaBatch.config`. Library `initialize` defaults below; install generator may ship larger production values.

### Connection / identity / topics

`brokers`, `redis_url`/`redis`, `redis_pool_size`, `topic_prefix`, `consumer_group`, `jobs_topic`, `events_topic`, `callbacks_topic`, `dead_letter_topic`, `retry_topic`, `scheduled_topic`, all `fair_*` topic accessors, `extra_job_topics`, `jobs_topics`, `handler_manifest_path`, `validate_topics_on_boot`, `topics_replication_factor`, `daemon_mode`, `producer_config`, `consumer_config`, `logger`, `web_authenticator`.

### Store / retention

`store`, `store_database_connection`, `batch_ttl`, `all_index_max_size`, `failures_ttl`, `max_failures_per_batch`, `retry_cancel_ttl`.

### SuperFetch / uniq / cancel / liveness / consumption

See sections 10, 14, 20, 22, 21.

### Retry

| Option | Default |
|--------|---------|
| `max_retries` | 7 |
| `retry_jitter` | 0.1 |
| `retry_tiers` | short 30 / medium 420 / large 1200 |
| `retry_tier_progression` | short→medium→large |
| `retry_max_pause_seconds` | 30 |
| `event_emit_retries` | 3 |
| `event_emit_backoff` | 1 |

### Schedule / fairness / priority / reconciler / producer / audit / metrics / recurring

See respective sections. Notable fairness defaults: `fairness_global_concurrency=50`, `fairness_ready_window=500`, `fairness_lease_ttl=1800`, `fairness_weighted_concurrency=true`, `fairness_dynamic_tenant_partitions=true`, `fairness_min_ingest_partitions=2`, `fairness_dispatcher_batch_size=50`, `fairness_dispatcher_concurrency=5`, `fairness_forwarder_idle_sleep=0.05`, `fairness_forwarding_recovery_grace=5.0`, `fairness_slot_dedup_ttl=0`, `fairness_active_count_ttl=5`, `fairness_active_count_source=:inflight_plus_ready`, `fairness_weight_cache_ttl=60`, `fairness_default_weight=1.0`, `fairness_max_inflight_per_tenant=0`, `fairness_reset_vtime_when_idle=true`, `fairness_vtime_idle_reset_debounce=15`.

### Recurring cron (full table in §17)

| Option | Default |
|--------|---------|
| `recurring_scheduler_enabled` | false (env `KAFKA_BATCH_RECURRING_SCHEDULER_ENABLED`) |
| `recurring_window` | 30.0 |
| `recurring_lock_ttl` | 60 |
| `recurring_batch_size` | 100 |
| `recurring_misfire_grace` | 60.0 |
| `recurring_max_backfill` | 1000 |
| `recurring_recover_every` | 300.0 |
| `recurring_recover_grace` | 120.0 |
| `recurring_prune_every` | 3600.0 |
| `recurring_prune_retention` | 604800 |
| `recurring_heartbeat_every` | 60.0 |
| `recurring_stale_factor` | 2.0 |

Also: `schedule_store_database_connection` (shared with MySQL delayed-job index + recurring tables).

### Env overrides (selected)

`KAFKA_BATCH_SUPER_FETCH_*`, `KAFKA_BATCH_LIVENESS_*`, `KAFKA_BATCH_REDIS_POOL_SIZE`, `KAFKA_BATCH_DAEMON_MODE`, `KAFKA_BATCH_HANDLER_MANIFEST`, `KAFKA_BATCH_PRIORITY_CONFIG(S)`, `KAFKA_BATCH_FAIRNESS_DYNAMIC_TENANT_PARTITIONS`, `KAFKA_BATCH_PERFORMANCE_METRICS_*`, `KAFKA_BATCH_RECURRING_*` (all twelve knobs in §17), `KB_ROLE`, `KB_SCHEDULE_POLLER`.

---

## 32. Go configuration reference

Aligned with [kafka-batch-go README appendix](https://github.com/y-shashank/kafka-batch-go) and `DefaultDaemon()` / `LoadDaemon` / `applyEnv`.

### Load order

`DefaultDaemon()` → YAML file → `${VAR}` / `${VAR:-default}` expansion → field merge → `applyEnv()` → `prefixTopics()`.

Most numeric YAML values only override when **positive** (zeros often mean “keep default”).

### Kafka & identity

| Key | Default |
|-----|---------|
| `brokers` | `["localhost:9092"]` |
| `topic_prefix` | `""` |
| `consumer_group` | `kafka-batch` |
| `node_id` | hostname#pid |
| `handler_manifest` | path string |
| `jobs_topics` | derived from manifest if empty |
| `redis_url` | `redis://localhost:6379/0` |

### Core topics

| Key | Default |
|-----|---------|
| `events_topic` | `kafka_batch.events` |
| `callbacks_topic` | `kafka_batch.callbacks` |
| `dead_letter_topic` | `kafka_batch.dead_letter` |
| `retry_topic` | `kafka_batch.jobs.retry` (base; tiers append `.short`/`.medium`/`.large`) |

### Retry / producer / fetch

| Key | Default |
|-----|---------|
| `max_retries` | 7 |
| `retry_tiers` | short 30 / medium 420 / large 1200 (seconds) |
| `retry_max_pause` | 30 s |
| `producer_required_acks` | `all_isr` (`leader` allowed) |
| `consumer_fetch_max_bytes` | 1048576 (1 MiB) |
| `consumer_fetch_max_partition_bytes` | 131072 (128 KiB) |
| `consumer_fetch_max_wait_ms` | 200 |
| `consumer_stall_timeout` | 90 s |

**Code-only (not YAML today):** `retry_jitter` 0.1, `event_emit_retries` 3, `event_emit_backoff` 1s, `batch_ttl` 7d, `retry_progression` short→medium→large.

### Worker throughput (`kbatch worker`)

| Key | Default |
|-----|---------|
| `jobs_consumer_concurrency` | 8 |
| `fair_ready_consumer_concurrency` | 8 |
| `priority_consumer_concurrency` | 4 |
| `super_fetch_concurrency` | 10 |
| `super_fetch_claim_window` | 0 → **2×** concurrency |
| `super_fetch_lease_ttl` | 120 s |
| `super_fetch_reclaim_interval` | 30 s |
| `super_fetch_reclaim_limit` | 100 |
| `super_fetch_orphan_grace` | 40 s |
| `super_fetch_drain_timeout` | 30 s |
| `execution_mode` | `superfetch` (`watermark` advanced — Redis-free offset watermark; requires idempotent handlers; one mode per topic) |

### Delayed jobs (SchedulePoller)

| Key | Default | Notes |
|-----|---------|-------|
| `schedule_poller_enabled` | false | Scheduler/control pods only |
| `scheduled_topic` | `kafka_batch.scheduled` | |
| `schedule_store` | empty → **redis** | `mysql` needs SQL block A |
| `schedule_mysql_dsn` | | env `KAFKA_BATCH_SCHEDULE_MYSQL_DSN` |
| `schedule_poll_interval` | 5 s | |
| `schedule_poll_max_interval` | 60 s | |
| `schedule_poll_jitter` | **0** | ⚠️ set `0.1` for Ruby parity |
| `schedule_batch_size` | 100 | |
| `schedule_lease_seconds` | 60 | |
| `schedule_reclaim_interval` | 30 s | |

### Recurring cron (see §17)

| Key | Default |
|-----|---------|
| `recurring_scheduler_enabled` | false |
| `recurring_mysql_dsn` | falls back to `schedule_mysql_dsn` |
| `recurring_window` | 30 s |
| `recurring_lock_ttl` | 60 s |
| `recurring_batch_size` | 100 |
| `recurring_misfire_grace` | 60 s |
| `recurring_max_backfill` | 1000 |
| `recurring_recover_every` | 300 s |
| `recurring_recover_grace` | 120 s |
| `recurring_prune_every` | 3600 s |
| `recurring_prune_retention` | 604800 s (7d) |
| `recurring_heartbeat_every` | 60 s |
| `recurring_stale_factor` | 2.0 |

No `KAFKA_BATCH_RECURRING_*` env vars in Go today — YAML only.

### Priority

| Key | Default |
|-----|---------|
| `priority_config_paths` | `[]` |
| `priority_lag_check_interval` | 2 s |
| `priority_weighted_interleave` | 4 |

### Fairness

| Key | Default | Notes |
|-----|---------|-------|
| `fairness_enabled` | **false** | Must set true for fair dispatch |
| `fairness_time_ingest` / `_ready` / `_ready_go` / `_ready_ruby` | `kafka_batch.fair_time_*` | |
| `fairness_throughput_ingest` / `_ready` / `_ready_go` / `_ready_ruby` | `kafka_batch.fair_throughput_*` | |
| `fairness_ready_window` | **100** | ⚠️ Ruby 500 — set 500 for parity |
| `fairness_global_concurrency` | 50 | |
| `fairness_max_inflight_per_tenant` | 0 | 0 = dynamic share |
| `fairness_lease_ttl` | 1800 s | |
| `fairness_default_weight` | 1.0 | |
| `fairness_weighted_concurrency` | true | |
| `fairness_active_count_ttl` | 5 s | |
| `fairness_active_count_source` | `inflight_plus_ready` | or `inflight` |
| `fairness_reset_vtime_when_idle` | **true** | Clear vtime ledger (weights kept) when a lane goes fully idle |
| `fairness_vtime_idle_reset_debounce` | 15 s | Lane must stay idle this long before the reset fires |
| `fairness_dynamic_tenant_partitions` | true | |
| `fairness_tenant_partition_cache_ttl` | 30 s | |
| `fairness_tenant_partitions` | map | static pins win |

Idle vtime reset is YAML-only in Go (no env var). In Ruby the same two keys exist plus env `KAFKA_BATCH_FAIRNESS_RESET_VTIME_WHEN_IDLE`. Both runtimes default it on; behavior is identical (atomic ring-empty-guarded DEL, fires at most once per idle period, weights preserved).

### Store / cancel / consumption / reconciler

| Key | Default |
|-----|---------|
| `store` | redis (`mysql` → SQL block B) |
| `store_mysql_dsn` | env `KAFKA_BATCH_STORE_MYSQL_DSN` |
| `skip_cancelled_jobs` | true |
| `cancellation_cache_ttl` | 120 s |
| `consumption_control_refresh_interval` | 30 s |
| `reconciliation_interval` | 300 s |
| `reconciler_lock_ttl` | 600 s |
| `max_reconcile_per_run` | 100 |

### Liveness / metrics / performance

| Key | Default |
|-----|---------|
| `liveness_enabled` | false |
| `liveness_http_addr` | `:8080` |
| `liveness_ttl` | 180 s |
| `liveness_heartbeat_interval` | 20 s |
| `track_running_jobs` | true |
| `metrics_enabled` | false |
| `metrics_prefix` | `kafka_batch` |
| `metrics_statsd_addr` | |
| `performance_metrics_enabled` | false |
| `performance_metrics_retention` | 86400 s |
| `performance_metrics_max_job_types` | 50 |
| `performance_metrics_bucket_seconds` | 60 |
| `performance_metrics_sample_rate` | 1.0 |
| `redis_rtt_probe_interval` | 15 s |
| `redis_rtt_probe_timeout` | 0.2 s |

### Env overrides (`applyEnv` — complete list)

`KAFKA_BROKERS`, `KAFKA_PREFIX`, `REDIS_URL`, `KAFKA_BATCH_HANDLER_MANIFEST`, `KAFKA_BATCH_SCHEDULE_MYSQL_DSN`, `KAFKA_BATCH_STORE_MYSQL_DSN`, `KAFKA_BATCH_PRIORITY_CONFIG`, `KAFKA_BATCH_PRIORITY_CONFIGS`, `KAFKA_BATCH_METRICS_ENABLED`, `KAFKA_BATCH_METRICS_PREFIX`, `KAFKA_BATCH_METRICS_STATSD_ADDR`, `KAFKA_BATCH_LIVENESS_ENABLED`, `KAFKA_BATCH_LIVENESS_HTTP_ADDR`, `KAFKA_BATCH_LIVENESS_TTL`, `KAFKA_BATCH_LIVENESS_HEARTBEAT_INTERVAL`, `KAFKA_BATCH_PERFORMANCE_METRICS_ENABLED`, `KAFKA_BATCH_PERFORMANCE_METRICS_RETENTION`, `KAFKA_BATCH_PERFORMANCE_METRICS_MAX_JOB_TYPES`, `KAFKA_BATCH_PERFORMANCE_METRICS_BUCKET_SECONDS`, `KAFKA_BATCH_PERFORMANCE_METRICS_SAMPLE_RATE`, `KAFKA_BATCH_REDIS_RTT_PROBE_INTERVAL`, `KAFKA_BATCH_REDIS_RTT_PROBE_TIMEOUT`, `KAFKA_BATCH_RETRY_MAX_PAUSE`, `KAFKA_BATCH_PRODUCER_REQUIRED_ACKS`, `KAFKA_BATCH_JOBS_CONSUMER_CONCURRENCY`, `KAFKA_BATCH_FAIR_READY_CONSUMER_CONCURRENCY`, `KAFKA_BATCH_PRIORITY_CONSUMER_CONCURRENCY`, `KAFKA_BATCH_SKIP_CANCELLED_JOBS`, `KAFKA_BATCH_FAIRNESS_DYNAMIC_TENANT_PARTITIONS`, `KAFKA_BATCH_CANCELLATION_CACHE_TTL`, `KAFKA_BATCH_SUPER_FETCH_CONCURRENCY`, `KAFKA_BATCH_SUPER_FETCH_CLAIM_WINDOW`, `KAFKA_BATCH_EXECUTION_MODE`, `KAFKA_BATCH_CONSUMER_FETCH_MAX_BYTES`, `KAFKA_BATCH_CONSUMER_FETCH_MAX_PARTITION_BYTES`, `KAFKA_BATCH_CONSUMER_FETCH_MAX_WAIT_MS`, `KAFKA_BATCH_CONSUMER_STALL_TIMEOUT`.

MySQL DSNs: go-sql-driver form **or** `mysql2://` / `mysql://` URLs.

### Handler manifest (`kafka_batch_handlers.yml`)

| Field | Purpose |
|-------|---------|
| `runtime` | `go` \| `ruby` (required) |
| `worker_class` | Ruby constant (required if ruby) |
| `topic` | Plain job topic (default jobs topic) |
| `apply_topic_prefix` | Prepend `topic_prefix` |
| `max_retries` | Override daemon default |
| `retry_tier` | `short` \| `medium` \| `large` |
| `fairness_type` | `time` \| `throughput` (no plain topic) |
| `uniq` | Fingerprint dedupe |

YAML map key = wire `job_type`. Recurring schedules must reference these keys (or a resolvable worker class name).

### MySQL DDL (Go-only bootstrap)

| Block | Tables | When |
|-------|--------|------|
| **A** | `kafka_batch_scheduled_jobs` | `schedule_store: mysql` |
| **B** | `kafka_batch_failures`, `kafka_batch_consumption_pauses` | `store: mysql` |
| **C** | `kafka_batch_recurring_schedules`, `kafka_batch_recurring_fires` | recurring enabled |

Full SQL + annotated `daemon.yml` live in the Go README appendix. Batch ledger / uniq / fair weights **always Redis**.

Control plane scales by **pods + partitions** (one franz-go client per group per pod; events fan out per partition).

---

## 33. Ruby ↔ Go parity gaps

| Setting | Ruby | Go | Action |
|---------|------|-----|--------|
| `super_fetch_concurrency` | 1 | 10 | Intentional; do not copy blindly |
| `fairness_ready_window` | 500 | 100 | Set Go YAML to 500 for parity |
| `schedule_poll_jitter` | 0.1 | 0 unless set | Set Go `0.1` |
| Fairness enable | per-worker | `fairness_enabled` | Must set true on Go daemon |
| Legacy CallbackConsumer | Ruby control | Go produces only | Need Ruby for legacy class callbacks |
| SF lease/grace/reclaim env | many `KAFKA_BATCH_SUPER_FETCH_*` | often YAML-only | Set in daemon.yml |
| `fairness_min_ingest_partitions` | 2 (warn) | internal | Ops often size ingest to 300 |
| `max_retries` docs | README table may say 3 | both library 7 | Prefer Configuration / YAML |
| Recurring env overrides | all `KAFKA_BATCH_RECURRING_*` | **YAML only** | Set daemon.yml for Go |
| Cron parser | Fugit (needs gem) | built-in 5-field + macros | Prefer portable 5-field exprs |
| Recurring UI writes | Ruby Web API | — | Dashboard is Ruby even under Go control |
| Recurring auto-schema | migrate `--recurring` | `EnsureSchema` on boot | Prefer explicit SQL in prod |

---

## 34. Atomicity and idempotency matrix

| Feature | Atomic primitive | Idempotency expectation |
|---------|------------------|-------------------------|
| Batch create | `HSETNX` Lua | Create-once |
| Add jobs / seal | Lua | Reject after terminal |
| Job completion count | Three bitmaps + counters in one Lua | Re-delivered events do not double-count |
| Callback dispatch claim | `HSETNX` | One dispatcher; handler at-least-once |
| SuperFetch claim | `CLAIM_LUA` + fence | Lost claim → ack duplicate |
| SuperFetch complete | Fence `COMPLETE_LUA` | Stolen fence → no-op |
| Workset reclaim | NX + `work:produced:` | At-most-once produce; at-least-once perform |
| Fairness enqueue | Window Lua | Full → pause (no silent drop) |
| Fairness checkout | Lease ZSET Lua | Complete by slot id idempotent |
| Fairness forward | Forwarding HASH | Crash-safe pop→produce |
| Uniqueness | SETNX + compare-del | Duplicate enqueue skipped/raised; fail-open on Redis error |
| Schedule dispatch | Lease + reclaim | At-least-once due dispatch |
| Recurring fire | `(schedule_id, fire_at)` INSERT IGNORE + `sched-{id}-{unix}` job id | At-most-once emit per instant; at-least-once perform (Kafka) |
| Reconciler | `SET NX EX` | Single sweeper |
| Job `#perform` | — | **Always application-idempotent** |
| Job callbacks | Deterministic `job_id` | **Always application-idempotent** |

---

## 35. Scaling and tuning

1. Partitions ≥ peak_pods × (Karafka concurrency or Go members) × SuperFetch concurrency.
2. Ruby: prefer pods over huge SF for CPU; keep product ≤ 10 in prod unless tested.
3. Go: members ≈ partitions; SF ~50×cores for IO; 1–2×cores for CPU.
4. `fairness_lease_ttl` and `super_fetch_lease_ttl` above longest job (+ renew margin).
5. Schedule poller **and** recurring ticker only on few control/scheduler pods.
6. Workset reclaim on ≥1 control plane sharing Redis.
7. Autoscale on lag; partitions fixed.
8. Cap/shard mega-batches.
9. Extreme throughput: `track_running_jobs = false` if per-job `/live` unused.
10. Never share execution topic across Ruby and Go.
11. One control plane per topic set.
12. Size fair ingest partitions for active tenants when using exclusive dynamic partitions.
13. Size `redis_pool_size` before raising Ruby SF.
14. Fair ready backlog with ingest OK → raise `fairness_global_concurrency` or reduce oversubscribed workers.

### Go tuning sketch (32 partitions, IO, 4-core)

```yaml
jobs_consumer_concurrency: 32
fair_ready_consumer_concurrency: 32
priority_consumer_concurrency: 8
super_fetch_concurrency: 200
producer_required_acks: all_isr
fairness_enabled: true
fairness_ready_window: 500
schedule_poll_jitter: 0.1
```

---

## 36. Operator cheat sheet

```bash
# Topics
REPLICATION_FACTOR=1 PARTITIONS=12 bundle exec rake kafka_batch:create_topics
bundle exec rake kafka_batch:topics
kbatch topics create --brokers ... --manifest ... --include-control

# Recurring MySQL (pick one)
bundle exec rails generate kafka_batch:install --recurring && rails db:migrate
# or copy SQL block C from kafka-batch-go README appendix

# Recurring ticker (Ruby dedicated pod)
KAFKA_BATCH_RECURRING_SCHEDULER_ENABLED=true bundle exec rake kafka_batch:recurring:run
# one-shot leader-gated pass:
bundle exec rake kafka_batch:recurring:tick

# Reconciler
bundle exec rake kafka_batch:reconcile
kbatch reconcile --config daemon.yml

# Dev all-in-one Ruby
KB_ROLE=all bundle exec karafka server

# API only
KAFKA_BATCH_DAEMON_MODE=1 bundle exec puma

# Go control + worker (enable recurring in daemon.yml)
kbatch daemon --config config/daemon.yml
kbatch worker --config config/daemon.yml
```

Typical env: `KAFKA_BROKERS`, `REDIS_URL`, `KAFKA_PREFIX`, `KB_ROLE`, `KAFKA_BATCH_DAEMON_MODE`, `KAFKA_BATCH_HANDLER_MANIFEST`, `KAFKA_BATCH_PRIORITY_CONFIG(S)`, `KAFKA_BATCH_RECURRING_SCHEDULER_ENABLED`, `KAFKA_BATCH_SCHEDULE_MYSQL_DSN` / `KAFKA_BATCH_STORE_MYSQL_DSN`.

---

## 37. Document maintenance

When adding a feature or knob:

1. Update the matching section here (behavior + keys + knobs + atomicity row).
2. Add Q&A in `ai/FAQ.md`.
3. Note Ruby↔Go parity gaps if defaults diverge.
4. Rebuild packaged chunks: `bin/build_ai_chunks` → `lib/kafka_batch/ai/knowledge_chunks.json`.
5. Commit markdown + JSON; ship a new gem version so `corpus_version` changes.

Full release checklist: **`lib/kafka_batch/ai/README.md`**.

### Packaged chunks + Redis sync (boot)

Editable source lives in `ai/*.md`. Clients do **not** re-chunk markdown at boot.

| Artifact | Role |
|----------|------|
| `lib/kafka_batch/ai/knowledge_chunks.json` | Prebuilt chunks (shipped in the gem) |
| `kafka_batch:ai:knowledge:chunks` | Redis HASH id → chunk JSON |
| `kafka_batch:ai:knowledge:config` | Live masked config snapshot |
| `kafka_batch:ai:knowledge:meta` | `corpus_version`, `config_refreshed_at`, … |
| `kafka_batch:ai:knowledge:lock` | Short NX lock during write only |

**Many UI pods:** every pod calls `sync!` after Rails init; only one writer wins the lock.

| Data | Refresh rule |
|------|----------------|
| Knowledge chunks | Only when packaged `corpus_version` ≠ Redis meta |
| Config snapshot + topic inventory + routing | At most every **24 hours** on boot (`config_refreshed_at`); NX lock so only one pod writes. Includes masked knobs, `Topics.inventory` (broker partitions), and `Ai::RoutingSnapshot` (handler manifest + priority queue YAML). |

```ruby
config.ai_knowledge_enabled = true
# ENV: KAFKA_BATCH_AI_KNOWLEDGE_ENABLED=false to disable
```

Force full re-sync: `FORCE=1 bundle exec rake kafka_batch:sync_ai_knowledge`

Live RAG chunk (`config:live`) is injected first for chat. Prefer:
- `live_broker_partitions` over DEFAULT_PARTITIONS docs for partition counts
- AUTHORITATIVE LIVE ROUTING (handlers / priority groups) over example YAML in docs

This file intentionally expands beyond the root README so the assistant has a self-contained corpus without live cluster operational state.


---

## 38. Batch hash fields (Redis)

Typical fields on `kafka_batch:b:{id}` (names may evolve; treat as operational contract mirrored in Go store):

| Field | Role |
|-------|------|
| `id` | Batch id (HSETNX create sentinel) |
| `total_jobs` | Reserved/pushed job count |
| `touched_count` | First-execution count |
| `completed_count` | Terminal success count |
| `failed_count` | Terminal fail count |
| `status` | `running` / `success` / `complete` / `cancelled` / … |
| `locked_at` | Seal timestamp (empty while open/block populating) |
| `on_success` / `on_complete` | Serialized callback descriptors |
| `meta` / `callback_args` / `description` | App metadata |
| `tenant_id` | Default fair tenant for pushes |
| `complete_callback_dispatched_at` | HSETNX claim stamp |
| `success_callback_dispatched_at` | HSETNX claim stamp |
| `callback_dispatched_at` | Legacy/any claim stamp |
| `created_at` / other timestamps | Observability |

Indexes: `index:running`, `index:done`, `index:all`, `index:cancelled`; aggregate `kafka_batch:counts`.

---

## 39. Instrumentation event catalog (Ruby)

Subscribe via ActiveSupport::Notifications (`*.kafka_batch`) or Metrics bridge. Notable events:

| Event | When |
|-------|------|
| `job.processed` | Successful perform |
| `job.retried` | Scheduled onto retry tier |
| `job.failed` | Terminal failure / exhausted |
| `job.cancelled` | Skipped due to cancel |
| `job.uniq_skipped` | Duplicate enqueue skipped |
| `job.expired` | `valid_till` gate |
| `batch.created` / `batch.sealed` / `batch.completed` | Lifecycle |
| `callback.dispatched` / related | Callback path |
| `dlt.published` | Dead letter write |
| `consumer.priority_yielded` | Priority gate yielded |
| `reconciler.ran` | Sweep finished |
| `workset.reclaimed` | Orphan reclaim sweep |
| `super_fetch.drained` | Graceful shutdown drain |
| `scheduled.claimed` / related | Schedule poller |
| `cron.fired` / `cron.enqueue_failed` / `cron.stale` / `cron.heartbeat` | Recurring ticker (§17) |

Go `pkg/instrument` mirrors the cross-runtime subset (including `workset.reclaimed`, `super_fetch.drained`, `cron.*`).

---

## 40. Client produce safety and partial failure

### Ordering invariant
Redis batch mutations that reserve `batch_seq` happen **before** or tightly around Kafka produce. On produce failure, client attempts:

1. Release uniqueness locks for unproduced items
2. Decrement/reserve rollback via `add_jobs(-n)` where applicable

### Schedule index
If Kafka scheduled produce succeeds but index write fails: `schedule_index_write_retries` with linear backoff; may raise `PartialProduceError`. Poller reclaim cannot find undindexed jobs — treat as operator incident.

### max_message_bytes
Encoded payload size guard (default 1 MiB) raises `ProducerError` before broker reject.

### push_many chunking
Chunks preserve contiguous `batch_seq` while pipelining `produce_many_sync`.

---

## 41. Fairness Lua responsibilities (summary)

| Script | Role |
|--------|------|
| `ENQUEUE_LUA` | Bound window; admit tenant to ring; RPUSH ready list |
| `CHECKOUT_LUA_COUNT` | Throughput lane: advance vtime at dispatch; lease slots |
| Time checkout / `COMPLETE_LUA_TIME_LEASE` | Time lane: advance vtime at completion by duration/weight |
| Tenant partition `CHECKOUT_LUA` | Exclusive partition assignment from free pool |
| Forward confirm / reclaim paths | Clear forwarding HASH; recover stale forwards after grace |
| `RESET_VTIME_IF_QUIESCENT_LUA` | Clear vtime hash (weights kept) iff ring empty + no live leases + empty forwarding |

In-flight is **derived from lease ZSETs**, not a brittle counter — expired leases self-heal concurrency budget after crashes.

---

## 42. ConsumptionGate and PriorityGate interaction

1. **ConsumptionGate** — if topic/partition paused in Redis snapshot, consumer does not process (refresh ≤30s).
2. **PriorityGate** — lower ranks check higher-topic lag; paused higher topics count as inactive so lower ranks proceed.
3. SuperFetch still applies inside allowed consumes.

---

## 43. Empty and edge batches

| Case | Behavior |
|------|----------|
| Seal with `total_jobs=0` | Immediate success path |
| Open unsealed batch | Cannot finalize until seal |
| Events for unknown/cancelled batch | Lua duplicate/ignore |
| Mega-batch | One hash + three bitmaps; shard in app policy |

---

## 44. Go client API mirror

| Go API | Ruby analogue |
|--------|----------------|
| `client.New` | configured `KafkaBatch` |
| `CreateBatch` | `Batch.create` |
| `OpenBatch` | `Batch.open` |
| `PushJob` / `PushJobAt` | `push` / `push_at` / `push_job` |
| `Seal` / `Cancel` | `seal!` / `cancel` |
| `EnqueueJob` / `EnqueueJobAt` | `enqueue` / `enqueue_at` / `enqueue_job*` |

BatchOptions: OnSuccess, OnComplete, Meta, CallbackArgs, Description, TenantID. PushOptions: JobID, TenantID, ValidTill.

---

## 45. README drift vs library defaults (operators)

| Topic | Library default | Often documented / install |
|-------|-----------------|----------------------------|
| `max_retries` | 7 | 3 in install / some tables |
| `fairness_global_concurrency` | 50 | 1000 install / fairness examples |
| `fairness_lease_ttl` | 1800 | 7200 install |
| `fairness_min_ingest_partitions` | 2 | 300 ops target |
| Bitmaps | touch + ok + fail | Older docs mentioned only touch |
| Completion dedup | `batch_seq` bitmaps | Not Kafka offset keys |

When answering “what is the default?”, prefer `Configuration#initialize` / Go `DefaultDaemon` over marketing tables.
