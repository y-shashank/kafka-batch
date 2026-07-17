# kafka-batch AI knowledge base

> **Purpose.** Canonical corpus for the kafka-batch Web UI assistant (RAG). Covers architecture, every major subsystem, Redis/Kafka contracts, configuration knobs (Ruby gem + Go companion), SuperFetch, fairness, batches, retries, delayed jobs, and how each piece preserves **atomicity** and **idempotency**.
>
> **Assistant safety rule.** Answers must be derived from this knowledge base, `ai/FAQ.md`, and the **live configuration snapshot** (`config:live` / `topic_inventory`) when present. The assistant must **never** read, write, or mutate live Redis keys used by the batch ledger, fairness scheduler, workset, uniqueness locks, schedule index, liveness, consumption pause, retry-cancel, reconciler, or performance counters. Separate assistant Redis keys (API-key ciphertext, knowledge chunks, chat history) are not operational kafka-batch state. For live partition counts use `live_broker_partitions` from the snapshot — not DEFAULT_PARTITIONS documentation.

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
17. [Retries, RetryCancel, dead letter](#17-retries-retrycancel-dead-letter)
18. [Priority queues](#18-priority-queues)
19. [Cancellation and CancellationCache](#19-cancellation-and-cancellationcache)
20. [Consumption pause / resume](#20-consumption-pause--resume)
21. [Liveness](#21-liveness)
22. [Reconciler](#22-reconciler)
23. [Instrumentation, Metrics, PerformanceMetrics](#23-instrumentation-metrics-performancemetrics)
24. [Stores — Redis vs MySQL](#24-stores--redis-vs-mysql)
25. [Topics, partitions, create_all](#25-topics-partitions-create_all)
26. [Deployment, consumer groups, KB_ROLE, daemon_mode](#26-deployment-consumer-groups-kb_role-daemon_mode)
27. [Web UI and JSON API](#27-web-ui-and-json-api)
28. [Redis key namespace catalog](#28-redis-key-namespace-catalog)
29. [Kafka topic catalog](#29-kafka-topic-catalog)
30. [Ruby configuration reference](#30-ruby-configuration-reference)
31. [Go configuration reference](#31-go-configuration-reference)
32. [Ruby ↔ Go parity gaps](#32-ruby--go-parity-gaps)
33. [Atomicity and idempotency matrix](#33-atomicity-and-idempotency-matrix)
34. [Scaling and tuning](#34-scaling-and-tuning)
35. [Operator cheat sheet](#35-operator-cheat-sheet)
36. [Document maintenance](#36-document-maintenance)

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

### 2.11 Schedule poller off by default

Enable only on a few scheduler/control pods.

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
| **2 — Control** | Fair forward, events, retry, callbacks produce, schedule, reclaim, reconcile | Karafka `-control`, `-dispatch-*` | `kbatch daemon` |
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
│ retry, schedule, reclaim  │   │ SuperFetch   │  │ SuperFetch   │
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
push → fair_*_ingest (tenant partition)
     → Dispatcher ({CG}-dispatch-{lane})
     → ENQUEUE_LUA (bounded ready list)
     → Forwarder checkout (lease ZSETs)
     → produce ready with _fair_slot / _fair_type / _fair_slot_id
     → confirm forwarding HASH
     → JobConsumer / kbatch worker
```

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

---

## 17. Retries, RetryCancel, dead letter

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

## 18. Priority queues

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

## 19. Cancellation and CancellationCache

`Batch.cancel(id)` → status cancelled + cancelled index. Consumers with `skip_cancelled_jobs` (default true) skip when ID is in process-local cache.

| Setting | Default |
|---------|---------|
| `skip_cancelled_jobs` | true |
| `cancellation_cache_ttl` | 120s |
| `retry_cancel_ttl` | 7 days |

`CancellationCache`: refresh at most every TTL from Redis cancelled index; web cancel calls `add(id)` for immediate same-process effect. SchedulePoller also drops scheduled jobs for cancelled batches.

Eventually consistent across pods up to TTL.

---

## 20. Consumption pause / resume

Redis:

| Key | Members |
|-----|---------|
| `kafka_batch:consumption:topics` | `group\x1ftopic` |
| `kafka_batch:consumption:partitions` | `group\x1ftopic\x1fpartition` |

Consumers (`ConsumptionGate`) refresh every `consumption_control_refresh_interval` (30s). UI reads fresh. MySQL fallback table when `store: :mysql` and Redis down. Go mirrors via franz-go PauseFetch so paused topics do not advance offsets.

---

## 21. Liveness

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

## 22. Reconciler

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

## 23. Instrumentation, Metrics, PerformanceMetrics

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

## 24. Stores — Redis vs MySQL

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

Hot batch counters are **never** in SQL by design.

---

## 25. Topics, partitions, create_all

Ruby `KafkaBatch::Topics` / rake `kafka_batch:create_topics` / `kafka_batch:topics` (dry-run).

Go: `kbatch topics create|validate`.

### DEFAULT_PARTITIONS (Ruby)

These are **create_topics defaults only** — not necessarily what exists on a live cluster.
Kafka cannot shrink partitions; ops often create smaller topics for local/dev (e.g. 10).

| Category | Create default |
|----------|----------------|
| jobs / priority / ready | 768 |
| events | 48 |
| callbacks | 6 |
| retry per tier | 12 |
| scheduled | 48 |
| dead_letter | 3 |
| fair ingest per lane | 300 |

Env: `REPLICATION_FACTOR` (default 3), `PARTITIONS` uniform override.

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

## 26. Deployment, consumer groups, KB_ROLE, daemon_mode

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

### KB_ROLE (schedule poller helper — does not filter Karafka groups by itself)

| Role | Poller |
|------|--------|
| `all`, `scheduler` | on (generated initializer patterns) |
| other | off unless `KB_SCHEDULE_POLLER=true` |

Filter groups explicitly: `karafka server --include-consumer-groups ...`.

### Layouts

All-in-one (dev), standalone (small prod), split (scale). Mixed: Ruby or Go control **xor** per topic set; Ruby exec + Go exec in parallel on different topics.

---

## 27. Web UI and JSON API

Mount `KafkaBatch::Web` at `/kafka_batch` behind host auth.

### Pages

`/`, `/batches/:id`, `/lag`, `/live`, `/weights/*`, `/fairness/*`, `/failures`, `/dead_letter`, `/scheduled`, `/reconciler`, `/system`, `/audit`, `/performance`, `/ai`

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

Read-only `KafkaBatch::SystemInfo` sections: Overview, Kafka, Redis, MySQL (if store), SuperFetch, Uniqueness, Liveness, Fairness, Scheduled jobs, Retry, Reconciliation, Cancellation, Priority, Retention, Performance metrics, Instrumentation metrics, Audit, AI, optional rdkafka overrides. Secrets masked.

### AI assistant

Settings + shared chat at `/ai`. RAG over packaged `knowledge_chunks.json` + live config snapshot (`config:live`). OpenRouter key encrypted in Redis (`kafka_batch:ai:settings`). Never touches operational ledger/fairness/workset keys.

### Mutating API (CSRF cookie `_kb_csrf` + `X-CSRF-Token`)

Batch cancel/delete/bulk; lag pause/resume; weights set/reset; retries delete/delete_all; AI settings/chat/history.

Optional `web_authenticator`, `audit_enabled` (MySQL audit table). Secrets masked on `/system`.

---

## 28. Redis key namespace catalog

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

## 29. Kafka topic catalog

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

## 30. Ruby configuration reference

All on `KafkaBatch.config`. Library `initialize` defaults below; install generator may ship larger production values.

### Connection / identity / topics

`brokers`, `redis_url`/`redis`, `redis_pool_size`, `topic_prefix`, `consumer_group`, `jobs_topic`, `events_topic`, `callbacks_topic`, `dead_letter_topic`, `retry_topic`, `scheduled_topic`, all `fair_*` topic accessors, `extra_job_topics`, `jobs_topics`, `handler_manifest_path`, `validate_topics_on_boot`, `topics_replication_factor`, `daemon_mode`, `producer_config`, `consumer_config`, `logger`, `web_authenticator`.

### Store / retention

`store`, `store_database_connection`, `batch_ttl`, `all_index_max_size`, `failures_ttl`, `max_failures_per_batch`, `retry_cancel_ttl`.

### SuperFetch / uniq / cancel / liveness / consumption

See sections 10, 14, 19, 21, 20.

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

### Schedule / fairness / priority / reconciler / producer / audit / metrics

See respective sections. Notable fairness defaults: `fairness_global_concurrency=50`, `fairness_ready_window=500`, `fairness_lease_ttl=1800`, `fairness_weighted_concurrency=true`, `fairness_dynamic_tenant_partitions=true`, `fairness_min_ingest_partitions=2`, `fairness_dispatcher_batch_size=50`, `fairness_dispatcher_concurrency=5`, `fairness_forwarder_idle_sleep=0.05`, `fairness_forwarding_recovery_grace=5.0`, `fairness_slot_dedup_ttl=0`, `fairness_active_count_ttl=5`, `fairness_active_count_source=:inflight_plus_ready`, `fairness_weight_cache_ttl=60`, `fairness_default_weight=1.0`, `fairness_max_inflight_per_tenant=0`.

### Env overrides (selected)

`KAFKA_BATCH_SUPER_FETCH_*`, `KAFKA_BATCH_LIVENESS_*`, `KAFKA_BATCH_REDIS_POOL_SIZE`, `KAFKA_BATCH_DAEMON_MODE`, `KAFKA_BATCH_HANDLER_MANIFEST`, `KAFKA_BATCH_PRIORITY_CONFIG(S)`, `KAFKA_BATCH_FAIRNESS_DYNAMIC_TENANT_PARTITIONS`, `KAFKA_BATCH_PERFORMANCE_METRICS_*`, `KB_ROLE`, `KB_SCHEDULE_POLLER`.

---

## 31. Go configuration reference

Load `daemon.yml` → `DefaultDaemon` → YAML → `applyEnv` → `prefixTopics`. Values support `${VAR}` / `${VAR:-default}`.

### Shared env

`KAFKA_BROKERS`, `REDIS_URL`, `KAFKA_PREFIX`, `KAFKA_BATCH_HANDLER_MANIFEST`, `KAFKA_BATCH_SCHEDULE_MYSQL_DSN`, `KAFKA_BATCH_STORE_MYSQL_DSN`, `KAFKA_BATCH_PRIORITY_CONFIG(S)`, metrics/liveness/performance env vars, consumer fetch/acks/stall, SF concurrency/claim window, cancellation TTL, jobs/fair/priority consumer concurrency.

MySQL DSNs accept go-sql-driver form or `mysql2://` / `mysql://` URLs.

### Go-only knobs

| Knob | Default | Purpose |
|------|---------|---------|
| `jobs_consumer_concurrency` | 8 | In-process members plain jobs |
| `fair_ready_consumer_concurrency` | 8 | Members per fair-ready lane |
| `priority_consumer_concurrency` | 4 | Members per priority group |
| `producer_required_acks` | `all_isr` | or `leader` |
| `consumer_fetch_max_bytes` | 1 MiB | |
| `consumer_fetch_max_partition_bytes` | 128 KiB | |
| `consumer_fetch_max_wait_ms` | 200 | |
| `consumer_stall_timeout` | 90s | Force reconnect on stall |
| `fairness_enabled` | false | Daemon gate for fair dispatch |
| `liveness_enabled` | false | HTTP health (Redis HB still on for SF) |
| `liveness_http_addr` | `:8080` | |

Control plane scales by **pods + partitions** (one franz-go client per group per pod; events fan out per partition).

---

## 32. Ruby ↔ Go parity gaps

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

---

## 33. Atomicity and idempotency matrix

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
| Reconciler | `SET NX EX` | Single sweeper |
| Job `#perform` | — | **Always application-idempotent** |
| Job callbacks | Deterministic `job_id` | **Always application-idempotent** |

---

## 34. Scaling and tuning

1. Partitions ≥ peak_pods × (Karafka concurrency or Go members) × SuperFetch concurrency.
2. Ruby: prefer pods over huge SF for CPU; keep product ≤ 10 in prod unless tested.
3. Go: members ≈ partitions; SF ~50×cores for IO; 1–2×cores for CPU.
4. `fairness_lease_ttl` and `super_fetch_lease_ttl` above longest job (+ renew margin).
5. Schedule poller only on few control/scheduler pods.
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

## 35. Operator cheat sheet

```bash
# Topics
REPLICATION_FACTOR=1 PARTITIONS=12 bundle exec rake kafka_batch:create_topics
bundle exec rake kafka_batch:topics
kbatch topics create --brokers ... --manifest ...

# Reconciler
bundle exec rake kafka_batch:reconcile
kbatch reconcile --config daemon.yml

# Dev all-in-one Ruby
KB_ROLE=all bundle exec karafka server

# API only
KAFKA_BATCH_DAEMON_MODE=1 bundle exec puma

# Go control + worker
kbatch daemon --config config/daemon.yml
kbatch worker --config config/daemon.yml
```

Typical env: `KAFKA_BROKERS`, `REDIS_URL`, `KAFKA_PREFIX`, `KB_ROLE`, `KAFKA_BATCH_DAEMON_MODE`, `KAFKA_BATCH_HANDLER_MANIFEST`, `KAFKA_BATCH_PRIORITY_CONFIG(S)`.

---

## 36. Document maintenance

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
| Config snapshot + topic inventory | At most every **24 hours** on boot (`config_refreshed_at`); NX lock so only one pod writes. Includes masked knobs + `Topics.inventory` (broker partitions merged with create defaults). |

```ruby
config.ai_knowledge_enabled = true
# ENV: KAFKA_BATCH_AI_KNOWLEDGE_ENABLED=false to disable
```

Force full re-sync: `FORCE=1 bundle exec rake kafka_batch:sync_ai_knowledge`

Live RAG chunk (`config:live`) is injected first for chat. For partition questions prefer `live_broker_partitions` over docs that cite DEFAULT_PARTITIONS (768, etc.).

This file intentionally expands beyond the root README so the assistant has a self-contained corpus without live cluster operational state.


---

## 37. Batch hash fields (Redis)

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

## 38. Instrumentation event catalog (Ruby)

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

Go `pkg/instrument` mirrors the cross-runtime subset (including `workset.reclaimed`, `super_fetch.drained`).

---

## 39. Client produce safety and partial failure

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

## 40. Fairness Lua responsibilities (summary)

| Script | Role |
|--------|------|
| `ENQUEUE_LUA` | Bound window; admit tenant to ring; RPUSH ready list |
| `CHECKOUT_LUA_COUNT` | Throughput lane: advance vtime at dispatch; lease slots |
| Time checkout / `COMPLETE_LUA_TIME_LEASE` | Time lane: advance vtime at completion by duration/weight |
| Tenant partition `CHECKOUT_LUA` | Exclusive partition assignment from free pool |
| Forward confirm / reclaim paths | Clear forwarding HASH; recover stale forwards after grace |

In-flight is **derived from lease ZSETs**, not a brittle counter — expired leases self-heal concurrency budget after crashes.

---

## 41. ConsumptionGate and PriorityGate interaction

1. **ConsumptionGate** — if topic/partition paused in Redis snapshot, consumer does not process (refresh ≤30s).
2. **PriorityGate** — lower ranks check higher-topic lag; paused higher topics count as inactive so lower ranks proceed.
3. SuperFetch still applies inside allowed consumes.

---

## 42. Empty and edge batches

| Case | Behavior |
|------|----------|
| Seal with `total_jobs=0` | Immediate success path |
| Open unsealed batch | Cannot finalize until seal |
| Events for unknown/cancelled batch | Lua duplicate/ignore |
| Mega-batch | One hash + three bitmaps; shard in app policy |

---

## 43. Go client API mirror

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

## 44. README drift vs library defaults (operators)

| Topic | Library default | Often documented / install |
|-------|-----------------|----------------------------|
| `max_retries` | 7 | 3 in install / some tables |
| `fairness_global_concurrency` | 50 | 1000 install / fairness examples |
| `fairness_lease_ttl` | 1800 | 7200 install |
| `fairness_min_ingest_partitions` | 2 | 300 ops target |
| Bitmaps | touch + ok + fail | Older docs mentioned only touch |
| Completion dedup | `batch_seq` bitmaps | Not Kafka offset keys |

When answering “what is the default?”, prefer `Configuration#initialize` / Go `DefaultDaemon` over marketing tables.
