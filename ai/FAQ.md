# kafka-batch curated FAQ

> Companion to [`ai/README.md`](./README.md). Large Q&A corpus for RAG. Stay consistent with the knowledge base. The Web UI assistant answers from **docs only** — never query or mutate live operational Redis (ledger, fairness, workset, uniq, schedule, **cron leader lock**, liveness, pause, retry-cancel, reconciler, perf).

---

## A. Product basics

### What is kafka-batch?
A Sidekiq Pro–style batch job system using Kafka as durable transport and Redis for coordination (ledger, fairness, uniqueness, SuperFetch workset, liveness, schedule index). Ruby gem `kafka-batch` plus companion `kafka-batch-go`.

### How is it different from Sidekiq?
Jobs travel on Kafka topics (durable, partition-scalable). Batch counting and fairness live in Redis. Execution can be Ruby (Karafka) and/or Go (`kbatch worker`). Callbacks can be job-style on your topics.

### Do I still need Redis if jobs live in Kafka?
Yes. Redis is mandatory. Kafka holds payloads/offsets; Redis holds batch counters/bitmaps, WFQ state, uniq locks, SuperFetch claims, schedule pointers (unless MySQL), heartbeats, pause state.

### Can I run without MySQL?
Yes. Defaults `store: :redis` and `schedule_store: :redis` need no MySQL. Use MySQL for durable failure logs/pauses, a disk-backed schedule index, and/or **recurring cron tables**.

### Does `store: :mysql` move the batch ledger to MySQL?
No. Hot ledger and bitmaps stay in Redis. MySQL adds failure log / pause fallback tables.

### Ruby vs Go — which should I use?
Either or both. Client can enqueue both. Control must be **one** runtime per shared topic set. Execution must match handler runtime. Never share one execution topic across Ruby and Go.

### What repos make up the system?
`kafka-batch` (Ruby client, Karafka control/exec, Web UI) and `kafka-batch-go` (Go client, `kbatch daemon`, `kbatch worker`).

---

## B. Architecture and tiers

### What are the three tiers?
1. **Client** — enqueue batches/jobs. 2. **Control** — fair forward, events, retry, callback produce, schedule poller, **recurring ticker**, workset reclaim, reconciler. 3. **Execution** — run handlers and emit completion events.

### Do tiers talk over HTTP or gRPC?
No. Only Kafka + Redis.

### What runs in Ruby control vs Go daemon?
Both can run events, retry, fair dispatch/forward, schedule poller, **recurring ticker**, reclaim, reconciler. **Do not run both control planes** on the same events/retry/ingest topics. Ruby + Go **recurring** tickers **may** share one cluster (ledger + leader lock prevent double-fire).

### What does `kbatch worker` run?
Go execution only: plain Go topics, fair-ready `.go`, priority Go groups — all via SuperFetch. It does not run control consumers.

### What does `kbatch daemon` run?
Control plane only. It does **not** execute job handlers.

### What is `daemon_mode` in Ruby?
API/client pods skip Karafka (`draw_routes` skipped). Enqueue only; run control/execution elsewhere.

### Does `KB_ROLE` select which Karafka groups run?
Not by itself. It mainly gates schedule poller in generated initializer patterns. Filter groups with `--include-consumer-groups`. Recurring is separate: set `recurring_scheduler_enabled` / `KAFKA_BATCH_RECURRING_SCHEDULER_ENABLED` on those same scheduler pods.

### What is `CG`?
`consumer_group` base (default `kafka-batch`). With `topic_prefix=myapp` → `myapp.kafka-batch`. Groups are `{CG}-control`, `{CG}-jobs`, etc.

### List Ruby consumer groups
`{CG}-control`, `{CG}-dispatch-{time|throughput}`, `{CG}-jobs-fair-{lane}`, `{CG}-{priority-suffix}`, `{CG}-jobs`.

### List Go worker groups
`{CG}-go-worker-jobs`, `{CG}-go-worker-fair-ready-{lane}`, `{CG}-go-worker-{priority-suffix}`.

---

## C. job_type, handlers, manifest

### What is `job_type`?
Stable cross-language handler ID on the wire. Must match manifest key, Ruby `job_type`, and Go `kbatch.Register` name.

### How does JobConsumer resolve handlers?
1) `job_type` in registry 2) `worker_class` const_get + auto-register 3) unknown → DLT.

### What if a Go job lands on a Ruby consumer?
Treated as error / poison path — one execution topic must map to one runtime.

### Is the handler manifest required?
Required for Go jobs and recommended for mixed routing. Path: `handler_manifest_path` or `KAFKA_BATCH_HANDLER_MANIFEST`.

### Can the same `job_type` appear twice?
No — boot error on duplicate registration.

### What is default `job_type` for a Ruby worker?
Derived from class name (`ProcessOrderWorker` → `process_order`) unless overridden.

### How do I enqueue a Go job from Ruby?
`KafkaBatch::Batch.enqueue_job("segment.export", ...)` or `b.push_job(...)` inside a batch. Manifest must declare `runtime: go`.

### How do I register a Go handler?
`kbatch.Register("job_type", func(ctx *kbatch.Context) error { ... })` matching the manifest key. Worker boot validates all Go handlers are registered.

### What fields are on the job wire envelope?
`job_type`, `worker_class`, `job_id`, `payload`, `attempt`, `max_retries`, optional `batch_id`, `batch_seq`, `tenant_id`, `_uniq_fp`, `_reclaim`, `retry_tier`, `valid_till`, `batch_counted`, fair markers.

---

## D. At-least-once, atomicity, idempotency

### Why did my job run twice?
Kafka redelivery, retries, or SuperFetch reclaim (`_reclaim: true`). Handlers must be idempotent. Framework dedups **batch counters**, not business side effects.

### Are completion counts exactly-once?
Yes per `batch_seq` via three Redis bitmaps (touch/ok/fail) inside one Lua script.

### Are callbacks exactly-once?
No. Claim is atomic; delivery is at-least-once. Use deterministic callback `job_id`s and idempotent handlers.

### Does uniqueness make perform idempotent?
No. It only dedups **enqueue** while a job is queued/in-flight.

### What does fail-open uniqueness mean?
If Redis errors during claim, enqueue proceeds without dedup (availability over strict dedup).

### Is batch create atomic with Kafka produce?
Redis create is atomic. Produce is separate; failures roll back uniq + job count when possible. Schedule index write after produce is retried / reclaimable.

### What if event emit fails after a successful perform?
Job is not retried as a failure path — Kafka offset stays uncommitted so the job may redeliver. Handlers must tolerate that.

### What is a fence token?
Random token on SuperFetch claim. Renew/complete require matching consumer+fence so a stolen job cannot be completed by the old performer.

---

## E. Batches and callbacks

### When does `on_complete` fire vs `on_success`?
`on_complete` when every job has been **touched** (first execution) — retries may still run. `on_success` only when every job succeeded. Terminal when `completed+failed >= total`.

### What is a touch / `executed` event?
First finish attempt of a job (often first failure before retry). Increments touch bitmap without counting as terminal fail.

### Why create Redis batch before Kafka produce?
So completion counting has a ledger. If produce fails mid-way you may have a partial batch — cancel/delete or reconciler; don’t reverse the order.

### What is seal?
Opens the completion gate (`locked_at`), pre-sizes bitmaps. Block form seals when the block exits (even on exception after some pushes).

### What is `Batch.open`?
Rehydrate an existing batch to push more jobs (jobs-adding-jobs), restoring `tenant_id` for fair routing.

### What is `batch_seq`?
1-based contiguous job index within a batch used as bitmap bit position (`seq-1`). Allocated atomically via `kafka_batch:b:seq:{id}`.

### Why did push raise `BatchClosedError`?
Batch already completed or cancelled (or otherwise closed to new jobs).

### What is the difference between `meta` and `callback_args`?
Both stored on the batch. Only `callback_args` go to callback handlers. `meta` is for dashboard/API labeling.

### How do I enqueue many jobs without a batch?
`KafkaBatch::Batch.enqueue_many(Worker, payloads, tenant_id: …)` or `Worker.perform_bulk(payloads, …)`. Chunked Kafka produce; **no** Redis batch ledger, completion events, or callbacks. Use for throughput / fire-and-forget. Delayed: `enqueue_many_in` / `enqueue_many_at`. Go: `EnqueueMany` / `EnqueueManyJobs`.

### When should I use `enqueue_many` vs `Batch.create { push_many }`?
`enqueue_many` = standalone delivery only. `Batch.create` + `push_many` = tracked batch with completion counting and optional `on_success` / `on_complete`.

### Can a batch mix Ruby and Go jobs?
Yes. Callbacks fire when the whole batch finishes.

### How do job callbacks pick a runtime?
By execution topic: Go topic → `kbatch worker`; Ruby topic → Karafka. Fair work needs explicit callback topic (never fair ingest).

### What is the deterministic callback job_id?
`#{batch_id}:on_success` or `#{batch_id}:on_complete` — helps idempotent redelivery.

### What are legacy callbacks?
Ruby class string → `callbacks_topic` → `CallbackConsumer`. Prefer job callbacks for new code.

### Does Go daemon run CallbackConsumer?
No. It can **produce** preclaimed legacy callback messages. Ruby control must consume them, or use job callbacks only.

### What is `preclaimed: true` on a callback message?
Events Lua already won `HSETNX` claim fields; CallbackConsumer should invoke without re-claiming.

### Outcome `success` vs `success_only` vs `complete`?
`success` fires both success+complete; `success_only` only success; `complete` only on_complete (including early complete while retries run).

---

## F. SuperFetch and workset

### What is SuperFetch?
Always-on execution path: Redis claim → Kafka offset mark → thread/goroutine pool perform. Offsets advance at delivery rate, not perform latency.

### Why Claim before Kafka mark?
So one partition can feed many long jobs. Crash after mark leaves the job in the workset for reclaim.

### Why is Ruby SF default 1 but Go is 10?
MRI GVL vs Go true parallelism. Don’t copy Go defaults into Ruby blindly.

### What is `super_fetch_claim_window`?
Max Claimed∨Queued∨Performing. Default `2×` concurrency. Gates claim+ack so perform backlog doesn’t block rebalance forever.

### Who runs workset reclaim?
≥1 control plane: Ruby `Workset::ReclaimScheduler` or Go `kbatch daemon` on shared Redis. Execution workers do not replace reclaim.

### My long job keeps getting reclaimed — why?
Lease TTL too short, renew failing (Redis pool), missing heartbeat, or orphan grace mis-tuned. Raise `super_fetch_lease_ttl`, size `redis_pool_size`, verify liveness TTL/heartbeat.

### What does `_reclaim: true` mean?
Control reclaim re-produced an orphaned claimed job. Treat as at-least-once redelivery.

### What are the workset Redis keys?
`work:job:`, `work:by_consumer:`, `work:index`, `work:reclaiming:`, `work:produced:` plus `live:consumer:` for ownership.

### What happens on SIGTERM?
Drain in-flight perform up to `super_fetch_drain_timeout`; leftovers stay in workset for reclaim.

### How do I size Ruby redis_pool for SF?
Auto ≥16; scales with SF + claim window + Karafka floor. Raise before raising SF if you see pool wait timeouts.

### Go max concurrent jobs formula?
`jobs_consumer_concurrency × super_fetch_concurrency` (and same per fair-ready / priority group).

---

## G. Fairness

### How do I enable fairness in Ruby?
`fairness_type :time` or `:throughput` on the Worker / manifest. No global on/off.

### How do I enable fairness in Go?
Set `fairness_enabled: true` on the daemon. Without it, fair jobs sit on ingest forever.

### Time vs throughput lane?
Time: weighted wall-clock (vtime at completion). Throughput: weighted job count (vtime at checkout).

### What happens when the ready window is full?
Enqueue Lua returns full → Dispatcher pauses ingest partition; backlog stays in Kafka.

### Why is my whale tenant starving others?
Weights, weighted concurrency, or ingest collisions. Check `/weights`, dynamic tenant partitions, pin VIPs, size ingest partitions.

### Do fair retries go through ingest again?
No — they re-enter **ready** and skip WFQ.

### Why must `fairness_lease_ttl` exceed max job runtime?
In-flight slots are TTL leases. Early expiry admits extra work (soft overshoot), not data loss.

### What is dynamic tenant partition assignment?
On first enqueue, checkout an exclusive ingest partition from a Redis free-pool (per lane). Static map always wins. Fallback murmur2 when pool exhausted.

### Ruby vs Go `fairness_ready_window` default?
Ruby **500**, Go **100**. Set Go to 500 for parity.

### Where are weights stored?
Redis `kafka_batch:fair_{lane}:weight` regardless of `config.store`. UI `/weights/*`. Cached in-process for `fairness_weight_cache_ttl` (60s).

### What is forwarding HASH?
Durable copy of a checked-out job until produce to ready is confirmed — crash between LPOP and produce does not lose the job.

### What is slot dedup?
Prevents double-admit on ready-topic redelivery after reclaim (`fairness_slot_dedup_ttl`, 0 → lease_ttl).

### Fair ready backlog but ingest OK — what now?
Raise `fairness_global_concurrency` and/or reduce oversubscribed execution concurrency.

### Time lane boot validation fails — why?
Need `fairness_global_concurrency > 0` or `fairness_max_inflight_per_tenant > 0` so one tenant cannot monopolize while vtime only advances at completion.

### Hybrid fair Ruby+Go without split ready topics?
Boot `ConfigurationError` — need `.ruby` and `.go` ready topics.

### Is there a combined (non-suffixed) fair ready topic?
No. Fair ready topics are **always** runtime-split: `fair_time_ready.go` / `fair_time_ready.ruby` (and throughput). The legacy combined `fair_*_ready` topic was removed and is no longer supported, routed, or created. The Forwarder always routes to the `.go` or `.ruby` topic by handler runtime (unknown → `.ruby`); the Go worker consumes `.go`, Ruby Karafka consumes `.ruby`. Ingest stays a single topic per lane (`fair_*_ingest`), shared by both runtimes.

---

## H. Delayed jobs / schedule

### Why aren’t `perform_in` / `enqueue_at` jobs running?
`schedule_poller_enabled` defaults **false**. Enable on few scheduler/control pods only.

### Redis vs MySQL schedule store?
Redis ZSET = low latency. MySQL = disk-backed at scale. Independent of batch `store`.

### Can delayed jobs dispatch twice?
Yes — lease reclaim after crash is at-least-once. Handlers must be idempotent.

### What if scheduled payload was compacted away?
Poller drops + acks after read misses; job is gone — size `scheduled_topic` retention ≥ `max_schedule_horizon`.

### Why set Go `schedule_poll_jitter: 0.1`?
Ruby defaults 0.1; Go defaults 0. Matching jitter de-syncs pods and reduces schedule-store stampedes.

### Does CancellationCache affect scheduled jobs?
Yes — poller skips jobs whose batch is cancelled.

### What is `schedule_lease_seconds`?
TTL on a claimed due pointer so a crashed poller does not strand the job forever.

### Is recurring cron the same as delayed jobs?
**No.** Delayed jobs (§16 / FAQ H) are one-shot `perform_in`/`perform_at` via `scheduled_topic` + pointer index. Recurring cron (FAQ H2 / README §17) stores schedules in MySQL and fires via normal `enqueue_job`.

---

## H2. Recurring (cron) scheduler

### What is the recurring scheduler?
Whenever-style repeating cron that enqueues a **manifest `job_type`** on a schedule. Ruby `Recurring::Ticker` and Go `pkg/cron` share MySQL tables + Redis leader lock.

### How do I install the tables?
Ruby: `rails g kafka_batch:install --recurring && rails db:migrate`. Go-only: copy SQL block C from kafka-batch-go README (or rely on `EnsureSchema` on first daemon boot — prefer explicit SQL in prod).

### How do I enable it?
Ruby: `config.recurring_scheduler_enabled = true` or `KAFKA_BATCH_RECURRING_SCHEDULER_ENABLED=true`. Go: `recurring_scheduler_enabled: true` in `daemon.yml` (**no** Go env override today). Enable on few scheduler/control pods only.

### Can Ruby and Go tickers run together?
Yes. Correctness is the fire ledger PK `(schedule_id, fire_at)` + deterministic `job_id` `sched-{id}-{unix}`. Leader lock `kafka_batch:cron:leader_lock` is an optimization.

### What are misfire policies?
`fire_once` (default) — one catch-up fire then jump to next after now. `skip` — fire only if within `misfire_grace`, else skip. `backfill` — every missed instant capped by `recurring_max_backfill` per tick.

### Where is the UI?
`/kafka_batch/recurring`. APIs: `GET/POST /api/recurring`, `POST/PATCH/DELETE /api/recurring/:name`, `POST /api/recurring/:name/run` (run-now has **no** ledger row).

### What DB connection does recurring use?
Ruby: `schedule_store_database_connection`. Go: `recurring_mysql_dsn` falling back to `schedule_mysql_dsn`.

### Why is a schedule “stale”?
Enabled and idle longer than `recurring_stale_factor × cron_interval` (default factor 2). Idle measured from `last_fire_at` or `next_run_at`.

### Dedicated Ruby process?
`rake kafka_batch:recurring:run` (loop) or `kafka_batch:recurring:tick` (one pass). Requires fugit for cron parsing / staleness interval.

---

## I. Retries and DLT

### How do retry tiers avoid head-of-line blocking?
Each delay tier has its own Kafka topic.

### Can I pin all retries to one tier?
Yes — Worker `retry_tier :large` (or other configured tier).

### Why does RetryConsumer pause the partition?
Next message’s `retry_after` is in the future. It never skips ahead of a not-due head message.

### What is RetryCancel?
UI/operator cancel of specific retry jobs via Redis SET + skip watermarks (`kafka_batch:retry:cancel`, `retry:skip`).

### Where do exhausted jobs go?
`dead_letter_topic` + failed completion event. Optional MySQL failure log if `store: :mysql`.

### What is `max_retries` default?
Library **7**. Install template / some README tables may say **3** — prefer Configuration / YAML source of truth.

### List common `dlt_type` values
`malformed_event`, `incomplete_event`, `malformed_callback`, `callback`, `callback_error`, `retry_routing`, `expired`, `malformed_ingest`, `schedule_route_error`, poison/unknown handler paths, etc.

---

## J. Priority queues

### Does priority preempt running jobs?
No. It affects which messages are consumed next.

### Can the same topic be in priority YAML and plain `-jobs`?
No — boot rejects overlaps to prevent double-processing.

### Does fairness override priority?
Yes. Fair workers use ingest/ready, not priority groups.

### What is strict vs weighted priority mode?
Strict blocks lower ranks while higher has lag. Weighted allows 1-in-N lower-rank interleave (`weighted_interleave`).

### What if lag Admin API fails?
Gate **fail-open** (uses last result / allows progress) — prefer availability.

### How does pause interact with priority?
Topic-level pause excludes topic from lag gate so lower ranks keep flowing.

### Go priority concurrency knob?
`priority_consumer_concurrency` (default 4) in-process members per priority group.

---

## K. Cancellation and pause

### I cancelled a batch but jobs still ran — bug?
Usually eventual consistency (`cancellation_cache_ttl`, default 120s). Same-process cancel is immediate via optimistic cache add.

### How does `/lag` pause/resume work?
UI writes Redis pause sets; consumers refresh within `consumption_control_refresh_interval` (30s). Go uses PauseFetch so offsets don’t advance while paused.

### Pause key format?
`group\x1ftopic` and `group\x1ftopic\x1fpartition` in consumption SETs.

### MySQL store and pause?
Pause can fall back to MySQL table when Redis is unavailable (`store: :mysql`).

---

## L. Liveness and /live

### What do heartbeats do?
Prove consumer alive for `/live` UI and SuperFetch steal/reclaim (`EXISTS` on `live:consumer:`).

### Defaults for TTL and interval?
TTL 180s, heartbeat every 20s (~9 misses before expiry).

### Go `liveness_enabled: false` but reclaim still works?
Yes — SuperFetch still writes Redis heartbeats. HTTP `/health` is optional.

### What does `track_running_jobs = false` do?
Stops per-job live keys; keeps consumer heartbeats. Use at extreme throughput if `/live` job detail isn’t needed.

### Are liveness writes allowed to fail the job path?
No — best-effort with circuit breaker.

---

## M. Reconciler

### What does the reconciler fix?
Stuck `running` batches that are actually drained, and terminal batches missing callback dispatch timestamps.

### How often does it run?
Default every 300s on control/EventConsumer; also `rake kafka_batch:reconcile` / `kbatch reconcile`.

### Why did reconciler skip?
Another process holds `kafka_batch:b:reconciler_lock`, or batches are open/unsealed / still genuinely in progress. See `reconciler:last_skip`.

### Cap per sweep?
`max_reconcile_per_run` default 100 — limits callback storms during incidents.

---

## N. Uniqueness and expiry

### How does `uniq true` fingerprint payloads?
XXHash64 over `worker_class + NUL + canonical JSON` (sorted keys). Wire carries `_uniq_fp`.

### Cross-runtime uniq — will Ruby and Go agree?
Yes when payload canonicalization matches; matrix tests cover special characters.

### What is `valid_till`?
Consumption-time expiry. Expired jobs go to DLT (+ failed event if batched). Not the same as delayed enqueue.

### Unparseable `valid_till`?
Treated as expired (poison-safe).

---

## O. Configuration and defaults

### Where is the full Ruby knob list?
`ai/README.md` §30 and `KafkaBatch::Configuration`.

### Where is the full Go knob list?
`ai/README.md` §32 (Go configuration reference — all `daemon.yml` keys, worker throughput knobs, env overrides, manifest fields, MySQL DDL blocks) and `kafka-batch-go` `config/daemon.example.yml`. §31 is the Ruby reference.

### Why do README tables disagree with library defaults?
Install generator often ships production-oriented values (e.g. fairness 1000, lease 7200, max_retries 3) while library `initialize` keeps smaller defaults. Prefer code/YAML as truth; see parity section.

### Does `topic_prefix` apply to everything?
Prefixed settings (default topics + consumer group) yes. Explicit worker/manifest topics are typically the final names you configure; priority YAML names go through `resolve_topic`.

### How do Go YAML env interpolations work?
Any value may use `${VAR}` or `${VAR:-default}`. Env overrides also applied after YAML for many keys (see §32 for the complete `applyEnv` list).

### What is Go `execution_mode`?
`superfetch` (default) claims jobs into a Redis workset and runs `#perform` on a bounded goroutine pool with TTL leases + reclaim. `watermark` is an advanced Redis-free mode that tracks committed offset watermarks instead of a workset — it needs strictly idempotent handlers and one mode per topic. Set per daemon/worker via `execution_mode:` or `KAFKA_BATCH_EXECUTION_MODE`.

### Which Go settings are worker-only vs daemon-only?
`kbatch worker` reads the throughput/SuperFetch knobs (`jobs_consumer_concurrency`, `fair_ready_consumer_concurrency`, `priority_consumer_concurrency`, `super_fetch_*`, `execution_mode`). `kbatch daemon` (control) reads events/retry/fairness dispatch/schedule/recurring/reconciler/liveness. Both share Kafka/Redis identity, topics, and the handler manifest. See §32.

### Go MySQL DSN formats?
Native go-sql-driver **or** `mysql2://` / `mysql://` URLs (converted at connect). Prefer `parseTime=true&loc=UTC`.

---

## P. Topics and partitions

### How many partitions do I need?
Roughly peak_pods × (concurrency or members) × SuperFetch concurrency on each scaled execution topic. Fair ingest often hundreds for exclusive tenants.

### Default partition targets?
**create_topics defaults only:** every topic defaults to **16** partitions except the fairness **ingest** and **ready** lanes, which default to **64**. Replication factor defaults to **1**. Live clusters often differ (Kafka cannot shrink). Scale execution topics up before heavy load.

### How many partitions does my live topic have?
Use the AI live config snapshot `topic_inventory` → `live_broker_partitions` for that topic name (e.g. `kafka_batch.fair_time_ready.ruby`). Do **not** answer with the DEFAULT_PARTITIONS number unless broker metadata is unavailable — then say so. Refresh: boot NX sync every 24h, or `FORCE=1 rake kafka_batch:sync_ai_knowledge`.

### Can I shrink partitions later?
No — Kafka cannot shrink. Oversizing is safer than undersizing.

### Are manifest Go topics auto-created by Ruby rake?
Not always — create via Go `kbatch topics` or explicitly. README notes this gap.

### Scheduled topic retention rule?
Must be ≥ `max_schedule_horizon` (default 7 days) or pointers can reference deleted payloads.

---

## Q. Web UI

### Is the dashboard safe without auth?
No. Mount behind app authentication. CSRF protects mutating APIs; `web_authenticator` is defence-in-depth only.

### What does Live refresh do?
Toolbar toggle (`kafka_batch_live`) refetches APIs every 5s.

### Pending in batches vs Jobs pending on the home page?
**Pending in batches** (`pending_jobs`) = untouched jobs in running Redis batches. **Jobs pending** (`topic_pending`) = sum of Kafka lag across gem topics excluding the scheduled topic’s log-size rows. Different signals.

### Why is Performance empty?
Need `performance_metrics_enabled` on processes that emit job metrics (and retention window). Shared Redis bucket schema for Ruby+Go.

### Why does System hide secrets?
By design — masks passwords/tokens/api keys. AI encryption salt is masked; OpenRouter keys are encrypted in Redis.

### Which UI actions need CSRF?
Cancel/delete/bulk, pause/resume, weight edits, retry deletes, AI settings/chat — cookie `_kb_csrf` + `X-CSRF-Token` from bootstrap.

---

## R. Metrics and instrumentation

### Does the gem ship a metrics backend?
No. Opt-in bridge to StatsD/Datadog/proc; you supply the client.

### What is PerformanceMetrics vs Metrics?
Metrics = external StatsD/Datadog export. PerformanceMetrics = Redis minute hashes for the Web UI Performance page. Both can run.

### Sample rate less than 1.0?
`performance_metrics_sample_rate` randomly samples writes to cut Redis load at extreme throughput.

---

## S. Deployment scenarios

### Dev all-in-one Ruby?
`KB_ROLE=all bundle exec karafka server` + Rails. Schedule poller on.

### API-only Rails?
`KAFKA_BATCH_DAEMON_MODE=1` — no consumers.

### Mixed Ruby control + Go workers?
Yes — common. Share Redis/topics/manifest. Do not also run Go daemon on same control topics.

### Mixed Go control + Ruby CallbackConsumer?
Needed if you still use legacy class callbacks while Go produces them.

### Scheduler pods only?
Enable schedule poller on 2–3 pods; don’t enable on every execution replica.

### What must stay aligned across runtimes?
Topic names/prefix, Redis, manifest job_types, fairness topics/TTLs, workset schema, retry tiers, DLT, pause key format, uniq fingerprinting, perf bucket keys.

---

## T. Assistant / RAG scope

### Can the AI inspect live Redis or change weights?
No. Docs-only. Cluster actions stay on existing UI pages.

### Where will API keys live when chat ships?
Encrypted in Redis with salt from initializer; masked on AI Settings page. Separate from operational keys.

### What if the assistant contradicts production?
Treat `ai/README.md` + source as truth; update docs and re-index. Do not give the assistant live Redis access.

### Which files are the RAG corpus?
`ai/README.md` and `ai/FAQ.md` (this file).

---

## U. Troubleshooting cheat sheet

| Symptom | Likely cause | Check |
|---------|--------------|-------|
| Jobs stuck claimed | Dead worker / reclaim off | Control reclaim; heartbeats; lease TTL; redis pool |
| Fair lane monopolized | Window/weights | `fairness_global_concurrency`; weighted mode; `/weights` |
| Fair jobs never run (Go) | `fairness_enabled` false | daemon.yml |
| Delayed jobs idle | Poller off | `schedule_poller_enabled` on few pods |
| Double perform | Redelivery/reclaim | Idempotent handler; fence logs |
| Cancel ignored briefly | Cache TTL | `cancellation_cache_ttl` |
| Pods idle, lag skewed | Partitions / key skew | Partition count; tenant partition map |
| Redis pool timeouts | SF too high | `redis_pool_size`; lower SF |
| Go+Ruby double consume | Shared execution or dual control | One runtime/topic; one control plane |
| Legacy callbacks missing | No CallbackConsumer | Ruby control or job callbacks |
| Schedule stampede | Jitter 0 on Go | `schedule_poll_jitter: 0.1` |
| Ready backlog, ingest OK | Admission vs workers | Raise global concurrency or reduce workers |
| Event lag | Control underprovisioned | More control pods + events partitions |
| One partition lags on member | Fetch budget | Lower `consumer_fetch_max_partition_bytes` |
| Uniq not working | Redis error fail-open | Redis health; check logs |
| Partial batch after crash | Produce mid-push | Cancel/reconcile; inspect total_jobs |
| Callback twice | At-least-once | Idempotent callback; deterministic job_id |
| Performance empty | Feature off | `performance_metrics_enabled` |
| `/live` empty | Liveness off / Redis | `liveness_backend`; TTL |
| Pause slow to apply | Refresh interval | up to 30s consumer cache |
| DLT flooded with incomplete_event | Bad producers | Event schema required fields |
| Reclaim storms | Orphan grace too low / HB broken | grace, liveness_ttl, heartbeat interval |

---

## V. Atomicity quick answers

### Is completion counting race-safe across partitions?
Yes — Lua + bitmaps; concurrent EventConsumers can process different events safely per batch_seq.

### Is fair checkout race-safe across forwarders?
Checkout Lua + leases are designed for concurrent callers; slot ids and forwarding HASH cover crash windows.

### Is uniqueness claim race-safe?
SETNX — first writer wins; losers skip/raise.

### Is reconciler single-flight?
Yes — `SET NX EX` lock.

### Is reclaim produce exactly-once?
At-most-once produce via `work:produced:` marker; perform remains at-least-once.

---

## W. Payload and routing edge cases

### What is `retry_to`?
Destination topic for RetryConsumer to re-produce when due (plain or ready).

### What fair markers appear on ready messages?
`_fair_slot`, `_fair_type`, `_fair_slot_id` (and related) for lease/complete accounting.

### What is `batch_counted`?
Marks that the first-failure touch event was already emitted so retries don’t double-touch.

### Standalone jobs (no batch)?
Allowed — no batch_id/bitmap path; still at-least-once perform; may still emit/omit events per code paths for non-batched jobs.

### Shared default `jobs_topic`?
Workers without `kafka_topic` share it. Dispatch is safe because messages embed handler identity. Use dedicated topics for isolation/scaling.

---

## X. Security and ops hygiene

### Should the Web UI be public?
No — cancel, pause, weights, DLT contents are sensitive.

### Are Redis passwords shown in System?
Masked as `***`.

### Audit log?
Optional `audit_enabled` → MySQL `kafka_batch_audit_logs` for mutating UI actions; strip secrets from params.

### Producer `max_message_bytes`?
Default 1 MiB guard; oversized payloads raise before Kafka reject. Set 0 to disable.

---

## Y. Cross-runtime parity FAQ

### Which defaults intentionally differ?
SF concurrency (1 vs 10), fairness_ready_window (500 vs 100), schedule_poll_jitter (0.1 vs 0). Recurring: Ruby has `KAFKA_BATCH_RECURRING_*` env overrides; Go is YAML-only. Cron parsers differ (Fugit vs Go 5-field).

### Which contracts must not differ?
Workset keys/Lua semantics, batch ledger/bitmaps, fair namespaces, uniq fingerprint, pause key format, retry tier topic naming, event/callback envelopes, perf Redis layout.

### How is partition parity tested?
Cross-runtime matrix tests compare murmur2 partitioning for fairness co-partitioning.

### Can Ruby client + Go schedule poller + Go worker work?
Yes — supported matrix combination when topics/Redis/manifest align.

---

## Z. See also

- Deep dive: [`ai/README.md`](./README.md)
- Gem README: [`../README.md`](../README.md)
- Go companion: [kafka-batch-go](https://github.com/y-shashank/kafka-batch-go)
- Ruby config source: `lib/kafka_batch/configuration.rb`
- Go config source: `pkg/config` + `config/daemon.example.yml`


---

## AA. Batch hash and indexes

### What Redis indexes exist for batches?
`index:running`, `index:done`, `index:all`, `index:cancelled`, plus `kafka_batch:counts` hash for O(1) status tallies.

### What is `index:all` capped by?
`all_index_max_size` (default 200_000) — oldest evicted so the UI listing cannot grow forever.

### What does `locked_at` empty mean?
Batch still open (block population) — completion gate closed; cannot finalize mid-population.

### What claims callback dispatch?
`HSETNX` on `complete_callback_dispatched_at` / `success_callback_dispatched_at` (and legacy `callback_dispatched_at`).

### Can two EventConsumers double-fire callbacks?
Lua claim stamps prevent double dispatch; handlers may still see at-least-once job delivery afterward.

---

## AB. EventConsumer deep dive

### Are events processed one-by-one or batched?
Per-poll batching: pipeline Lua evals, then commit offsets for the whole poll.

### What if one event in a poll is malformed?
Malformed → DLT path; incomplete required fields → `incomplete_event` DLT. Design avoids silent skips.

### Why keep src_topic/partition/offset on events?
Observability / DLT / debugging. Counting dedup is bitmap on `batch_seq`, not offset keys.

### Does reconciler run inside EventConsumer?
Yes — shared background timer every `reconciliation_interval` (class-level across partitions).

### What happens on cancelled batch events?
Lua treats as duplicate/ignore — no counter bump.

---

## AC. JobConsumer deep dive

### Order of gates before perform?
Handler resolve → cancel check → valid_till → fair slot claim/renew → perform → events/retry/DLT.

### Does cancel skip emit a completion event?
Typically skip without success/fail event (job not counted as terminal). Retries may have separate cancel paths via RetryCancel.

### What does `retries_exhausted` hook receive?
Job metadata + error after terminal failure path (before/around DLT depending on implementation path).

### PriorityJobConsumer vs JobConsumer?
Priority wraps gating around consume; still SuperFetch underneath for execution groups.

### What is ConsumptionGate prepend?
Module prepended so pause checks apply uniformly across consumer types.

---

## AD. Fairness deep dive

### What is the ring ZSET?
Active tenants ordered by virtual time (ascending = most deprived). Checkout picks smallest vtime with ready work.

### What happens when an idle tenant returns?
Re-admitted at max(its vtime, current min vtime) so it cannot hoard idle credit.

### Does vtime reset, or grow forever?
When a lane goes fully idle — empty ring, no live leases, empty forwarding buffer, and zero ingest lag — held for `fairness_vtime_idle_reset_debounce` (default 15s), the Forwarder clears the `vtime` hash (weights preserved). This gives fresh per-active-period fairness (a busy period never carries vtime debt/credit into the next) and bounds vtime growth. Controlled by `fairness_reset_vtime_when_idle` (default true; Ruby env `KAFKA_BATCH_FAIRNESS_RESET_VTIME_WHEN_IDLE`, Go YAML `fairness_reset_vtime_when_idle`).

### Can the idle vtime reset disrupt a running lane or lose fairness state?
No. The DEL runs atomically only when the ring is empty (`RESET_VTIME_IF_QUIESCENT_LUA` / `Scheduler#reset_vtime_if_quiescent!`), so a tenant re-enqueuing mid-check is never wiped. It fires at most once per idle period and never mid-run — there is no fixed-interval reset. Weights are untouched.

### What is work-conserving checkout?
First pass respects fair caps; second pass fills slack so capacity is not wasted when some tenants are idle.

### What is `fairness_active_count_source`?
Default `inflight_plus_ready` — smoothed denominator for per-tenant caps. Alternative `ingest_lag` uses Kafka lag (can undercount after dispatcher drains ingest).

### Why cache active count TTL?
Raw ring membership flickers as tenants briefly drain; TTL floors volatility so caps don’t jump wildly.

### Dispatcher batch size meaning?
`fairness_dispatcher_batch_size` / Karafka `max_messages` — how many ingest messages drained per consume call; fairness order is scheduler’s job, not this batch size.

### Forwarder idle sleep?
`fairness_forwarder_idle_sleep` (default 0.05s) when checkout yields nothing.

### Idle vtime reset debounce?
`fairness_vtime_idle_reset_debounce` (default 15s) — how long a lane must stay fully quiescent before the vtime ledger is cleared. Prevents resets during transient empty-ring lulls; the Kafka ingest-lag check runs only at the moment of reset, not every idle tick.

### What is forwarding recovery grace?
Seconds after forwarding lease expiry before reclaiming orphaned checkout (avoids racing a slow produce).

### Can both fairness lanes run in one batch?
Yes — different jobs can use `:time` vs `:throughput`; lanes are independent namespaces.

### Weight 0 or missing?
Falls back to `fairness_default_weight` (1.0).

### How fast do UI weight edits apply?
Within `fairness_weight_cache_ttl` (60s) on dispatcher/forwarder processes.

---

## AE. Schedule deep dive

### Is there a Kafka consumer group for the schedule poller?
No — in-process loop reading the index and fetching payloads by absolute partition/offset.

### What does active drain mean (Go)?
While due jobs remain, poller can loop tightly without sleeping between ticks.

### What is read_miss?
Counter for failed payload reads; after enough misses the pointer is dropped as poison.

### PartialProduceError on schedule — meaning?
Kafka produce of scheduled payload succeeded but index write failed after retries — job may be orphaned from poller view.

### Can schedule_store be MySQL while store is Redis?
Yes — independent knobs.

---

## AF. Retry deep dive

### Why never skip a not-due head message?
Preserves offset order per partition; skipping would strand earlier retries or violate pause semantics.

### What is stripped when retry is due?
`retry_after` / routing fields cleaned; attempt/retry_count updated; fair markers handled so retry targets ready correctly.

### Does first failure touch the batch?
Yes — `executed` event once; subsequent retries should not double-touch (`batch_counted`).

### Fair job retry destination?
Ready topic (not ingest).

### UI delete all retries?
Mutating API clears cancel set / skip watermarks paths — see Web API retry delete endpoints.

---

## AG. SuperFetch deep dive

### Claim Lua return codes (conceptual)?
Won new claim; resumed same owner; lost to live owner (ack duplicate). Steal allowed when owner heartbeat missing past grace.

### Why renew from claim time?
Lease must survive queueing for a perform slot, not only during perform.

### What if complete Redis fails after perform?
Job left in workset for reclaim — may re-perform (at-least-once).

### Lost fence during perform?
Skip complete; do not delete someone else’s claim.

### Drain timeout exceeded?
Process exits; remaining jobs reclaimed later by control plane.

### Go vs Ruby reclaim env surface?
Ruby has more `KAFKA_BATCH_SUPER_FETCH_*` env knobs; Go often YAML-only for lease/grace/interval — set daemon.yml explicitly.

---

## AH. Uniqueness deep dive

### Why binary digest keys?
RAM savings vs hex; wire still uses hex `_uniq_fp` for portability.

### Why release by fingerprint?
Avoid re-canonicalizing payload after JSON round-trips; matches claim material.

### Legacy 8-byte keys?
Older versions; release path also clears legacy keys so upgrades don’t orphan locks until TTL.

### claim_many behavior?
Chunked Redis round-trips; first wins within duplicates in the same bulk push.

### Does uniq apply to Go handlers?
Yes when manifest `uniq: true` and client uniq enabled — shared Redis keyspace.

---

## AI. DLT and poison pills

### Unknown handler — what happens?
DLT + failed event (if batched) so batch can still finalize.

### Incomplete event — what happens?
DLT `incomplete_event` — not silent ack without record.

### Should I build alerting on DLT?
Yes — subscribe/consume `dead_letter_topic` or use UI `/dead_letter` + stats.

### DLT retention at creation?
30 days (topic creation helper).

---

## AJ. Web API surface (detailed)

### Bootstrap endpoint purpose?
SPA shell config + CSRF token for mutating calls.

### Fairness UI endpoints?
`GET /api/fairness/:type`, weights `GET/PUT/DELETE /api/weights/:type[/:tenant_id]`.

### Recurring API?
`GET/POST /api/recurring`, `POST|PATCH /api/recurring/:name` (enabled), `DELETE /api/recurring/:name`, `POST /api/recurring/:name/run`. Requires migrated tables; see FAQ H2.

### Lag pause/resume?
`POST /api/lag/pause`, `POST /api/lag/resume`.

### Dead letter API?
Paginated read of Kafka DLT (newest first) — not Redis.

### Audit API?
Requires `audit_enabled`; otherwise explains disabled.

### Performance API?
Requires `performance_metrics_enabled`; otherwise explains disabled.

### Dashboard `topic_pending`?
`GET /api/dashboard` → sum of `Lag.pending_total` (consumer lag, excludes scheduled log-archive). Separate from `pending_jobs` (batch ledger).

### AI chat partition answers wrong (cites create defaults)?
Docs cite create defaults (16, or 64 for fair ingest/ready). Live snapshot must include `topic_inventory` with `live_broker_partitions`. Force sync after deploy; assistant must prefer live broker counts over DEFAULT_PARTITIONS.

### Does RAG know my handlers.yml and priority queues?
Yes — on the same 24h NX-locked config sync, `routing` embeds parsed `kafka_batch_handlers.yml` (job_type → runtime/topic/fairness) and priority YAML groups (ordered topics + consumer group). Ask “what topic does X use?” / “what’s the jobs-fast priority order?” from LIVE ROUTING, not docs examples.

---

## AK. Topics CLI and rake

### Ruby dry-run topics?
`bundle exec rake kafka_batch:topics`

### Create missing only?
`kafka_batch:create_topics` — skips existing, never alters partition counts.

### Go validate?
`kbatch topics validate` fails if required topics missing.

### PARTITIONS env?
Uniform override across categories for create helpers.

---

## AL. Failure mode playbooks

### Playbook: lag on fair ready, ingest low
Admission starved relative to workers or workers down. Check forwarder health, `fairness_global_concurrency`, worker SF/members, ready consumer groups.

### Playbook: lag on fair ingest, ready empty
Dispatcher paused (window full) or dispatcher down. Check ready window, Redis, dispatch group, tenant partition hotspots.

### Playbook: events lag
Scale control pods / events partitions; check EventConsumer errors / Redis Lua latency.

### Playbook: jobs claimed forever
Heartbeat/liveness broken or reclaim disabled. Verify control reclaim, `live:consumer` TTLs, orphan grace.

### Playbook: duplicate side effects after deploy
Rolling bounce caused reclaim. Confirm handlers idempotent; consider longer drain timeout.

### Playbook: schedule backlog
Poller disabled or too few scheduler pods; MySQL/Redis schedule store saturated; raise `schedule_batch_size` carefully.

### Playbook: uniq not deduping
Redis errors (fail-open), fingerprint mismatch across languages, or `uniq_enabled` false / worker missing `uniq true`.

### Playbook: callbacks missing
Check claim stamps, callbacks topic lag, whether job callback topic has consumers, legacy needing CallbackConsumer, reconciler lost-callback recovery.

---

## AM. Config validation FAQ

### What does `validate!` require?
Store/schedule_store symbols, non-empty brokers, redis configured, liveness backend valid, fairness time-lane concurrency constraint, metrics/performance metric ranges when enabled.

### What is `validate_topics_on_boot`?
Optional broker check that configured topics exist — off by default (needs broker at boot).

### Hybrid fair without split ready?
Raises ConfigurationError when both runtimes need fair ready.

---

## AN. Performance and cost

### Redis memory hotspots?
Uniq keys, fair ready lists, workset jobs, schedule ZSET, bitmaps for huge batches, perf hashes, all_index.

### When to disable track_running_jobs?
Very high job rates when `/live` per-job detail isn’t worth the write amplification.

### When to sample performance metrics?
Extreme throughput — lower `performance_metrics_sample_rate` below 1.0.

### Kafka cost drivers?
Partition count, retention on scheduled/DLT, dual control mistake (double consume), oversized fetch.

---

## AO. Testing and matrix

### What cross-runtime tests matter?
Uniq fingerprint parity, partition murmur2 parity, Ruby client → Go poller → Go worker, cancellation, DLT exhausted paths, callback message parity, consumption pause parity.

### Integration env flags (Go)?
`KAFKA_BATCH_INTEGRATION=1` and test Redis URL — see kafka-batch-go README.

---

## AP. Assistant corpus hygiene

### Should the assistant cite live lag numbers?
No — it has no cluster access. Tell operators which UI page to open.

### Should the assistant invent Redis KEYS commands?
No — and never suggest mutating operational keys via the assistant path.

### How to extend the corpus?
Edit `ai/README.md` + `ai/FAQ.md`, then rebuild chunks/embeddings.

---

## AQ. Glossary

| Term | Meaning |
|------|---------|
| CG | Consumer group base |
| SF | SuperFetch concurrency |
| WFQ | Weighted fair queuing |
| Touch | First execution counted toward on_complete |
| Fence | Claim generation token |
| Ready window | Bounded per-tenant Redis list |
| Ingest | Fair Kafka backlog topic |
| Ready | Fair Kafka execution topic |
| Claim window | Outstanding Claimed∨Queued∨Performing budget |
| DLT | Dead letter topic |
| Manifest | YAML job_type → runtime/topic map |
| Preclaimed | Callback claim already won in Lua |
| Reclaim | Re-produce orphaned SuperFetch workset job |
| Seal | Open batch completion gate |
| Daemon mode | Ruby API without Karafka consumers |
| Members | Go in-process Kafka group members |

---

## AR. One-line answers operators love

### Is Redis optional? No.
### Are jobs exactly-once? No — at-least-once.
### Are counts exactly-once per job seq? Yes.
### Can Ruby and Go share a topic? No for execution.
### Can two control planes share topics? No.
### Default Ruby SF? 1.
### Default Go SF? 10.
### Schedule poller default? Off.
### Cancel instantaneous cluster-wide? No — up to cache TTL.
### Priority kills running jobs? No.
### Fairness global off switch (Ruby)? No — per worker.
### Go fairness daemon switch? `fairness_enabled`.
### Ledger in MySQL when store mysql? No — still Redis.
### Assistant can fix my lag? No — docs only; use UI/ops.
