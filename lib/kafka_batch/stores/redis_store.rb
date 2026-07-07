require "redis"
require "connection_pool"
require "securerandom"
require "time"
require_relative "base"

module KafkaBatch
  module Stores
    class RedisStore < Base
      # Redis key layout:
      #   kafka_batch:b:{id}         – Hash of all batch fields (expires after batch_ttl)
      #   kafka_batch:b:seq:{id}     – Integer counter; reserves 1-based batch_seq at enqueue
      #   kafka_batch:b:bitmap:{id}  – Completion dedup bitmap (~1 bit/job; pre-sized at seal)
      #   kafka_batch:offsets        – Hash of monotonic per-partition cursors;
      #                                field = "source_topic/source_partition"
      #                                → last applied source offset. O(num_partitions),
      #                                never grows with job count, no TTL.
      #   kafka_batch:index:running  – ZSET of batch ids, score = created_at epoch
      #   kafka_batch:index:done     – ZSET of finished-but-uncallbacked ids,
      #                                score = finished_at epoch
      #
      # The two index ZSETs power the reconciler (stale_batches /
      # done_batches_without_callback).  Members are pruned as batches advance
      # through their lifecycle, and the reconciler self-heals any stale members
      # (e.g. left behind by a TTL-expired batch) by re-validating actual state.

      KEY_PREFIX    = "kafka_batch:b"
      RUNNING_INDEX = "kafka_batch:index:running"
      DONE_INDEX    = "kafka_batch:index:done"
      # ZSET of every batch id (score = created_at epoch) powering the admin UI
      # listing. Members are pruned lazily when their (TTL-expired) hash is gone.
      ALL_INDEX     = "kafka_batch:index:all"
      # SET of cancelled batch ids, read periodically by the cancellation cache.
      CANCELLED_INDEX = "kafka_batch:index:cancelled"
      # Global (legacy) offsets key — kept for reference.  Active code now uses
      # per-topic sharded keys via offsets_key(source_topic): "kafka_batch:offsets:#{topic}"
      OFFSETS_KEY   = "kafka_batch:offsets"
      # Hash field → integer count for each batch status ("running", "success",
      # "complete", "cancelled").  Maintained atomically inside the Lua scripts
      # and in create_batch / update_batch_status / delete_batch so batch_counts
      # can return in O(1) instead of O(N pipelined HGETs).
      COUNTS_KEY    = "kafka_batch:counts"

      # Atomically apply a completion, deduplicating by batch_seq (1-based bitmap
      # bit) so each job counts at most once regardless of completion order.
      #
      #   KEYS[1] = batch hash
      #   KEYS[2] = per-batch bitmap (kafka_batch:b:bitmap:{batch_id})
      #   KEYS[3] = RUNNING_INDEX zset, KEYS[4] = DONE_INDEX zset
      #   KEYS[5] = COUNTS_KEY hash (Bug #2: O(1) status counters)
      #   ARGV[1] = batch_seq (1-based; must be > 0)
      #   ARGV[2] = counter field ("completed_count"|"failed_count"),
      #   ARGV[3] = ttl, ARGV[4] = now (iso8601),
      #   ARGV[5] = finished_at score (unix float as string, for DONE_INDEX zadd)
      # Returns [code, payload]:
      #   [0, "duplicate"]  – batch_seq already applied (dedup)
      #   [0, "not_found"]  – batch hash does not exist
      #   [0, "invalid"]    – batch_seq missing or out of range
      #   [1, outcome]      – batch just completed; outcome = "success"|"complete"
      #   [2, "continue"]   – still jobs outstanding
      BATCH_DONE_JOB_LUA = <<~LUA.freeze
        local seq = tonumber(ARGV[1])
        if not seq or seq < 1 then return {0, 'invalid'} end

        local bit = seq - 1
        if redis.call('GETBIT', KEYS[2], bit) == 1 then return {0, 'duplicate'} end
        redis.call('SETBIT', KEYS[2], bit, 1)
        redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))

        if redis.call('EXISTS', KEYS[1]) == 0 then return {0, 'not_found'} end

        local status = redis.call('HGET', KEYS[1], 'status')
        if status == 'success' or status == 'complete' or status == 'cancelled' then
          return {0, 'duplicate'}
        end

        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
        redis.call('HINCRBY', KEYS[1], ARGV[2], 1)

        local total     = tonumber(redis.call('HGET', KEYS[1], 'total_jobs'))      or 0
        local completed = tonumber(redis.call('HGET', KEYS[1], 'completed_count')) or 0
        local failed    = tonumber(redis.call('HGET', KEYS[1], 'failed_count'))    or 0
        local sealed    = redis.call('HGET', KEYS[1], 'locked_at')

        -- Only finalize once the batch is sealed (block-form population finished,
        -- or created bare). A still-held batch may keep growing, so don't fire.
        if (completed + failed) >= total and sealed and sealed ~= '' then
          local outcome = (failed > 0) and 'complete' or 'success'
          redis.call('HSET', KEYS[1], 'status',      outcome)
          redis.call('HSET', KEYS[1], 'finished_at', ARGV[4])
          redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
          -- Atomically move from RUNNING_INDEX to DONE_INDEX so no intermediate
          -- state is visible if the process crashes after Lua returns.
          local batch_id = redis.call('HGET', KEYS[1], 'id')
          if batch_id then
            redis.call('ZREM', KEYS[3], batch_id)
            redis.call('ZADD', KEYS[4], tonumber(ARGV[5]), batch_id)
          end
          -- Bug #2: keep COUNTS_KEY in sync atomically with finalization so
          -- batch_counts() can be answered in O(1) without scanning ALL_INDEX.
          redis.call('HINCRBY', KEYS[5], 'running', -1)
          redis.call('HINCRBY', KEYS[5], outcome, 1)
          return {1, outcome}
        end

        return {2, 'continue'}
      LUA

      # Atomically claim callback dispatch rights.
      # HSETNX returns 1 if field was absent (we won the race), 0 if already set.
      # Guarded by EXISTS so a stale message for a TTL-expired batch does not
      # recreate a partial, TTL-less hash (orphan key); returns 0 in that case.
      #   KEYS[1] = batch hash, KEYS[2] = DONE_INDEX zset
      #   ARGV[1] = now (iso8601), ARGV[2] = dispatched_by, ARGV[3] = batch id
      # #8 fix: ZREM DONE_INDEX inside Lua so claim + index removal are atomic.
      CLAIM_CALLBACK_LUA = <<~LUA.freeze
        if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
        local won = redis.call('HSETNX', KEYS[1], 'callback_dispatched_at', ARGV[1])
        if won == 1 then
          if ARGV[2] ~= '' then
            redis.call('HSET', KEYS[1], 'callback_dispatched_by', ARGV[2])
          end
          -- #8 fix: remove from DONE_INDEX atomically with the claim so the
          -- reconciler cannot re-fire a callback whose claim just succeeded.
          redis.call('ZREM', KEYS[2], ARGV[3])
        end
        return won
      LUA

      # Atomically create a batch record only if it does not already exist.
      # Uses HSETNX on the 'id' field as an existence sentinel.
      # Returns 1 if created, 0 if already existed.
      CREATE_BATCH_LUA = <<~LUA.freeze
        local created = redis.call('HSETNX', KEYS[1], 'id', ARGV[1])
        if created == 0 then return 0 end
        redis.call('HMSET', KEYS[1],
          'total_jobs',      ARGV[2],
          'completed_count', '0',
          'failed_count',    '0',
          'status',          'running',
          'on_success',      ARGV[3],
          'on_complete',     ARGV[4],
          'meta',            ARGV[5],
          'created_at',      ARGV[6],
          'locked_at',       ARGV[8],
          'description',     ARGV[9],
          'tenant_id',       ARGV[10]
        )
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[7]))
        return 1
      LUA

      # Grow an open batch's total_jobs and (for positive counts) reserve a
      # contiguous run of 1-based batch_seq values via KEYS[2] (seq counter).
      # Returns:
      #   0 not_found | 2 cancelled | 3 closed (finalized)
      #   {1, seq_start, seq_end} on success when count > 0
      #   {1} on success when count <= 0 (rollback — seq counter unchanged)
      ADD_JOBS_LUA = <<~LUA.freeze
        if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
        local status = redis.call('HGET', KEYS[1], 'status')
        if status == 'cancelled' then return 2 end
        if status == 'success' or status == 'complete' then return 3 end
        local dispatched = redis.call('HGET', KEYS[1], 'callback_dispatched_at')
        if dispatched and dispatched ~= '' then return 3 end
        local n = tonumber(ARGV[1])
        local ttl = tonumber(ARGV[2])
        redis.call('HINCRBY', KEYS[1], 'total_jobs', n)
        redis.call('EXPIRE', KEYS[1], ttl)
        if n > 0 then
          local seq_end = redis.call('INCRBY', KEYS[2], n)
          redis.call('EXPIRE', KEYS[2], ttl)
          return {1, seq_end - n + 1, seq_end}
        end
        return {1}
      LUA

      # Seal a batch (open its completion gate) and finalize if already drained.
      # Pre-allocates the completion bitmap (KEYS[5]) via SETBIT at index
      # total_jobs (one past the last live bit total_jobs-1) so the completion
      # storm does not grow the string incrementally without touching dedup bits.
      #   KEYS[1] = batch hash, KEYS[2] = COUNTS_KEY (Bug #2: O(1) status counters)
      #   KEYS[3] = RUNNING_INDEX zset, KEYS[4] = DONE_INDEX zset (#7 fix: atomic move)
      #   KEYS[5] = per-batch bitmap
      #   ARGV[1] = now (iso8601), ARGV[2] = ttl, ARGV[3] = now (unix float for DONE_INDEX score)
      # Returns [0,'not_found'] | [1,outcome] (just finalized) | [2,'sealed']
      SEAL_BATCH_LUA = <<~LUA.freeze
        if redis.call('EXISTS', KEYS[1]) == 0 then return {0, 'not_found'} end
        local status = redis.call('HGET', KEYS[1], 'status')
        local sealed = redis.call('HGET', KEYS[1], 'locked_at')
        if not sealed or sealed == '' then
          redis.call('HSET', KEYS[1], 'locked_at', ARGV[1])
        end
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))

        local total = tonumber(redis.call('HGET', KEYS[1], 'total_jobs')) or 0
        if total > 0 then
          redis.call('SETBIT', KEYS[5], total, 0)
          redis.call('EXPIRE', KEYS[5], tonumber(ARGV[2]))
        end

        if status == 'running' then
          local total     = tonumber(redis.call('HGET', KEYS[1], 'total_jobs'))      or 0
          local completed = tonumber(redis.call('HGET', KEYS[1], 'completed_count')) or 0
          local failed    = tonumber(redis.call('HGET', KEYS[1], 'failed_count'))    or 0
          if (completed + failed) >= total then
            local outcome = (failed > 0) and 'complete' or 'success'
            redis.call('HSET', KEYS[1], 'status', outcome)
            redis.call('HSET', KEYS[1], 'finished_at', ARGV[1])
            -- Bug #2: keep COUNTS_KEY in sync atomically with finalization.
            redis.call('HINCRBY', KEYS[2], 'running', -1)
            redis.call('HINCRBY', KEYS[2], outcome, 1)
            -- #7 fix: move the batch from RUNNING_INDEX to DONE_INDEX inside Lua
            -- so a process crash between Lua return and the Ruby ZREM/ZADD can
            -- never strand the id in RUNNING_INDEX or miss DONE_INDEX.
            local batch_id = redis.call('HGET', KEYS[1], 'id')
            if batch_id then
              redis.call('ZREM', KEYS[3], batch_id)
              redis.call('ZADD', KEYS[4], tonumber(ARGV[3]), batch_id)
            end
            return {1, outcome}
          end
        end
        return {2, 'sealed'}
      LUA

      # Distributed reconciler lock via SET NX EX.
      # Returns 1 if lock acquired, 0 otherwise.
      ACQUIRE_LOCK_LUA = <<~LUA.freeze
        return redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2]))
      LUA

      RELEASE_LOCK_LUA = <<~LUA.freeze
        if redis.call('GET', KEYS[1]) == ARGV[1] then
          redis.call('DEL', KEYS[1])
          return 1
        end
        return 0
      LUA

      # Record (upsert) a failure with a per-batch cap + TTL.
      #   KEYS[1] failures hash; ARGV[1] job_id, ARGV[2] entry, ARGV[3] ttl, ARGV[4] cap
      # Existing jobs always update (status/attempt change). A brand-new failing
      # job is skipped once the cap is reached, bounding RAM – the real job data
      # remains durable in Kafka, so this only trims the dashboard view.
      # Returns 1 if stored, 0 if skipped due to the cap.
      RECORD_FAILURE_LUA = <<~LUA.freeze
        local cap = tonumber(ARGV[4])
        if cap > 0 and redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
          if redis.call('HLEN', KEYS[1]) >= cap then
            return 0
          end
        end
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
        return 1
      LUA

      # Transition a batch to a terminal outcome only when it is still running.
      #   KEYS[1]=batch hash  KEYS[2]=COUNTS_KEY  KEYS[3]=RUNNING_INDEX  KEYS[4]=DONE_INDEX
      #   ARGV[1]=outcome  ARGV[2]=finished_at  ARGV[3]=done_score  ARGV[4]=batch_id
      MARK_FINISHED_IF_RUNNING_LUA = <<~LUA.freeze
        if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
        if redis.call('HGET', KEYS[1], 'status') ~= 'running' then return 0 end
        redis.call('HSET', KEYS[1], 'status', ARGV[1])
        redis.call('HSET', KEYS[1], 'finished_at', ARGV[2])
        redis.call('ZREM', KEYS[3], ARGV[4])
        redis.call('ZADD', KEYS[4], tonumber(ARGV[3]), ARGV[4])
        redis.call('HINCRBY', KEYS[2], 'running', -1)
        redis.call('HINCRBY', KEYS[2], ARGV[1], 1)
        return 1
      LUA

      def initialize
        cfg = KafkaBatch.config
        @pool = ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
          KafkaBatch::RedisClient.new(cfg) || raise(ConfigurationError, "Redis is not configured")
        end
        @ttl          = cfg.batch_ttl
        # Failure metadata is a UI convenience (real data lives in Kafka), so it
        # gets its own shorter TTL and an optional per-batch cap to bound RAM.
        @failures_ttl = (cfg.failures_ttl || cfg.batch_ttl).to_i
        @failures_cap = cfg.max_failures_per_batch.to_i
      end

      # ── Public interface ──────────────────────────────────────────────────

      def create_batch(id:, total_jobs:, on_success: nil, on_complete: nil, meta: {}, description: nil, tenant_id: nil, sealed: true)
        key = batch_key(id)
        now = Time.now
        with_redis do |r|
          created = r.eval(CREATE_BATCH_LUA,
            keys: [key],
            argv: [
              id,
              total_jobs.to_s,
              on_success.to_s,
              on_complete.to_s,
              serialize(meta),
              now.iso8601,
              @ttl.to_s,
              sealed ? now.iso8601 : "",
              description.to_s,
              tenant_id.to_s       # ARGV[10]
            ]
          )
          # Returns 1 if created, 0 if already existed (idempotent).
          if created == 1
            # Register in the running index (reconciler) and the all index (UI).
            r.zadd(RUNNING_INDEX, now.to_f, id)
            r.zadd(ALL_INDEX, now.to_f, id)
            # Bug #2: maintain O(1) status counters.
            r.hincrby(COUNTS_KEY, "running", 1)
            # Bug #2: cap ALL_INDEX so it never grows unbounded at 500 pods × 7-day TTL.
            max_size = KafkaBatch.config.all_index_max_size.to_i
            r.zremrangebyrank(ALL_INDEX, 0, -(max_size + 1)) if max_size > 0
          end
          created
        end
      end

      def find_batch(id)
        with_redis do |r|
          h = r.hgetall(batch_key(id))
          return nil if h.nil? || h.empty?
          hash_to_batch(h)
        end
      end

      def record_completion_by_offset(batch_id:, job_id:, source_topic:, source_partition:, source_offset:, status:, batch_seq:)
        return { status: :invalid } if batch_seq.nil? || batch_seq.to_i <= 0

        field = status == "success" ? "completed_count" : "failed_count"
        now   = Time.now.iso8601

        result = with_redis do |r|
          r.eval(BATCH_DONE_JOB_LUA,
            keys: completion_lua_keys(batch_id),
            argv: completion_lua_argv(batch_seq: batch_seq, field: field, now: now)
          )
        end

        code, payload = result
        case code
        when 0 then { status: payload.to_sym } # :duplicate, :not_found, :invalid
        when 1
          { status: :done, outcome: payload, batch: find_batch(batch_id) }
        when 2 then { status: :continue }
        end
      end

      # Batched counter application for a whole Kafka poll. Reuses the proven
      # per-event Lua (atomic dedup + increment + finalize) but pipelines all the
      # events in one network round-trip. Each event is still exactly-once via
      # batch_seq bitmap (O(1) per event), so nothing is double-counted.
      #
      # @return [Hash]
      #   :finished [Array<Hash>] { batch:, outcome: } for batches that just finished
      #   :replays  [Array<String>] batch_ids whose events were DEDUPED (Lua
      #     returned 'duplicate') — the only batches that can be already-finalized
      #     with an undispatched callback on a redelivery. In steady state (first
      #     delivery) this is empty, so the EventConsumer does zero extra reads.
      def record_completions_batch(events)
        return { finished: [], replays: [] } if events.empty?
        now = Time.now.iso8601

        now_float = Time.now.to_f.to_s
        results = with_redis do |r|
          r.pipelined do |pipe|
            events.each do |e|
              field = e[:status] == "success" ? "completed_count" : "failed_count"
              pipe.eval(BATCH_DONE_JOB_LUA,
                keys: completion_lua_keys(e[:batch_id]),
                argv: completion_lua_argv(
                  batch_seq: e[:batch_seq], field: field,
                  now: now, now_float: now_float
                ))
            end
          end
        end

        # One pass over the results: code 1 = just finalized (needs a callback),
        # code 0 + 'duplicate' = replayed event (candidate for inline callback
        # re-fire). code 2 = still counting → nothing to do.
        finalized_indices = []
        replays           = []
        results.each_with_index do |res, i|
          case res[0]
          when 1 then finalized_indices << i
          when 0 then replays << events[i][:batch_id] if res[1] == "duplicate"
          end
        end
        replays.uniq!

        return { finished: [], replays: replays } if finalized_indices.empty?

        # #29 fix: pipeline all finalized-batch HGETALLs in ONE round-trip instead
        # of calling find_batch (one HGETALL each) sequentially inside the loop.
        batch_hashes = with_redis do |r|
          r.pipelined do |pipe|
            finalized_indices.each { |i| pipe.hgetall(batch_key(events[i][:batch_id])) }
          end
        end

        finished = finalized_indices.each_with_index.map do |idx, j|
          _code, payload = results[idx]
          h = batch_hashes[j]
          { batch: (h && !h.empty? ? hash_to_batch(h) : nil), outcome: payload }
        end.compact

        { finished: finished, replays: replays }
      end

      def add_jobs(id, count)
        result = with_redis do |r|
          r.eval(ADD_JOBS_LUA,
            keys: [batch_key(id), seq_key(id)],
            argv: [count.to_i.to_s, @ttl.to_s])
        end
        case result
        when Integer
          add_jobs_status(result)
        when Array
          code = result[0]
          status = add_jobs_status(code)
          return status unless status == :ok && count.to_i.positive? && result[1]

          { status: :ok, seq_start: result[1].to_i, seq_end: result[2].to_i }
        end
      end

      def seal_batch(id)
        now     = Time.now
        now_iso = now.iso8601
        now_f   = now.to_f.to_s
        result = with_redis do |r|
          # Bug #2: KEYS[2]=COUNTS_KEY keeps status counters in sync.
          # #7 fix: KEYS[3]=RUNNING_INDEX, KEYS[4]=DONE_INDEX so the index move
          # happens atomically inside Lua — no split-brain if the process crashes
          # between the Lua return and the former Ruby-side ZREM/ZADD.
          r.eval(SEAL_BATCH_LUA,
            keys: [batch_key(id), COUNTS_KEY, RUNNING_INDEX, DONE_INDEX, bitmap_key(id)],
            argv: [now_iso, @ttl.to_s, now_f]
          )
        end
        code, payload = result
        case code
        when 0 then { status: :not_found }
        when 1 then { status: :done, outcome: payload, batch: find_batch(id) }
        when 2 then { status: :sealed }
        end
      end

      def claim_callback(id, dispatched_by = nil)
        now = Time.now.iso8601
        # #8 fix: KEYS[2]=DONE_INDEX and ARGV[3]=id so the ZREM happens inside
        # the Lua script atomically with the HSETNX claim, preventing a crash
        # between the two from leaving the batch stranded in DONE_INDEX.
        result = with_redis do |r|
          r.eval(CLAIM_CALLBACK_LUA,
            keys: [batch_key(id), DONE_INDEX],
            argv: [now, dispatched_by.to_s, id]
          )
        end
        result == 1
      end

      def callback_dispatched?(id)
        with_redis do |r|
          !presence(r.hget(batch_key(id), "callback_dispatched_at")).nil?
        end
      end

      def update_batch_status(id, status)
        with_redis do |r|
          # Bug #2: read old status so COUNTS_KEY can be adjusted correctly.
          old_status = r.hget(batch_key(id), "status")
          r.hset(batch_key(id), "status", status)
          # Terminal/cancelled batches drop out of the running index.
          r.zrem(RUNNING_INDEX, id) if %w[success complete cancelled].include?(status)
          # Bug #2: maintain COUNTS_KEY.  old_status may be nil if the key expired.
          if old_status && old_status != status
            r.hincrby(COUNTS_KEY, old_status, -1)
            r.hincrby(COUNTS_KEY, status,     1)
          end
          # Bug #6: CANCELLED_INDEX is now a ZSET (scored by timestamp) so old
          # entries can be pruned cheaply with ZREMRANGEBYSCORE.
          zadd_cancelled(r, id) if status == "cancelled"
        end
      end

      def batch_status(id)
        with_redis { |r| presence(r.hget(batch_key(id), "status")) }
      end

      # Bug #6: CANCELLED_INDEX is now a ZSET scored by cancellation timestamp.
      # We only need IDs cancelled within the last 2 × batch_ttl window (any
      # older job record will have expired from Redis anyway).  Pruning old
      # entries and reading recent ones happen in a single pipelined round-trip.
      def cancelled_batch_ids
        with_redis do |r|
          cutoff = (Time.now.to_f - 2 * @ttl)
          begin
            results = r.pipelined do |pipe|
              pipe.zremrangebyscore(CANCELLED_INDEX, "-inf", cutoff)  # prune stale
              pipe.zrangebyscore(CANCELLED_INDEX, cutoff, "+inf")      # read active
            end
            results[1] || []
          rescue Redis::CommandError => e
            raise unless e.message.include?("WRONGTYPE")
            # Pre-upgrade deployment had a plain SET — migrate it once on first read.
            migrate_cancelled_index_to_zset(r)
            r.zrangebyscore(CANCELLED_INDEX, cutoff, "+inf")
          end
        end
      end

      def record_failure(batch_id:, job_id:, worker_class:, error_class:, error_message:, attempt: 0, status: "failed", next_retry_at: nil)
        entry = Oj.dump({
          "job_id"        => job_id,
          "worker_class"  => worker_class.to_s,
          "error_class"   => error_class.to_s,
          "error_message" => error_message.to_s,
          "attempt"       => attempt.to_i,
          "status"        => status,
          "next_retry_at" => (next_retry_at.respond_to?(:iso8601) ? next_retry_at.iso8601 : next_retry_at),
          "failed_at"     => Time.now.iso8601
        }, mode: :compat)

        with_redis do |r|
          r.eval(RECORD_FAILURE_LUA,
            keys: [failures_key(batch_id)],
            argv: [job_id, entry, @failures_ttl.to_s, @failures_cap.to_s]
          )
        end
      end

      def clear_failure(batch_id, job_id)
        with_redis { |r| r.hdel(failures_key(batch_id), job_id) }
      end

      def list_failures(batch_id, limit: 100, offset: 0)
        raw = with_redis { |r| r.hvals(failures_key(batch_id)) }
        entries = raw.map { |v| failure_hash(deserialize(v)) }
        sort_paginate(entries, limit, offset)
      end

      # Bug #10 fix: pipeline all hvals calls into one round-trip instead of
      # O(N) sequential round-trips (one per batch ID).
      def list_all_failures(limit: 100, offset: 0, status: nil)
        ids = with_redis { |r| r.zrevrange(ALL_INDEX, 0, -1) }
        return sort_paginate([], limit, offset) if ids.empty?

        all_vals = with_redis do |r|
          r.pipelined do |pipe|
            ids.each { |bid| pipe.hvals(failures_key(bid)) }
          end
        end

        entries = []
        ids.zip(all_vals).each do |bid, vals|
          next if vals.nil? || vals.empty?
          vals.each do |v|
            h = deserialize(v)
            next if status && h["status"] != status
            entries << failure_hash(h, batch_id: bid)
          end
        end
        sort_paginate(entries, limit, offset)
      end

      # Bug #5 fix: pipeline all hgetall calls into a single round-trip instead
      # of calling find_batch (one HGETALL each) per ID in a Ruby loop.
      def list_batches(status: nil, limit: 50, offset: 0, search: nil)
        ids = with_redis { |r| r.zrevrange(ALL_INDEX, 0, -1) }
        return [] if ids.empty?

        q = presence(search)&.downcase

        raw_batches = with_redis do |r|
          r.pipelined do |pipe|
            ids.each { |id| pipe.hgetall(batch_key(id)) }
          end
        end

        expired = []
        result  = []
        skipped = 0

        ids.zip(raw_batches).each do |id, h|
          if h.nil? || h.empty?
            expired << id
            next
          end
          batch = hash_to_batch(h)
          next if status && batch[:status] != status
          next if q && !batch_matches?(batch, q)

          if skipped < offset
            skipped += 1
            next
          end

          result << batch
          break if result.size >= limit
        end

        # Lazily prune expired entries from the index and fix COUNTS_KEY drift.
        with_redis { |r| expired.each { |id| drop_expired_batch_from_indexes(r, id) } } unless expired.empty?

        result
      end

      def pending_jobs_total
        ids = with_redis { |r| r.zrange(ALL_INDEX, 0, -1) }
        return 0 if ids.empty?

        rows = with_redis do |r|
          r.pipelined do |p|
            ids.each { |id| p.hmget(batch_key(id), "status", "total_jobs", "completed_count", "failed_count") }
          end
        end

        rows.sum do |h|
          next 0 if h[0].nil? || h[0] != "running"
          [h[1].to_i - h[2].to_i - h[3].to_i, 0].max
        end
      end

      # Bug #2 fix: O(1) fast path via the pre-maintained COUNTS_KEY hash.
      # Falls back to the full pipeline scan only on first call after deployment
      # (before COUNTS_KEY is populated) or if the key was evicted/flushed.
      def batch_counts
        with_redis do |r|
          raw = r.hgetall(COUNTS_KEY)
          return raw.transform_values(&:to_i) unless raw.empty?

          # Fallback: rebuild counts from ALL_INDEX via pipelined HGET.
          ids = r.zrange(ALL_INDEX, 0, -1)
          return {} if ids.empty?

          statuses = r.pipelined do |pipe|
            ids.each { |id| pipe.hget(batch_key(id), "status") }
          end

          counts  = Hash.new(0)
          expired = []
          ids.zip(statuses).each do |id, st|
            if st.nil?
              expired << id
            else
              counts[st] += 1
            end
          end

          expired.each { |id| drop_expired_batch_from_indexes(r, id) } unless expired.empty?
          counts
        end
      end

      # Rebuild COUNTS_KEY from live batch hashes (reconciler / drift heal).
      def reconcile_batch_counts!
        with_redis do |r|
          ids = r.zrange(ALL_INDEX, 0, -1)
          counts = Hash.new(0)
          expired = []

          unless ids.empty?
            statuses = r.pipelined { |pipe| ids.each { |id| pipe.hget(batch_key(id), "status") } }
            ids.zip(statuses).each do |id, st|
              if st.nil? || st.empty?
                expired << id
              else
                counts[st] += 1
              end
            end
          end

          expired.each { |id| drop_expired_batch_from_indexes(r, id) }

          r.del(COUNTS_KEY)
          counts.each { |k, v| r.hset(COUNTS_KEY, k, v) if v.positive? }
          counts
        end
      end

      def mark_finished(id, outcome)
        now = Time.now
        result = with_redis do |r|
          r.eval(
            MARK_FINISHED_IF_RUNNING_LUA,
            keys: [batch_key(id), COUNTS_KEY, RUNNING_INDEX, DONE_INDEX],
            argv: [outcome, now.iso8601, now.to_f.to_s, id]
          )
        end
        result == 1
      end

      # Batches still in the running index that were created before +older_than+.
      # Self-heals the index by dropping members that have expired or already
      # advanced past "running".
      # #9 fix: pipeline all HGETALL calls into one round-trip instead of N
      # sequential find_batch calls (one HGETALL each).
      def stale_batches(older_than:)
        ids = with_redis do |r|
          r.zrangebyscore(RUNNING_INDEX, "-inf", older_than.to_f)
        end
        return [] if ids.empty?

        raw_hashes = with_redis do |r|
          r.pipelined { |pipe| ids.each { |id| pipe.hgetall(batch_key(id)) } }
        end

        stale    = []
        to_prune = []
        ids.zip(raw_hashes).each do |id, h|
          if h.nil? || h.empty?
            to_prune << id  # expired – prune
          else
            batch = hash_to_batch(h)
            if batch[:status] != "running"
              to_prune << id  # already advanced – prune
            else
              stale << batch
            end
          end
        end

        unless to_prune.empty?
          with_redis { |r| r.pipelined { |pipe| to_prune.each { |id| pipe.zrem(RUNNING_INDEX, id) } } }
        end

        stale
      end

      # Batches in the done index that finished before +older_than+ but whose
      # callback was never dispatched.  Prunes expired or already-dispatched ids.
      # #9 fix: pipeline all HGETALL calls into one round-trip.
      def done_batches_without_callback(older_than:)
        ids = with_redis do |r|
          r.zrangebyscore(DONE_INDEX, "-inf", older_than.to_f)
        end
        return [] if ids.empty?

        raw_hashes = with_redis do |r|
          r.pipelined { |pipe| ids.each { |id| pipe.hgetall(batch_key(id)) } }
        end

        pending  = []
        to_prune = []
        ids.zip(raw_hashes).each do |id, h|
          if h.nil? || h.empty?
            to_prune << id  # expired – prune
          else
            batch = hash_to_batch(h)
            if !batch[:callback_dispatched_at].nil?
              to_prune << id  # already dispatched – prune
            elsif !%w[success complete].include?(batch[:status])
              to_prune << id  # not actually done – prune
            else
              pending << batch
            end
          end
        end

        unless to_prune.empty?
          with_redis { |r| r.pipelined { |pipe| to_prune.each { |id| pipe.zrem(DONE_INDEX, id) } } }
        end

        pending
      end

      def delete_batch(id)
        with_redis do |r|
          # Bug #2: decrement COUNTS_KEY before erasing the batch hash.
          st = r.hget(batch_key(id), "status")
          r.del(batch_key(id), failures_key(id), bitmap_key(id), seq_key(id))
          r.zrem(RUNNING_INDEX, id)
          r.zrem(DONE_INDEX, id)
          r.zrem(ALL_INDEX, id)
          # Bug #6: CANCELLED_INDEX is now a ZSET — zrem works for both types.
          r.zrem(CANCELLED_INDEX, id)
          r.hincrby(COUNTS_KEY, st, -1) if st
        end
      end

      # Distributed lock using SET NX EX.
      # Yields only if this process acquires the lock; silently skips otherwise.
      # @param ttl [Integer] lock expiry in seconds
      def with_reconciler_lock(ttl: 300)
        lock_key   = "#{KEY_PREFIX}:reconciler_lock"
        token      = SecureRandom.hex(16)

        acquired = with_redis do |r|
          r.eval(ACQUIRE_LOCK_LUA,
            keys: [lock_key],
            argv: [token, ttl.to_s]
          )
        end

        return unless acquired == "OK"

        begin
          yield
        ensure
          with_redis do |r|
            r.eval(RELEASE_LOCK_LUA, keys: [lock_key], argv: [token])
          end
        end
      rescue StandardError => e
        # Best-effort sweep: swallow + log (consistent with MysqlStore) so a
        # reconciler error never crashes the scheduler. The lock is released
        # by the ensure block above before we get here.
        KafkaBatch.logger.error("[KafkaBatch][RedisStore] Reconciler lock error: #{e.message}")
      end

      # Raw Redis for dashboard metadata keys (reconciler / DLT stats).
      def with_redis(&block)
        @pool.with(&block)
      rescue Redis::BaseError => e
        raise StoreError, "Redis error: #{e.message}"
      end

      private

      def batch_key(id)
        "#{KEY_PREFIX}:#{id}"
      end

      def failures_key(id)
        "#{batch_key(id)}:failures"
      end

      def bitmap_key(batch_id)
        "#{KEY_PREFIX}:bitmap:#{batch_id}"
      end

      def seq_key(batch_id)
        "#{KEY_PREFIX}:seq:#{batch_id}"
      end

      def add_jobs_status(code)
        case code
        when 0 then :not_found
        when 1 then :ok
        when 2 then :cancelled
        when 3 then :closed
        end
      end

      def completion_lua_keys(batch_id)
        [batch_key(batch_id), bitmap_key(batch_id),
         RUNNING_INDEX, DONE_INDEX, COUNTS_KEY]
      end

      def completion_lua_argv(batch_seq:, field:, now:, now_float: nil)
        [batch_seq.to_i.to_s, field, @ttl.to_s, now, (now_float || Time.now.to_f.to_s)]
      end

      # Legacy per-topic offset keys (pre-bitmap). Retained for reference only.
      def offsets_key(source_topic)
        "kafka_batch:offsets:#{source_topic}"
      end

      # Bug #6: write a cancellation entry to the ZSET-backed CANCELLED_INDEX.
      # Handles a one-time migration from the legacy SET type with a WRONGTYPE rescue.
      def zadd_cancelled(r, id)
        r.zadd(CANCELLED_INDEX, Time.now.to_f, id)
      rescue Redis::CommandError => e
        raise unless e.message.include?("WRONGTYPE")
        migrate_cancelled_index_to_zset(r)
        r.zadd(CANCELLED_INDEX, Time.now.to_f, id)
      end

      # Convert the legacy plain-SET CANCELLED_INDEX to a ZSET in-place.
      # Called at most once per Redis instance (right after a gem upgrade).
      def migrate_cancelled_index_to_zset(r)
        ids = r.smembers(CANCELLED_INDEX) rescue []
        r.del(CANCELLED_INDEX)
        now_f = Time.now.to_f
        # Score all legacy entries as "now" — they will age out naturally.
        r.zadd(CANCELLED_INDEX, ids.flat_map { |i| [now_f, i] }) unless ids.empty?
      end

      def failure_hash(h, batch_id: nil)
        out = {
          job_id:        h["job_id"],
          worker_class:  h["worker_class"],
          error_class:   h["error_class"],
          error_message: h["error_message"],
          attempt:       h["attempt"].to_i,
          status:        h["status"],
          next_retry_at: h["next_retry_at"],
          failed_at:     h["failed_at"]
        }
        out[:batch_id] = batch_id if batch_id
        out
      end

      def sort_paginate(entries, limit, offset)
        entries.sort_by { |e| e[:failed_at].to_s }.reverse.drop(offset).first(limit)
      end

      def hash_to_batch(h)
        {
          id:                     h["id"],
          total_jobs:             h["total_jobs"].to_i,
          completed_count:        h["completed_count"].to_i,
          failed_count:           h["failed_count"].to_i,
          status:                 h["status"],
          on_success:             presence(h["on_success"]),
          on_complete:            presence(h["on_complete"]),
          description:            presence(h["description"]),
          tenant_id:              presence(h["tenant_id"]),
          meta:                   deserialize(h["meta"]),
          created_at:             h["created_at"],
          finished_at:            h["finished_at"],
          callback_dispatched_at: presence(h["callback_dispatched_at"]),
          callback_dispatched_by: presence(h["callback_dispatched_by"]),
          locked_at:              presence(h["locked_at"])
        }
      end

      # Case-insensitive match of a batch against a search query (id or description).
      def batch_matches?(batch, downcased_query)
        batch[:id].to_s.downcase.include?(downcased_query) ||
          batch[:description].to_s.downcase.include?(downcased_query)
      end

      def serialize(obj)
        return "" if obj.nil? || (obj.respond_to?(:empty?) && obj.empty?)
        Oj.dump(obj, mode: :compat)
      end

      def deserialize(str)
        return {} if str.nil? || str.empty?
        Oj.load(str)
      rescue Oj::ParseError
        {}
      end

      def presence(str)
        (str.nil? || str.empty?) ? nil : str
      end

      # Batch hash expired (TTL) but index entries linger — prune and fix COUNTS_KEY.
      def drop_expired_batch_from_indexes(r, id)
        if r.zscore(RUNNING_INDEX, id)
          r.hincrby(COUNTS_KEY, "running", -1)
        else
          st = r.hget(batch_key(id), "status")
          r.hincrby(COUNTS_KEY, st, -1) if st && !st.empty?
        end
        r.zrem(ALL_INDEX, id)
        r.zrem(RUNNING_INDEX, id)
        r.zrem(DONE_INDEX, id)
      end
    end
  end
end
