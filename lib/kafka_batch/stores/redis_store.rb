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
      OFFSETS_KEY   = "kafka_batch:offsets"
      HEARTBEATS_KEY = "kafka_batch:heartbeats"

      # Atomically apply a completion, deduplicating by a monotonic per-partition
      # cursor over the job message's source offset, then check for completion.
      #   KEYS[1] = batch hash, KEYS[2] = offsets hash
      #   ARGV[1] = "topic/partition" field, ARGV[2] = source_offset,
      #   ARGV[3] = counter field, ARGV[4] = ttl, ARGV[5] = now (iso8601)
      # Returns [code, payload]:
      #   [0, "duplicate"]  – source_offset already applied (dedup)
      #   [0, "not_found"]  – batch hash does not exist
      #   [1, outcome]      – batch just completed; outcome = "success"|"complete"
      #   [2, "continue"]   – still jobs outstanding
      BATCH_DONE_OFFSET_LUA = <<~LUA.freeze
        local last = redis.call('HGET', KEYS[2], ARGV[1])
        if last and tonumber(ARGV[2]) <= tonumber(last) then return {0, 'duplicate'} end

        -- Advance the monotonic cursor first; the event is now "applied".
        redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])

        if redis.call('EXISTS', KEYS[1]) == 0 then return {0, 'not_found'} end

        local status = redis.call('HGET', KEYS[1], 'status')
        if status == 'success' or status == 'complete' or status == 'cancelled' then
          return {0, 'duplicate'}
        end

        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
        redis.call('HINCRBY', KEYS[1], ARGV[3], 1)

        local total     = tonumber(redis.call('HGET', KEYS[1], 'total_jobs'))      or 0
        local completed = tonumber(redis.call('HGET', KEYS[1], 'completed_count')) or 0
        local failed    = tonumber(redis.call('HGET', KEYS[1], 'failed_count'))    or 0
        local sealed    = redis.call('HGET', KEYS[1], 'locked_at')

        -- Only finalize once the batch is sealed (block-form population finished,
        -- or created bare). A still-held batch may keep growing, so don't fire.
        if (completed + failed) >= total and sealed and sealed ~= '' then
          local outcome = (failed > 0) and 'complete' or 'success'
          redis.call('HSET', KEYS[1], 'status',      outcome)
          redis.call('HSET', KEYS[1], 'finished_at', ARGV[5])
          redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4]))
          return {1, outcome}
        end

        return {2, 'continue'}
      LUA

      # Atomically claim callback dispatch rights.
      # HSETNX returns 1 if field was absent (we won the race), 0 if already set.
      # Guarded by EXISTS so a stale message for a TTL-expired batch does not
      # recreate a partial, TTL-less hash (orphan key); returns 0 in that case.
      CLAIM_CALLBACK_LUA = <<~LUA.freeze
        if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
        local won = redis.call('HSETNX', KEYS[1], 'callback_dispatched_at', ARGV[1])
        if won == 1 and ARGV[2] ~= '' then
          redis.call('HSET', KEYS[1], 'callback_dispatched_by', ARGV[2])
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
          'description',     ARGV[9]
        )
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[7]))
        return 1
      LUA

      # Grow an open batch's total_jobs. An open batch accepts jobs regardless of
      # whether it is sealed – only completed/cancelled batches are closed.
      # Returns: 0 not_found | 1 ok | 2 cancelled | 3 closed (finalized)
      ADD_JOBS_LUA = <<~LUA.freeze
        if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
        local status = redis.call('HGET', KEYS[1], 'status')
        if status == 'cancelled' then return 2 end
        if status == 'success' or status == 'complete' then return 3 end
        local dispatched = redis.call('HGET', KEYS[1], 'callback_dispatched_at')
        if dispatched and dispatched ~= '' then return 3 end
        redis.call('HINCRBY', KEYS[1], 'total_jobs', tonumber(ARGV[1]))
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
        return 1
      LUA

      # Seal a batch (open its completion gate) and finalize if already drained.
      #   ARGV[1] = now (iso8601), ARGV[2] = ttl
      # Returns [0,'not_found'] | [1,outcome] (just finalized) | [2,'sealed']
      SEAL_BATCH_LUA = <<~LUA.freeze
        if redis.call('EXISTS', KEYS[1]) == 0 then return {0, 'not_found'} end
        local status = redis.call('HGET', KEYS[1], 'status')
        local sealed = redis.call('HGET', KEYS[1], 'locked_at')
        if not sealed or sealed == '' then
          redis.call('HSET', KEYS[1], 'locked_at', ARGV[1])
        end
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))

        if status == 'running' then
          local total     = tonumber(redis.call('HGET', KEYS[1], 'total_jobs'))      or 0
          local completed = tonumber(redis.call('HGET', KEYS[1], 'completed_count')) or 0
          local failed    = tonumber(redis.call('HGET', KEYS[1], 'failed_count'))    or 0
          if (completed + failed) >= total then
            local outcome = (failed > 0) and 'complete' or 'success'
            redis.call('HSET', KEYS[1], 'status', outcome)
            redis.call('HSET', KEYS[1], 'finished_at', ARGV[1])
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

      def initialize
        cfg = KafkaBatch.config
        @pool = ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
          Redis.new(url: cfg.redis_url)
        end
        @ttl          = cfg.batch_ttl
        # Failure metadata is a UI convenience (real data lives in Kafka), so it
        # gets its own shorter TTL and an optional per-batch cap to bound RAM.
        @failures_ttl = (cfg.failures_ttl || cfg.batch_ttl).to_i
        @failures_cap = cfg.max_failures_per_batch.to_i
      end

      # ── Public interface ──────────────────────────────────────────────────

      def create_batch(id:, total_jobs:, on_success: nil, on_complete: nil, meta: {}, description: nil, sealed: true)
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
              description.to_s
            ]
          )
          # Returns 1 if created, 0 if already existed (idempotent).
          if created == 1
            # Register in the running index (reconciler) and the all index (UI).
            r.zadd(RUNNING_INDEX, now.to_f, id)
            r.zadd(ALL_INDEX, now.to_f, id)
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

      def record_completion_by_offset(batch_id:, source_topic:, source_partition:, source_offset:, status:)
        field        = status == "success" ? "completed_count" : "failed_count"
        offset_field = "#{source_topic}/#{source_partition}"
        bkey         = batch_key(batch_id)
        now          = Time.now.iso8601

        result = with_redis do |r|
          r.eval(BATCH_DONE_OFFSET_LUA,
            keys: [bkey, OFFSETS_KEY],
            argv: [offset_field, source_offset.to_s, field, @ttl.to_s, now]
          )
        end

        code, payload = result
        case code
        when 0 then { status: payload.to_sym }
        when 1
          with_redis do |r|
            r.zrem(RUNNING_INDEX, batch_id)
            r.zadd(DONE_INDEX, Time.now.to_f, batch_id)
          end
          { status: :done, outcome: payload, batch: find_batch(batch_id) }
        when 2 then { status: :continue }
        end
      end

      # Batched counter application for a whole Kafka poll. Reuses the proven
      # per-event Lua (atomic dedup + increment + finalize) but pipelines all the
      # events in one network round-trip. Each event is still exactly-once via
      # the per-partition offset cursor, so nothing is double-counted or missed.
      # @return [Array<Hash>] { batch:, outcome: } for batches that just finished
      def record_completions_batch(events)
        return [] if events.empty?
        now = Time.now.iso8601

        results = with_redis do |r|
          r.pipelined do |pipe|
            events.each do |e|
              field        = e[:status] == "success" ? "completed_count" : "failed_count"
              offset_field = "#{e[:source_topic]}/#{e[:source_partition]}"
              pipe.eval(BATCH_DONE_OFFSET_LUA,
                keys: [batch_key(e[:batch_id]), OFFSETS_KEY],
                argv: [offset_field, e[:source_offset].to_s, field, @ttl.to_s, now])
            end
          end
        end

        finalized = []
        results.each_with_index do |res, i|
          code, payload = res
          next unless code == 1  # 1 == just finalized

          batch_id = events[i][:batch_id]
          with_redis do |r|
            r.zrem(RUNNING_INDEX, batch_id)
            r.zadd(DONE_INDEX, Time.now.to_f, batch_id)
          end
          finalized << { batch: find_batch(batch_id), outcome: payload }
        end
        finalized
      end

      def add_jobs(id, count)
        code = with_redis do |r|
          r.eval(ADD_JOBS_LUA, keys: [batch_key(id)], argv: [count.to_i.to_s, @ttl.to_s])
        end
        case code
        when 0 then :not_found
        when 1 then :ok
        when 2 then :cancelled
        when 3 then :closed
        end
      end

      def seal_batch(id)
        now = Time.now.iso8601
        result = with_redis do |r|
          r.eval(SEAL_BATCH_LUA, keys: [batch_key(id)], argv: [now, @ttl.to_s])
        end
        code, payload = result
        case code
        when 0 then { status: :not_found }
        when 1
          with_redis do |r|
            r.zrem(RUNNING_INDEX, id)
            r.zadd(DONE_INDEX, Time.now.to_f, id)
          end
          { status: :done, outcome: payload, batch: find_batch(id) }
        when 2 then { status: :sealed }
        end
      end

      def claim_callback(id, dispatched_by = nil)
        now = Time.now.iso8601
        result = with_redis do |r|
          won = r.eval(CLAIM_CALLBACK_LUA,
            keys: [batch_key(id)],
            argv: [now, dispatched_by.to_s]
          )
          # Once dispatched the batch no longer needs reconciliation.
          r.zrem(DONE_INDEX, id) if won == 1
          won
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
          r.hset(batch_key(id), "status", status)
          # Terminal/cancelled batches drop out of the running index.
          r.zrem(RUNNING_INDEX, id) if %w[success complete cancelled].include?(status)
          # Track cancellations for the cancellation cache.
          r.sadd(CANCELLED_INDEX, id) if status == "cancelled"
        end
      end

      def batch_status(id)
        with_redis { |r| presence(r.hget(batch_key(id), "status")) }
      end

      def cancelled_batch_ids
        with_redis { |r| r.smembers(CANCELLED_INDEX) }
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

      # ── Liveness (:store backend) ────────────────────────────────────────────

      def record_heartbeat(consumer_id, data)
        entry = Oj.dump(data.merge(consumer_id: consumer_id, last_seen: Time.now.iso8601).transform_keys(&:to_s), mode: :compat)
        with_redis { |r| r.hset(HEARTBEATS_KEY, consumer_id, entry) }
      end

      def list_heartbeats(since)
        raw = with_redis { |r| r.hvals(HEARTBEATS_KEY) }
        raw.map { |v| deserialize(v) }
           .select { |h| h["last_seen"] && Time.parse(h["last_seen"]) >= since rescue false }
           .map { |h| symbolize_heartbeat(h) }
      end

      def sweep_stale_heartbeats(older_than)
        with_redis do |r|
          r.hgetall(HEARTBEATS_KEY).each do |cid, v|
            h = deserialize(v)
            ts = (Time.parse(h["last_seen"]) rescue nil)
            r.hdel(HEARTBEATS_KEY, cid) if ts.nil? || ts < older_than
          end
        end
      end

      # Aggregate failures across all (non-expired) batches via the all-index.
      def list_all_failures(limit: 100, offset: 0, status: nil)
        ids = with_redis { |r| r.zrevrange(ALL_INDEX, 0, -1) }
        entries = []
        ids.each do |bid|
          vals = with_redis { |r| r.hvals(failures_key(bid)) }
          next if vals.empty?
          vals.each do |v|
            h = deserialize(v)
            next if status && h["status"] != status
            entries << failure_hash(h, batch_id: bid)
          end
        end
        sort_paginate(entries, limit, offset)
      end

      def list_batches(status: nil, limit: 50, offset: 0, search: nil)
        ids = with_redis { |r| r.zrevrange(ALL_INDEX, 0, -1) }
        result  = []
        skipped = 0
        q       = presence(search)&.downcase

        ids.each do |id|
          batch = find_batch(id)
          if batch.nil?
            with_redis { |r| r.zrem(ALL_INDEX, id) }  # expired – prune
            next
          end
          next if status && batch[:status] != status
          next if q && !batch_matches?(batch, q)

          if skipped < offset
            skipped += 1
            next
          end

          result << batch
          break if result.size >= limit
        end

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

      def batch_counts
        ids    = with_redis { |r| r.zrange(ALL_INDEX, 0, -1) }
        counts = Hash.new(0)

        ids.each do |id|
          st = batch_status(id)
          if st.nil?
            with_redis { |r| r.zrem(ALL_INDEX, id) }  # expired – prune
          else
            counts[st] += 1
          end
        end

        counts
      end

      def mark_finished(id, outcome)
        now = Time.now
        with_redis do |r|
          r.hset(batch_key(id), "status", outcome)
          r.hset(batch_key(id), "finished_at", now.iso8601)
          # Move from running → done so a (re-)lost callback stays recoverable.
          r.zrem(RUNNING_INDEX, id)
          r.zadd(DONE_INDEX, now.to_f, id)
        end
      end

      # Batches still in the running index that were created before +older_than+.
      # Self-heals the index by dropping members that have expired or already
      # advanced past "running".
      def stale_batches(older_than:)
        ids = with_redis do |r|
          r.zrangebyscore(RUNNING_INDEX, "-inf", older_than.to_f)
        end

        ids.each_with_object([]) do |id, acc|
          batch = find_batch(id)
          if batch.nil?
            with_redis { |r| r.zrem(RUNNING_INDEX, id) }  # expired – prune
          elsif batch[:status] != "running"
            with_redis { |r| r.zrem(RUNNING_INDEX, id) }  # already advanced – prune
          else
            acc << batch
          end
        end
      end

      # Batches in the done index that finished before +older_than+ but whose
      # callback was never dispatched.  Prunes expired or already-dispatched ids.
      def done_batches_without_callback(older_than:)
        ids = with_redis do |r|
          r.zrangebyscore(DONE_INDEX, "-inf", older_than.to_f)
        end

        ids.each_with_object([]) do |id, acc|
          batch = find_batch(id)
          if batch.nil?
            with_redis { |r| r.zrem(DONE_INDEX, id) }  # expired – prune
          elsif !batch[:callback_dispatched_at].nil?
            with_redis { |r| r.zrem(DONE_INDEX, id) }  # already dispatched – prune
          elsif !%w[success complete].include?(batch[:status])
            with_redis { |r| r.zrem(DONE_INDEX, id) }  # not actually done – prune
          else
            acc << batch
          end
        end
      end

      def delete_batch(id)
        with_redis do |r|
          r.del(batch_key(id), failures_key(id))
          r.zrem(RUNNING_INDEX, id)
          r.zrem(DONE_INDEX, id)
          r.zrem(ALL_INDEX, id)
          r.srem(CANCELLED_INDEX, id)
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

      private

      def batch_key(id)
        "#{KEY_PREFIX}:#{id}"
      end

      def failures_key(id)
        "#{batch_key(id)}:failures"
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

      def symbolize_heartbeat(h)
        {
          consumer_id:       h["consumer_id"],
          hostname:          h["hostname"],
          pid:               h["pid"],
          topic:             h["topic"],
          current_job_id:    h["current_job_id"],
          current_worker:    h["current_worker"],
          current_batch_id:  h["current_batch_id"],
          current_topic:     h["current_topic"],
          current_partition: h["current_partition"],
          jobs_done:         h["jobs_done"],
          last_seen:         h["last_seen"]
        }
      end

      def with_redis(&block)
        @pool.with(&block)
      rescue Redis::BaseError => e
        raise StoreError, "Redis error: #{e.message}"
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
    end
  end
end
