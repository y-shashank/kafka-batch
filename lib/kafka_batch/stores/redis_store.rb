require "redis"
require "connection_pool"
require "securerandom"
require "time"
require_relative "base"

module KafkaBatch
  module Stores
    class RedisStore < Base
      # Redis key layout:
      #   kafka_batch:b:{id}            – Hash of all batch fields
      #   kafka_batch:b:{id}:done_jobs  – Set of "job_id:status" (dedup)
      #
      # Both keys expire after KafkaBatch.config.batch_ttl seconds.
      # The TTL is refreshed on every event so truly long-running batches
      # don't expire mid-flight.

      KEY_PREFIX = "kafka_batch:b"

      # Atomically increment job counter, extend TTL, and check for completion.
      # Returns [code, payload]:
      #   [0, "duplicate"]  – job_id already recorded (dedup)
      #   [0, "not_found"]  – batch hash does not exist
      #   [1, outcome]      – batch just completed; outcome = "success"|"complete"
      #   [2, "continue"]   – still jobs outstanding
      BATCH_DONE_LUA = <<~LUA.freeze
        local added = redis.call('SADD', KEYS[2], ARGV[1])
        if added == 0 then return {0, 'duplicate'} end

        -- Refresh TTLs on every completion so long batches don't expire
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
        redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))

        -- Guard: batch must exist
        local exists = redis.call('EXISTS', KEYS[1])
        if exists == 0 then return {0, 'not_found'} end

        redis.call('HINCRBY', KEYS[1], ARGV[2], 1)

        local total     = tonumber(redis.call('HGET', KEYS[1], 'total_jobs'))       or 0
        local completed = tonumber(redis.call('HGET', KEYS[1], 'completed_count'))  or 0
        local failed    = tonumber(redis.call('HGET', KEYS[1], 'failed_count'))     or 0
        local status    = redis.call('HGET', KEYS[1], 'status')

        -- Prevent double-finalisation on concurrent writes
        if status == 'success' or status == 'complete' or status == 'cancelled' then
          return {0, 'duplicate'}
        end

        if (completed + failed) >= total then
          local outcome = (failed > 0) and 'complete' or 'success'
          redis.call('HSET', KEYS[1], 'status',      outcome)
          redis.call('HSET', KEYS[1], 'finished_at', ARGV[4])
          redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
          return {1, outcome}
        end

        return {2, 'continue'}
      LUA

      # Atomically claim callback dispatch rights.
      # HSETNX returns 1 if field was absent (we won the race), 0 if already set.
      CLAIM_CALLBACK_LUA = <<~LUA.freeze
        local set = redis.call('HSETNX', KEYS[1], 'callback_dispatched_at', ARGV[1])
        return set
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
          'created_at',      ARGV[6]
        )
        redis.call('EXPIRE', KEYS[1], tonumber(ARGV[7]))
        return 1
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

      def initialize
        cfg = KafkaBatch.config
        @pool = ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
          Redis.new(url: cfg.redis_url)
        end
        @ttl = cfg.batch_ttl
      end

      # ── Public interface ──────────────────────────────────────────────────

      def create_batch(id:, total_jobs:, on_success: nil, on_complete: nil, meta: {})
        key = batch_key(id)
        with_redis do |r|
          r.eval(CREATE_BATCH_LUA,
            keys: [key],
            argv: [
              id,
              total_jobs.to_s,
              on_success.to_s,
              on_complete.to_s,
              serialize(meta),
              Time.now.iso8601,
              @ttl.to_s
            ]
          )
          # Returns 1 if created, 0 if already existed (idempotent).
        end
      end

      def find_batch(id)
        with_redis do |r|
          h = r.hgetall(batch_key(id))
          return nil if h.nil? || h.empty?
          hash_to_batch(h)
        end
      end

      def record_job_completion(batch_id:, job_id:, status:)
        field    = status == "success" ? "completed_count" : "failed_count"
        member   = "#{job_id}:#{status}"
        now      = Time.now.iso8601
        bkey     = batch_key(batch_id)
        done_key = "#{bkey}:done_jobs"

        result = with_redis do |r|
          r.eval(BATCH_DONE_LUA,
            keys: [bkey, done_key],
            argv: [member, field, @ttl.to_s, now]
          )
        end

        code, payload = result
        case code
        when 0 then { status: payload.to_sym }   # :duplicate or :not_found
        when 1 then { status: :done, outcome: payload, batch: find_batch(batch_id) }
        when 2 then { status: :continue }
        end
      end

      def claim_callback(id)
        now = Time.now.iso8601
        result = with_redis do |r|
          r.eval(CLAIM_CALLBACK_LUA,
            keys: [batch_key(id)],
            argv: [now]
          )
        end
        result == 1
      end

      def update_batch_status(id, status)
        with_redis do |r|
          r.hset(batch_key(id), "status", status)
        end
      end

      def stale_batches(older_than:)
        # Redis has no native range-scan on hash field values.
        # Implement a batch-ID registry (sorted set keyed by created_at) in your
        # app if you need Redis-side reconciliation of stuck running batches.
        KafkaBatch.logger.warn(
          "[KafkaBatch] RedisStore#stale_batches: no-op. " \
          "Maintain a sorted set of batch IDs by created_at for Redis reconciliation."
        )
        []
      end

      def done_batches_without_callback(older_than:)
        # Same limitation as stale_batches – Redis can't range-scan hash fields.
        KafkaBatch.logger.warn(
          "[KafkaBatch] RedisStore#done_batches_without_callback: no-op. " \
          "Maintain a sorted set of batch IDs by finished_at for Redis reconciliation."
        )
        []
      end

      def delete_batch(id)
        with_redis do |r|
          r.del(batch_key(id), "#{batch_key(id)}:done_jobs")
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
      rescue StoreError => e
        KafkaBatch.logger.error("[KafkaBatch][RedisStore] Reconciler lock error: #{e.message}")
      end

      private

      def batch_key(id)
        "#{KEY_PREFIX}:#{id}"
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
          meta:                   deserialize(h["meta"]),
          created_at:             h["created_at"],
          finished_at:            h["finished_at"],
          callback_dispatched_at: presence(h["callback_dispatched_at"])
        }
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
