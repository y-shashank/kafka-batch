# frozen_string_literal: true

require "connection_pool"
require "securerandom"
require "time"
require_relative "../redis_client"

module KafkaBatch
  module TenantGuard
    # Redis persistence for tenant-guard control records, the active-tenant
    # index, and the action audit log. Enforcement state (partition pause SET /
    # weight hash) is authoritative for "what is happening now"; these records
    # carry the intent + metadata (source, reason, until, original_weight) and
    # feed the reconciler and the dashboard. Shared Redis contract — see the
    # kafka-batch README "Tenant guard".
    module State
      RECORD_PREFIX = "kafka_batch:tenant_guard:"     # + {tenant_id}
      INDEX_KEY     = "kafka_batch:tenant_guard:index"
      ACTIONS_ZSET  = "kafka_batch:tenant_guard:actions"
      ACTION_PREFIX = "kafka_batch:tenant_guard:action:" # + {id}
      BREACH_PREFIX = "kafka_batch:tenant_guard:breach:" # + {tenant_id}
      LOCK_KEY      = "kafka_batch:tenant_guard:lock"

      # Cap the audit log so it cannot grow without bound.
      MAX_ACTIONS = 1000

      RECORD_FIELDS = %w[
        state lane action source reason group topic partition
        original_weight effective_weight created_at until created_by action_id
      ].freeze

      class << self
        def available?
          KafkaBatch.config.redis_configured?
        end

        # ── Control records ────────────────────────────────────────────────
        def put_record(tenant_id, fields)
          tid = tenant_id.to_s
          key = record_key(tid)
          flat = stringify(fields)
          redis_with do |r|
            r.mapped_hmset(key, flat) unless flat.empty?
            r.sadd(INDEX_KEY, tid)
          end
          nil
        end

        def get_record(tenant_id)
          h = redis_with { |r| r.hgetall(record_key(tenant_id)) }
          return nil if h.nil? || h.empty?

          h
        end

        def delete_record(tenant_id)
          tid = tenant_id.to_s
          redis_with do |r|
            r.del(record_key(tid))
            r.srem(INDEX_KEY, tid)
          end
          nil
        end

        def index_members
          redis_with { |r| r.smembers(INDEX_KEY) } || []
        end

        # ── Action audit log ───────────────────────────────────────────────
        # Appends an action and returns its id. created_at defaults to now.
        def append_action(fields, at: Time.now)
          id = SecureRandom.uuid
          flat = stringify(fields.merge("id" => id, "created_at" => (fields["created_at"] || at.to_i)))
          redis_with do |r|
            r.mapped_hmset(action_key(id), flat)
            r.zadd(ACTIONS_ZSET, at.to_i, id)
            trim_actions(r)
          end
          id
        end

        def update_action(id, fields)
          return if id.nil? || id.to_s.empty?

          flat = stringify(fields)
          redis_with { |r| r.mapped_hmset(action_key(id), flat) } unless flat.empty?
          nil
        end

        def get_action(id)
          return nil if id.nil? || id.to_s.empty?

          h = redis_with { |r| r.hgetall(action_key(id)) }
          return nil if h.nil? || h.empty?

          h
        end

        # Most-recent-first action log (each entry is a flat hash).
        def recent_actions(limit: 100)
          ids = redis_with { |r| r.zrevrange(ACTIONS_ZSET, 0, [limit.to_i - 1, 0].max) } || []
          return [] if ids.empty?

          redis_with do |r|
            ids.map { |id| r.hgetall(action_key(id)) }
          end.reject { |h| h.nil? || h.empty? }
        end

        # ── Grace-tick breach counters (for tenant_guard_grace_ticks) ───────
        # Returns the new count. TTL so a tenant that stops breaching decays.
        def incr_breach!(tenant_id, ttl:)
          key = "#{BREACH_PREFIX}#{tenant_id}"
          redis_with do |r|
            n = r.incr(key)
            r.expire(key, [ttl.to_i, 60].max)
            n
          end.to_i
        end

        def reset_breach!(tenant_id)
          redis_with { |r| r.del("#{BREACH_PREFIX}#{tenant_id}") }
          nil
        end

        def breach_count(tenant_id)
          redis_with { |r| r.get("#{BREACH_PREFIX}#{tenant_id}") }.to_i
        end

        # ── Locking (shared with the reconciler; NX single-flight) ──────────
        def try_lock!(ttl:)
          won = redis_with { |r| r.set(LOCK_KEY, "1", nx: true, ex: [ttl.to_i, 2].max) }
          won == true || won == "OK"
        end

        def unlock!
          redis_with { |r| r.del(LOCK_KEY) }
          nil
        end

        def reset!
          @pool = nil
        end

        private

        def record_key(tenant_id)
          "#{RECORD_PREFIX}#{tenant_id}"
        end

        def action_key(id)
          "#{ACTION_PREFIX}#{id}"
        end

        def trim_actions(r)
          # Keep only the newest MAX_ACTIONS ids; drop older ones (and their hashes).
          extra = r.zrange(ACTIONS_ZSET, 0, -(MAX_ACTIONS + 1))
          return if extra.nil? || extra.empty?

          extra.each { |id| r.del(action_key(id)) }
          r.zrem(ACTIONS_ZSET, extra)
        end

        def stringify(hash)
          hash.each_with_object({}) do |(k, v), acc|
            next if v.nil?

            acc[k.to_s] = v.to_s
          end
        end

        def redis_with
          return nil unless available?

          pool.with { |r| yield r }
        end

        def pool
          @pool ||= ConnectionPool.new(size: 2, timeout: 3) do
            client = RedisClient.new(KafkaBatch.config)
            raise "Redis not configured" unless client

            client
          end
        end
      end
    end
  end
end
