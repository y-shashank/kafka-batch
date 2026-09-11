# frozen_string_literal: true

require "connection_pool"

module KafkaBatch
  module Fairness
    # Resolves tenant_id → fairness ingest partition.
    #
    # Resolution order:
    #   1. config.fairness_tenant_partitions[tenant_id] — static map wins
    #   2. Redis checkout (when config.fairness_dynamic_tenant_partitions)
    #   3. nil → caller may fall back to murmur2 key-hash
    #
    # Dynamic mode keeps a per-lane Redis HASH (tenant → partition) and a SET of
    # free partition numbers. On boot / warm!, the free set is seeded from the
    # ingest topic's partition count minus already-assigned tenants.
    class TenantPartitions
      CHECKOUT_LUA = <<~LUA.freeze
        local tenant = ARGV[1]
        local count  = tonumber(ARGV[2])
        if not tenant or not count or count < 1 then return -2 end

        local existing = redis.call('HGET', KEYS[1], tenant)
        if existing then
          local p = tonumber(existing)
          if p and p >= 0 and p < count then return p end
          redis.call('HDEL', KEYS[1], tenant)
        end

        local p = redis.call('SPOP', KEYS[2])
        if not p then return -1 end

        p = tonumber(p)
        if not p or p < 0 or p >= count then
          redis.call('SADD', KEYS[2], p)
          return -2
        end

        redis.call('HSET', KEYS[1], tenant, p)
        return p
      LUA

      # Reconciles a lane's free-partition set from the tenant→partition map
      # ATOMICALLY (wire-identical to Go warmLua). The previous version computed
      # `free` from an HGETALL snapshot and then SADD'd missing members in
      # separate round trips: a checkout (SPOP + HSET) landing in that window
      # put the just-taken partition back into the free set, so a second tenant
      # could be assigned the same partition — breaking per-tenant isolation.
      #
      # KEYS[1]=map hash KEYS[2]=free set KEYS[3]=meta (partition count)
      # ARGV[1]=live partition count. Returns the free-set size.
      WARM_LUA = <<~LUA.freeze
        local count = tonumber(ARGV[1])
        if not count or count < 1 then return -1 end

        local taken = {}
        local raw = redis.call('HGETALL', KEYS[1])
        for i = 1, #raw, 2 do
          local tenant = raw[i]
          local p = tonumber(raw[i + 1])
          if p and p >= 0 and p < count then
            taken[p] = true
          else
            redis.call('HDEL', KEYS[1], tenant)
          end
        end

        local stored = tonumber(redis.call('GET', KEYS[3]) or '-1')
        if stored ~= count then
          redis.call('DEL', KEYS[2])
          for p = 0, count - 1 do
            if not taken[p] then redis.call('SADD', KEYS[2], p) end
          end
          redis.call('SET', KEYS[3], count)
          return redis.call('SCARD', KEYS[2])
        end

        local cur = {}
        for _, s in ipairs(redis.call('SMEMBERS', KEYS[2])) do
          local p = tonumber(s)
          if not p or p < 0 or p >= count then
            redis.call('SREM', KEYS[2], s)
          else
            cur[p] = true
          end
        end
        for p = 0, count - 1 do
          if not taken[p] and not cur[p] then
            redis.call('SADD', KEYS[2], p)
          end
        end
        return redis.call('SCARD', KEYS[2])
      LUA

      class << self
        def resolve(tenant_id, type = :time)
          return nil if tenant_id.nil?

          tid  = tenant_id.to_s
          lane = type.to_sym

          hit = read_cache(lane, tid)
          return hit unless hit.nil?

          configured = configured_partition(tid, lane)
          if configured
            write_cache(lane, tid, configured)
            return configured
          end

          return nil unless dynamic?

          partition = checkout(lane, tid)
          if partition
            write_cache(lane, tid, partition)
            return partition
          end

          nil
        end

        # Seed / reconcile the free-partition pool for a lane from the live topic
        # partition count. Safe to call on every boot and before checkout.
        def warm!(type = :time)
          return unless dynamic?

          lane  = type.to_sym
          count = KafkaBatch.fairness_ingest_partition_count(lane)
          return unless count&.positive?

          with_redis do |r|
            r.eval(WARM_LUA,
              keys: [map_key(lane), free_key(lane), meta_key(lane)],
              argv: [count])
          end
        rescue StandardError => e
          KafkaBatch.logger.warn(
            "[KafkaBatch::Fairness::TenantPartitions] warm!(#{lane}) failed: #{e.message}"
          )
        end

        def reset!
          @pool  = nil
          @cache = {}
        end

        def all_assigned(type = :time)
          lane = type.to_sym
          with_redis { |r| r.hgetall(map_key(lane)) } || {}
        rescue StandardError
          {}
        end

        private

        def dynamic?
          KafkaBatch.config.fairness_dynamic_tenant_partitions
        end

        def cache_ttl
          KafkaBatch.config.fairness_tenant_partition_cache_ttl.to_i
        end

        def configured_partition(tenant_id, type)
          map = KafkaBatch.config.fairness_tenant_partitions
          return nil if map.nil? || map.empty?

          configured = map[tenant_id]
          return nil if configured.nil?

          n = configured.to_i
          count = KafkaBatch.fairness_ingest_partition_count(type)
          if count && n >= count
            KafkaBatch.logger.warn(
              "[KafkaBatch] fairness_tenant_partitions[#{tenant_id}]=#{n} is out of range " \
              "(topic has #{count} partitions). Ignoring."
            )
            return nil
          end

          n
        end

        def checkout(lane, tenant_id)
          warm!(lane)

          count = KafkaBatch.fairness_ingest_partition_count(lane)
          return nil unless count&.positive?

          result =
            with_redis do |r|
              r.eval(CHECKOUT_LUA, keys: [map_key(lane), free_key(lane)], argv: [tenant_id, count])
            end

          case result.to_i
          when -1
            KafkaBatch.logger.warn(
              "[KafkaBatch::Fairness::TenantPartitions] no free ingest partitions left on " \
              "#{lane} lane (#{count} partitions, all assigned). " \
              "Add partitions to #{KafkaBatch.config.fairness_ingest_topic(lane)} or disable " \
              "fairness_dynamic_tenant_partitions."
            )
            nil
          when -2
            nil
          else
            result.to_i
          end
        rescue StandardError => e
          KafkaBatch.logger.warn(
            "[KafkaBatch::Fairness::TenantPartitions] checkout(#{tenant_id}, #{lane}) failed: #{e.message}"
          )
          nil
        end

        def read_cache(lane, tenant_id)
          ttl = cache_ttl
          return nil if ttl <= 0

          entry = (@cache ||= {})[[lane, tenant_id]]
          return nil unless entry
          return entry[:partition] if monotonic_now - entry[:at] < ttl

          @cache.delete([lane, tenant_id])
          nil
        end

        def write_cache(lane, tenant_id, partition)
          ttl = cache_ttl
          return if ttl <= 0

          (@cache ||= {})[[lane, tenant_id]] = { partition: partition, at: monotonic_now }
        end

        def monotonic_now
          Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end

        def map_key(lane)
          "kafka_batch:tenant_partitions:#{lane}"
        end

        def free_key(lane)
          "#{map_key(lane)}:free"
        end

        def meta_key(lane)
          "#{map_key(lane)}:partition_count"
        end

        def with_redis
          return nil unless KafkaBatch.config.redis_configured?

          pool.with { |r| yield r }
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch::Fairness::TenantPartitions] Redis error: #{e.message}")
          nil
        end

        def pool
          cfg = KafkaBatch.config
          @pool ||= ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
            KafkaBatch::RedisClient.new(cfg) ||
              raise(ConfigurationError, "Redis is not configured")
          end
        end
      end
    end
  end
end
