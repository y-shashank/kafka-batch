require "redis"
require "connection_pool"

module KafkaBatch
  module Fairness
    # Redis-backed Weighted Fair Queuing (WFQ) scheduler for multi-tenant
    # fairness. It is the heart of the "hybrid" design:
    #
    #   * the durable backlog stays in Kafka;
    #   * a dispatcher copies jobs into a BOUNDED per-tenant "ready" window here;
    #   * workers pull the next job via #checkout, which picks fairly across
    #     tenants and respects a single global concurrency budget.
    #
    # Fairness uses classic virtual-time WFQ: each tenant has a virtual time
    # (vtime) advanced by 1/weight on every dispatch; #checkout always serves the
    # ready tenant with the smallest vtime. Consequences:
    #
    #   * 1 active tenant  -> it is always chosen          => 100% of capacity
    #   * 2 active (equal) -> vtimes leapfrog              => ~50:50
    #   * a tenant that goes idle is simply skipped        => work-conserving
    #   * higher weight    -> vtime grows slower           => proportionally more
    #
    # A returning idle tenant is re-admitted at max(its vtime, current min vtime)
    # so it can't hoard capacity using credit accrued while it was absent.
    #
    # Memory is O(active_tenants * ready_window), NOT O(total jobs) — the backlog
    # lives in Kafka.
    class Scheduler
      NS             = "kafka_batch:fair".freeze
      RING           = "#{NS}:ring".freeze            # ZSET tenant => vtime (ready tenants only)
      VTIME          = "#{NS}:vtime".freeze           # HASH tenant => remembered vtime
      WEIGHT         = "#{NS}:weight".freeze          # HASH tenant => weight override
      INFLIGHT       = "#{NS}:inflight".freeze        # HASH tenant => in-flight count
      INFLIGHT_TOTAL = "#{NS}:inflight_total".freeze  # global in-flight counter
      READY_PREFIX   = "#{NS}:ready:".freeze          # LIST per tenant

      # Append to a tenant's bounded ready window. Re-admits an idle tenant into
      # the ring at the current minimum vtime so it starts on an equal footing.
      # Returns 1 if stored, 0 if the window is full (caller should apply
      # backpressure, e.g. pause the tenant's Kafka partition).
      ENQUEUE_LUA = <<~LUA.freeze
        local ring   = KEYS[1]
        local vth    = KEYS[2]
        local tenant = ARGV[1]
        local payload = ARGV[2]
        local window = tonumber(ARGV[3])
        local rk     = ARGV[4] .. tenant

        if window > 0 and redis.call('LLEN', rk) >= window then return 0 end

        if redis.call('ZSCORE', ring, tenant) == false then
          local vt = tonumber(redis.call('HGET', vth, tenant) or '0')
          local mn = redis.call('ZRANGE', ring, 0, 0, 'WITHSCORES')
          if mn[2] and tonumber(mn[2]) > vt then vt = tonumber(mn[2]) end
          redis.call('ZADD', ring, vt, tenant)
          redis.call('HSET', vth, tenant, vt)
        end

        redis.call('RPUSH', rk, payload)
        return 1
      LUA

      # Pick the next job fairly. Honors the global budget and optional per-tenant
      # in-flight cap. Returns {1, tenant, payload} | {0, 'budget'|'none'}.
      CHECKOUT_LUA = <<~LUA.freeze
        local ring   = KEYS[1]
        local vth    = KEYS[2]
        local inf    = KEYS[3]
        local inftot = KEYS[4]
        local wh     = KEYS[5]
        local budget = tonumber(ARGV[1])
        local cap    = tonumber(ARGV[2])
        local rprefix = ARGV[3]
        local dw     = tonumber(ARGV[4])

        local total = tonumber(redis.call('GET', inftot) or '0')
        if budget > 0 and total >= budget then return {0, 'budget'} end

        local members = redis.call('ZRANGE', ring, 0, -1)  -- ascending vtime
        for i = 1, #members do
          local t = members[i]
          local tin = tonumber(redis.call('HGET', inf, t) or '0')
          if cap == 0 or tin < cap then
            local rk  = rprefix .. t
            local job = redis.call('LPOP', rk)
            if job then
              local w = tonumber(redis.call('HGET', wh, t) or dw)
              if w == nil or w <= 0 then w = dw end
              local vt = tonumber(redis.call('ZSCORE', ring, t)) + (1.0 / w)
              redis.call('HSET', vth, t, vt)
              if redis.call('LLEN', rk) == 0 then
                redis.call('ZREM', ring, t)
              else
                redis.call('ZADD', ring, vt, t)
              end
              redis.call('HINCRBY', inf, t, 1)
              redis.call('INCR', inftot)
              return {1, t, job}
            else
              redis.call('ZREM', ring, t)  -- empty but ringed: self-heal
            end
          end
        end
        return {0, 'none'}
      LUA

      # Release one in-flight slot for a tenant after its job finishes.
      COMPLETE_LUA = <<~LUA.freeze
        local tin = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
        if tin > 0 then redis.call('HINCRBY', KEYS[1], ARGV[1], -1) end
        local tot = tonumber(redis.call('GET', KEYS[2]) or '0')
        if tot > 0 then redis.call('DECR', KEYS[2]) end
        return 1
      LUA

      def initialize(pool: nil)
        cfg            = KafkaBatch.config
        @pool          = pool || ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
          Redis.new(url: cfg.redis_url)
        end
        @window        = cfg.fairness_ready_window.to_i
        @budget        = cfg.fairness_global_concurrency.to_i
        @cap           = cfg.fairness_max_inflight_per_tenant.to_i
        @default_weight = cfg.fairness_default_weight.to_f
      end

      # Add a job to a tenant's ready window. @return [:ok, :full]
      def enqueue(tenant_id, payload)
        ok = with { |r| r.eval(ENQUEUE_LUA, keys: [RING, VTIME], argv: [tenant_id.to_s, payload, @window.to_s, READY_PREFIX]) }
        ok == 1 ? :ok : :full
      end

      # Pull the next job fairly, or nil when at budget / nothing ready.
      # @return [Hash, nil] { tenant_id:, payload: }
      def checkout
        res = with do |r|
          r.eval(CHECKOUT_LUA,
            keys: [RING, VTIME, INFLIGHT, INFLIGHT_TOTAL, WEIGHT],
            argv: [@budget.to_s, @cap.to_s, READY_PREFIX, @default_weight.to_s])
        end
        code, a, b = res
        code == 1 ? { tenant_id: a, payload: b } : nil
      end

      # Release the in-flight slot held by a tenant's finished job.
      def complete(tenant_id)
        with { |r| r.eval(COMPLETE_LUA, keys: [INFLIGHT, INFLIGHT_TOTAL], argv: [tenant_id.to_s]) }
        nil
      end

      # Per-tenant relative weight (>1 = larger share). Persisted until changed.
      def set_weight(tenant_id, weight)
        with { |r| r.hset(WEIGHT, tenant_id.to_s, weight.to_f) }
        nil
      end

      def ready_depth(tenant_id)
        with { |r| r.llen("#{READY_PREFIX}#{tenant_id}") }
      end

      # Snapshot for dashboards/metrics.
      # @return [Hash] { active_tenants:, inflight_total:, budget:, window: }
      def stats
        active, total = with { |r| r.multi { |m| m.zcard(RING); m.get(INFLIGHT_TOTAL) } }
        {
          active_tenants: active.to_i,
          inflight_total: total.to_i,
          budget:         @budget,
          window:         @window
        }
      end

      # In-flight count per tenant (for the fairness dashboard).
      def inflight_by_tenant
        with { |r| r.hgetall(INFLIGHT) }.transform_values(&:to_i).reject { |_, v| v.zero? }
      end

      # Remove all scheduler state (tests / full reset).
      def reset!
        with do |r|
          keys = r.scan_each(match: "#{NS}*").to_a
          r.del(*keys) unless keys.empty?
        end
      end

      private

      def with(&block)
        @pool.with(&block)
      end
    end
  end
end
