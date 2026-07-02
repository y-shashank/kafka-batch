require "redis"
require "connection_pool"

module KafkaBatch
  module Fairness
    # OPTIONAL Redis-backed Weighted Fair Queuing (WFQ) scheduler for STRICT
    # weighted multi-tenant fairness. It is NOT used by the default fairness path
    # (the Kafka-only Dispatcher needs no Redis) — it's a standalone engine to
    # build a custom dispatcher/worker around when you need precise weighted
    # shares. Design:
    #
    #   * the durable backlog stays in Kafka;
    #   * a dispatcher copies jobs into a BOUNDED per-tenant "ready" window here;
    #   * workers pull the next job via #checkout, which picks fairly across
    #     tenants and respects a single global concurrency budget.
    #
    # ── Fairness mode ──────────────────────────────────────────────────────────
    #
    # Two modes, selected via config.fairness_mode:
    #
    # :time_fairness (recommended default)
    #   Virtual time advances at job *completion* by actual_seconds / weight.
    #   Tenants with slow jobs (20-60s) are charged proportionally for the
    #   wall-clock time they consume. Over any rolling hour, each tenant gets
    #   roughly equal slot-time (weighted). Callers MUST pass `duration:` to
    #   #complete so the actual run time is recorded.
    #
    # :job_count_fairness (original behaviour)
    #   Virtual time advances by 1/weight at *dispatch*. Fair over job count,
    #   not duration. Correct when all tenants' jobs have similar runtimes;
    #   over-rewards tenants that dispatch fast-finishing jobs otherwise.
    #
    # ── Virtual-time WFQ invariant ────────────────────────────────────────────
    #
    # Each tenant has a virtual time (vtime) in the RING ZSET (ascending = most
    # deprived). #checkout always picks the ready tenant with the smallest vtime.
    # A returning idle tenant is re-admitted at max(its vtime, current min vtime)
    # so it cannot hoard capacity accrued while idle.
    #
    # ── Process-local weight cache ────────────────────────────────────────────
    #
    # Weights are cached in each dispatcher process for
    # config.fairness_weight_cache_ttl seconds (default 120s). Weight changes
    # propagate to all processes within that window. Because weights are
    # infrequently changed admin config (not per-job data), 120s is intentional:
    # it eliminates backend reads on every checkout / completion without making
    # the UI feel unresponsive.
    #
    # Redis backend  – cache is populated from the WEIGHT Redis hash (HGETALL).
    # MySQL backend  – cache is populated from kafka_batch_tenant_weights table.
    #                  Redis is NOT read or written for weights in this path.
    #
    # ── Bounded ring scan (fetch_n) ───────────────────────────────────────────
    #
    # CHECKOUT_LUA_* scripts use ZRANGE ring 0 (fetch_n-1) instead of 0 -1 to
    # avoid transferring the full ring (up to 500 tenants) on every call.
    # fetch_n = max(budget * 3, 60) covers all realistic scenarios where tenants
    # at their per-tenant cap need to be skipped.
    #
    # ── Job-count fairness + MySQL weight backend ─────────────────────────────
    #
    # CHECKOUT_LUA_COUNT_REDIS reads the Redis WEIGHT hash via HGET inside Lua
    # (one extra Redis read per dispatch). This is not acceptable when weights
    # live in MySQL and Redis should not be a weight dependency.
    #
    # CHECKOUT_LUA_COUNT_MYSQL solves this by accepting all weight overrides as
    # inline ARGV pairs (tenant_id, weight, …). Ruby serialises the process cache
    # into the call; Lua builds a local weight table from those pairs. The result
    # is a single Redis round-trip with zero Redis reads for weights — the process
    # cache is the only inter-call caching layer for the MySQL backend.
    #
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
      # Unchanged — same semantics in both fairness modes.
      ENQUEUE_LUA = <<~LUA.freeze
        local ring    = KEYS[1]
        local vth     = KEYS[2]
        local tenant  = ARGV[1]
        local payload = ARGV[2]
        local window  = tonumber(ARGV[3])
        local rk      = ARGV[4] .. tenant

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

      # ── Job-count fairness checkout ────────────────────────────────────────
      # Vtime advances by 1/weight at dispatch. Fair over dispatch count.
      # Weight is looked up from the WEIGHT hash inside Lua (one HGET per
      # successful checkout — acceptable since the Lua call is already one
      # Redis round-trip).
      # ARGV[5] = fetch_n (bounded scan depth; avoids full ZRANGE on large rings)
      # ARGV[6] = weighted (1 = weight-proportional per-tenant cap, 0 = equal share)
      CHECKOUT_LUA_COUNT = <<~LUA.freeze
        local ring     = KEYS[1]
        local vth      = KEYS[2]
        local inf      = KEYS[3]
        local inftot   = KEYS[4]
        local wh       = KEYS[5]
        local budget   = tonumber(ARGV[1])
        local cap      = tonumber(ARGV[2])
        local rprefix  = ARGV[3]
        local dw       = tonumber(ARGV[4])
        local fetch_n  = tonumber(ARGV[5]) - 1
        local weighted = tonumber(ARGV[6])
        local ahint    = tonumber(ARGV[7]) or 0   -- smoothed active-tenant count (cached in Ruby)
        local shint    = tonumber(ARGV[8]) or 0   -- smoothed sum of active weights (cached in Ruby)

        local total = tonumber(redis.call('GET', inftot) or '0')
        if budget > 0 and total >= budget then return {0, 'budget'} end

        -- Per-tenant in-flight cap.
        --   weighted == 0 : EQUAL dynamic fair share = ceil(budget / active).
        --   weighted == 1 : WEIGHTED share = floor(budget * w_t / sum_active_w),
        --                   min 1, so caps enforce the intended ratio even under
        --                   full saturation. One active tenant → whole budget
        --                   (work-conserving). The configured cap is an optional
        --                   HARD ceiling layered on top (0 = none).
        -- active / sum_w use the caller's smoothed hint as a FLOOR (max with the
        -- instantaneous ring) so caps track the real active set instead of
        -- flickering as tenants briefly drain. Hint <= 0 → compute from the ring.
        local active = redis.call('ZCARD', ring)
        if active < 1 then active = 1 end
        if ahint > active then active = ahint end

        local sum_w = 0
        if weighted == 1 and budget > 0 then
          if shint > 0 then
            sum_w = shint
          else
            local all = redis.call('ZRANGE', ring, 0, -1)
            for i = 1, #all do
              local wi = tonumber(redis.call('HGET', wh, all[i]) or dw)
              if wi == nil or wi <= 0 then wi = dw end
              sum_w = sum_w + wi
            end
            if sum_w <= 0 then sum_w = dw end
          end
        end

        -- Two-pass, WORK-CONSERVING selection:
        --   round 1 (fair)     : respect the per-tenant fair-share cap.
        --   round 2 (fallback) : if the fair pass found nothing dispatchable but
        --                        the global budget still has room, fill the slack
        --                        ignoring the DYNAMIC fair-share cap (still honor
        --                        the absolute hard cap `cap` and the global budget).
        -- This guarantees full utilization when few tenants are active (a lone
        -- tenant uses the whole budget) without starving newcomers: fairness only
        -- binds under contention (when the fair pass can fill the budget), and any
        -- freed slot goes to the lowest-vtime tenant first as jobs complete.
        local members = redis.call('ZRANGE', ring, 0, fetch_n)
        for round = 1, 2 do
          local fallback = (round == 2)
          for i = 1, #members do
            local t   = members[i]
            local tin = tonumber(redis.call('HGET', inf, t) or '0')

            local dispatchable
            if fallback then
              -- Slack fill: only the absolute hard cap (if any) still applies.
              dispatchable = (cap == 0 or tin < cap)
            else
              local eff_cap = 0
              if budget > 0 then
                if weighted == 1 then
                  local w_t = tonumber(redis.call('HGET', wh, t) or dw)
                  if w_t == nil or w_t <= 0 then w_t = dw end
                  eff_cap = math.floor(budget * w_t / sum_w)
                  if eff_cap < 1 then eff_cap = 1 end
                else
                  eff_cap = math.ceil(budget / active)
                  if eff_cap < 1 then eff_cap = 1 end
                end
              end
              if cap > 0 and (eff_cap == 0 or cap < eff_cap) then eff_cap = cap end
              dispatchable = (eff_cap == 0 or tin < eff_cap)
            end

            if dispatchable then
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
                redis.call('ZREM', ring, t)  -- stale entry: self-heal
              end
            end
          end
        end
        return {0, 'none'}
      LUA

      # ── Time fairness checkout ─────────────────────────────────────────────
      # Vtime is NOT advanced at dispatch — that happens at completion
      # (COMPLETE_LUA_TIME) using the actual job duration. The ring ordering
      # reflects accumulated slot-time consumed, not dispatch count.
      # WEIGHT hash (KEYS[4]) + dw (ARGV[5]) are used only for the weighted cap.
      # ARGV[6] = weighted (1 = weight-proportional per-tenant cap, 0 = equal share)
      CHECKOUT_LUA_TIME = <<~LUA.freeze
        local ring     = KEYS[1]
        local inf      = KEYS[2]
        local inftot   = KEYS[3]
        local wh       = KEYS[4]
        local budget   = tonumber(ARGV[1])
        local cap      = tonumber(ARGV[2])
        local rprefix  = ARGV[3]
        local fetch_n  = tonumber(ARGV[4]) - 1
        local dw       = tonumber(ARGV[5])
        local weighted = tonumber(ARGV[6])
        local ahint    = tonumber(ARGV[7]) or 0   -- smoothed active-tenant count (cached in Ruby)
        local shint    = tonumber(ARGV[8]) or 0   -- smoothed sum of active weights (cached in Ruby)

        local total = tonumber(redis.call('GET', inftot) or '0')
        if budget > 0 and total >= budget then return {0, 'budget'} end

        -- Per-tenant in-flight cap. In time mode vtime only advances at
        -- completion, so this cap is what interleaves tenants:
        --   weighted == 0 : EQUAL fair share = ceil(budget / active).
        --   weighted == 1 : WEIGHTED share = floor(budget * w_t / sum_active_w),
        --                   min 1 — enforces the intended time distribution even
        --                   under full saturation. Lone tenant → whole budget.
        -- active / sum_w use the caller's smoothed hint as a FLOOR (max with the
        -- instantaneous ring) so caps track the real active set instead of
        -- flickering as tenants briefly drain. Hint <= 0 → compute from the ring.
        local active = redis.call('ZCARD', ring)
        if active < 1 then active = 1 end
        if ahint > active then active = ahint end

        local sum_w = 0
        if weighted == 1 and budget > 0 then
          if shint > 0 then
            sum_w = shint
          else
            local all = redis.call('ZRANGE', ring, 0, -1)
            for i = 1, #all do
              local wi = tonumber(redis.call('HGET', wh, all[i]) or dw)
              if wi == nil or wi <= 0 then wi = dw end
              sum_w = sum_w + wi
            end
            if sum_w <= 0 then sum_w = dw end
          end
        end

        -- Two-pass, WORK-CONSERVING selection (see CHECKOUT_LUA_COUNT): a fair
        -- pass respecting the per-tenant fair-share cap, then a fallback pass that
        -- fills any remaining global budget ignoring the dynamic cap (honoring the
        -- absolute hard cap). Guarantees full utilization when few tenants are
        -- active without starving newcomers.
        local members = redis.call('ZRANGE', ring, 0, fetch_n)
        for round = 1, 2 do
          local fallback = (round == 2)
          for i = 1, #members do
            local t   = members[i]
            local tin = tonumber(redis.call('HGET', inf, t) or '0')

            local dispatchable
            if fallback then
              dispatchable = (cap == 0 or tin < cap)
            else
              local eff_cap = 0
              if budget > 0 then
                if weighted == 1 then
                  local w_t = tonumber(redis.call('HGET', wh, t) or dw)
                  if w_t == nil or w_t <= 0 then w_t = dw end
                  eff_cap = math.floor(budget * w_t / sum_w)
                  if eff_cap < 1 then eff_cap = 1 end
                else
                  eff_cap = math.ceil(budget / active)
                  if eff_cap < 1 then eff_cap = 1 end
                end
              end
              if cap > 0 and (eff_cap == 0 or cap < eff_cap) then eff_cap = cap end
              dispatchable = (eff_cap == 0 or tin < eff_cap)
            end

            if dispatchable then
              local rk  = rprefix .. t
              local job = redis.call('LPOP', rk)
              if job then
                if redis.call('LLEN', rk) == 0 then
                  redis.call('ZREM', ring, t)
                end
                -- vtime intentionally NOT advanced here; done at completion with
                -- the actual duration so accounting reflects wall-clock usage.
                redis.call('HINCRBY', inf, t, 1)
                redis.call('INCR', inftot)
                return {1, t, job}
              else
                redis.call('ZREM', ring, t)  -- stale entry: self-heal
              end
            end
          end
        end
        return {0, 'none'}
      LUA

      # ── Job-count mode completion ──────────────────────────────────────────
      # Just release the in-flight slot. Vtime was already advanced at checkout.
      COMPLETE_LUA_COUNT = <<~LUA.freeze
        local tin = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
        if tin > 0 then redis.call('HINCRBY', KEYS[1], ARGV[1], -1) end
        local tot = tonumber(redis.call('GET', KEYS[2]) or '0')
        if tot > 0 then redis.call('DECR', KEYS[2]) end
        return 1
      LUA

      # ── Time mode completion ───────────────────────────────────────────────
      # Release in-flight slot AND advance vtime by (duration_seconds / weight).
      # vtime_increment is pre-computed in Ruby using the process-local weight
      # cache — avoids a per-completion HGET on WEIGHT.
      # After advancing vtime, re-adds the tenant to the RING at the new score
      # if it still has queued ready jobs (ENQUEUE_LUA may have pushed more
      # while the job was in-flight, or the tenant was removed when its queue
      # ran dry at checkout time).
      # ZADD updates the score when the member already exists, so this is safe
      # regardless of whether the tenant is currently in the ring.
      COMPLETE_LUA_TIME = <<~LUA.freeze
        -- KEYS: [1]=INFLIGHT [2]=INFLIGHT_TOTAL [3]=VTIME [4]=RING
        -- ARGV: [1]=tenant   [2]=vtime_increment [3]=ready_prefix
        local t       = ARGV[1]
        local inc     = tonumber(ARGV[2])
        local rprefix = ARGV[3]

        local tin = tonumber(redis.call('HGET', KEYS[1], t) or '0')
        if tin > 0 then redis.call('HINCRBY', KEYS[1], t, -1) end
        local tot = tonumber(redis.call('GET', KEYS[2]) or '0')
        if tot > 0 then redis.call('DECR', KEYS[2]) end

        -- Advance vtime by actual slot-time consumed (duration / weight).
        -- VTIME is the durable ledger; it persists even when the tenant has no
        -- ready jobs and is absent from the RING, so a returning tenant is
        -- correctly charged for work it did earlier in the hour.
        local vt = tonumber(redis.call('HGET', KEYS[3], t) or '0') + inc
        redis.call('HSET', KEYS[3], t, vt)

        -- Re-add / update ring score only if the tenant still has ready jobs.
        local rk = rprefix .. t
        if redis.call('LLEN', rk) > 0 then
          redis.call('ZADD', KEYS[4], vt, t)
        end

        return 1
      LUA

      def initialize(pool: nil)
        cfg               = KafkaBatch.config
        @pool             = pool || ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
          KafkaBatch::RedisClient.new(cfg) || raise(ConfigurationError, "Redis is not configured")
        end
        @window           = cfg.fairness_ready_window.to_i
        @budget           = cfg.fairness_global_concurrency.to_i
        @cap              = cfg.fairness_max_inflight_per_tenant.to_i
        @default_weight   = cfg.fairness_default_weight.to_f
        @weighted         = cfg.fairness_weighted_concurrency ? 1 : 0
        @active_count_ttl    = cfg.fairness_active_count_ttl.to_f
        @active_count_source = cfg.fairness_active_count_source.to_sym
        @fairness_mode    = cfg.fairness_mode.to_sym   # :time_fairness | :job_count_fairness
        @weight_cache_ttl = cfg.fairness_weight_cache_ttl.to_f

        # Weight backend: where custom tenant weights are durably stored.
        #   :mysql  – kafka_batch_tenant_weights table (v1 migration). Redis is
        #             NOT required for weight reads or writes; the 60s process
        #             cache is the only layer in front of MySQL. CHECKOUT_LUA_COUNT
        #             (job-count mode) falls back to the default weight since the
        #             Redis WEIGHT hash is not populated in this path.
        #   :redis  – kafka_batch:fair:weight hash. Weights are read/written
        #             directly in Redis; no extra table needed.
        # Mirrors config.store so callers need not configure this separately.
        @weight_backend   = cfg.store  # :mysql | :redis

        # How many ring entries to scan per checkout. Must exceed the number of
        # simultaneously-capped tenants in the worst case. With budget B and
        # per-tenant cap C, at most floor(B/C) + headroom tenants can be at cap.
        # budget * 3 is a generous bound that covers all practical scenarios
        # while reducing ZRANGE data transfer from O(N_tenants) to O(budget*3).
        @fetch_n          = [(@budget * 3), 60].max

        # Process-local weight cache (see cached_weights / weight_for).
        @weights_mutex    = Mutex.new
        @weights_cache    = nil
        @weights_cache_at = 0.0

        # Process-local smoothed active-tenant view (see active_view).
        @active_mutex     = Mutex.new
        @active_view      = nil
        @active_view_at   = 0.0
      end

      # Smoothed active-tenant view used as the cap denominator in #checkout.
      # Returns { count:, sum_weight: }, refreshed at most once per
      # fairness_active_count_ttl seconds (default 5s) per process so the cap
      # tracks the real active set instead of the flickering instantaneous ring.
      # `count` is used as a floor (max with ZCARD(ring)) inside the Lua so it is
      # responsive to load increases but stable against transient drains.
      # @return [Hash{count: Integer, sum_weight: Float}]
      def active_view
        now = monotonic
        @active_mutex.synchronize do
          if @active_view.nil? || (now - @active_view_at) >= @active_count_ttl
            @active_view    = compute_active_view
            @active_view_at = now
          end
          @active_view
        end
      end

      # Add a job to a tenant's bounded ready window.
      # @return [:ok, :full]
      def enqueue(tenant_id, payload)
        ok = with do |r|
          r.eval(ENQUEUE_LUA,
            keys: [RING, VTIME],
            argv: [tenant_id.to_s, payload, @window.to_s, READY_PREFIX])
        end
        ok == 1 ? :ok : :full
      end

      # Pull the next job fairly, respecting the global budget and optional
      # per-tenant inflight cap. Returns a hash or nil when budget exhausted /
      # nothing ready.
      # @return [Hash{tenant_id:, payload:}, nil]
      def checkout
        view  = active_view                       # smoothed, cached (see active_view)
        ahint = view[:count].to_s
        shint = view[:sum_weight].to_s
        res = with do |r|
          if @fairness_mode == :time_fairness
            r.eval(CHECKOUT_LUA_TIME,
              keys: [RING, INFLIGHT, INFLIGHT_TOTAL, WEIGHT],
              argv: [@budget.to_s, @cap.to_s, READY_PREFIX, @fetch_n.to_s, @default_weight.to_s, @weighted.to_s, ahint, shint])
          else
            r.eval(CHECKOUT_LUA_COUNT,
              keys: [RING, VTIME, INFLIGHT, INFLIGHT_TOTAL, WEIGHT],
              argv: [@budget.to_s, @cap.to_s, READY_PREFIX, @default_weight.to_s, @fetch_n.to_s, @weighted.to_s, ahint, shint])
          end
        end
        code, a, b = res
        code == 1 ? { tenant_id: a, payload: b } : nil
      end

      # Release the in-flight slot held by a tenant's finished job.
      #
      # In :time_fairness mode, pass `duration:` (actual wall-clock seconds the
      # job ran). The scheduler advances vtime by duration/weight so the tenant
      # is charged for the time it consumed. Omitting duration treats the job as
      # zero-duration (tenant gets a free pass for that slot — avoid this).
      #
      # In :job_count_fairness mode, duration is ignored — vtime was already
      # advanced at checkout.
      #
      # @param tenant_id [String]
      # @param duration  [Numeric, nil]  seconds the job ran (required in :time_fairness)
      def complete(tenant_id, duration: nil)
        t = tenant_id.to_s
        if @fairness_mode == :time_fairness
          dur = (duration || 0).to_f
          w   = weight_for(t)
          inc = w > 0 ? dur / w : dur
          with do |r|
            r.eval(COMPLETE_LUA_TIME,
              keys: [INFLIGHT, INFLIGHT_TOTAL, VTIME, RING],
              argv: [t, inc.to_s, READY_PREFIX])
          end
        else
          with do |r|
            r.eval(COMPLETE_LUA_COUNT, keys: [INFLIGHT, INFLIGHT_TOTAL], argv: [t])
          end
        end
        nil
      end

      # Set a per-tenant weight override. Persists to the configured weight
      # backend (MySQL table or Redis hash) and immediately busts this process's
      # cache so weight_for returns the new value on the next call.
      # Other dispatcher processes pick up the change on their next cache expiry
      # (within fairness_weight_cache_ttl seconds, default 60s).
      def set_weight(tenant_id, weight)
        write_weight_to_backend(tenant_id.to_s, weight.to_f)
        bust_weight_cache!
        nil
      end

      # Remove a tenant's custom weight override (reverts to default_weight).
      def delete_weight(tenant_id)
        remove_weight_from_backend(tenant_id.to_s)
        bust_weight_cache!
        nil
      end

      # Register a batch of tenant IDs as "seen" without touching WFQ mechanics
      # (ring position, inflight counts, vtime advancement). Uses HSETNX so
      # existing entries (with real accumulated vtime) are never overwritten.
      # Called by the Dispatcher whenever it forwards ingest → ready so that
      # tenants appear on the /weights page automatically on their first job.
      def touch_tenants(tenant_ids)
        return if tenant_ids.empty?

        with do |r|
          r.pipelined do |pipe|
            tenant_ids.each { |t| pipe.hsetnx(VTIME, t.to_s, 0) }
          end
        end
      end

      # All tenants currently known to the scheduler — union of tenants with
      # custom weights (from the configured backend), active inflight jobs,
      # queued ready jobs, and tenants seen via touch_tenants (from the
      # Kafka-native Dispatcher) — enriched with runtime state from Redis.
      # Used by the weights dashboard to populate the table dynamically.
      # @return [Array<Hash>]
      def all_tenants
        # Weights from the configured backend (MySQL table or Redis hash).
        backend_weights = fetch_all_weights_from_backend

        # Runtime counters always live in Redis (ring, inflight, vtime).
        inflight, ring_members, vtimes = with do |r|
          r.pipelined do |pipe|
            pipe.hgetall(INFLIGHT)
            pipe.zrange(RING, 0, -1)
            pipe.hgetall(VTIME)
          end
        end

        # Include vtimes.keys so tenants registered via touch_tenants (Kafka-
        # native Dispatcher path) appear even before any Scheduler checkout.
        all_ids = (backend_weights.keys + inflight.keys + ring_members + vtimes.keys).uniq.sort
        all_ids.map do |t|
          custom_w = backend_weights[t]
          {
            tenant_id:         t,
            weight:            custom_w || @default_weight,
            has_custom_weight: !custom_w.nil?,
            inflight:          inflight[t].to_i,
            queued:            ring_members.include?(t),
            vtime:             vtimes[t].to_f
          }
        end
      end

      # Depth of a single tenant's ready queue.
      def ready_depth(tenant_id)
        with { |r| r.llen("#{READY_PREFIX}#{tenant_id}") }
      end

      # Snapshot for dashboards / metrics.
      def stats
        active, total = with { |r| r.multi { |m| m.zcard(RING); m.get(INFLIGHT_TOTAL) } }
        {
          active_tenants: active.to_i,
          inflight_total: total.to_i,
          budget:         @budget,
          window:         @window,
          fairness_mode:  @fairness_mode,
          weight_backend: @weight_backend
        }
      end

      # In-flight count per tenant (for the fairness dashboard).
      def inflight_by_tenant
        with { |r| r.hgetall(INFLIGHT) }.transform_values(&:to_i).reject { |_, v| v.zero? }
      end

      # Configured fairness mode.
      attr_reader :fairness_mode

      # Configured default weight (used when a tenant has no override).
      attr_reader :default_weight

      # Remove all scheduler state (tests / full reset). Does NOT touch Kafka.
      def reset!
        with do |r|
          keys = r.scan_each(match: "#{NS}*").to_a
          r.del(*keys) unless keys.empty?
        end
        bust_weight_cache!
      end

      private

      # ── Weight cache ─────────────────────────────────────────────────────────
      #
      # The process-level cache is always the first layer regardless of backend.
      # It holds all custom weights as a Hash{ tenant_id => Float } and is
      # refreshed at most once per @weight_cache_ttl seconds (default 60s).
      # This eliminates a backend round-trip on every job completion (time
      # fairness) without sacrificing responsiveness: a weight change written via
      # the UI propagates to all dispatcher processes within the TTL window.
      #
      # The Mutex prevents concurrent refreshes on JRuby / TruffleRuby. On MRI
      # the GIL already prevents races; the mutex is a no-cost safety net.
      #
      # MySQL backend specifics
      # ───────────────────────
      # Redis is NOT required for weight reads or writes. The process cache is
      # the sole optimization layer in front of MySQL. CHECKOUT_LUA_COUNT (job-
      # count fairness mode) reads the Redis WEIGHT hash inside Lua — when MySQL
      # is the backend that hash stays empty and Lua falls back to `dw` (the
      # default weight passed as ARGV). If Redis happens to be configured you can
      # opt in to populating the WEIGHT hash by calling sync_weights_to_redis
      # explicitly, but the scheduler does NOT do so automatically so that Redis
      # remains a pure WFQ-mechanics dependency and not a weight-storage one.
      def cached_weights
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @weights_mutex.synchronize do
          if @weights_cache.nil? || (now - @weights_cache_at) >= @weight_cache_ttl
            @weights_cache    = fetch_all_weights_from_backend
            @weights_cache_at = now
          end
          @weights_cache
        end
      end

      def weight_for(tenant_id)
        w = cached_weights[tenant_id.to_s]
        (w && w > 0) ? w : @default_weight
      end

      def bust_weight_cache!
        @weights_mutex.synchronize { @weights_cache_at = 0.0 }
      end

      # ── Active-tenant view ─────────────────────────────────────────────────
      # Compute the current active-tenant count (and, for weighted mode, the sum
      # of their weights). Called at most once per fairness_active_count_ttl.
      def compute_active_view
        if @active_count_source == :ingest_lag
          # Kafka-backlog notion: number of ingest partitions with lag > 0.
          # Cannot supply per-tenant weights → sum_weight 0 (weighted mode then
          # falls back to the ring weight sum inside the Lua).
          { count: ingest_active_count, sum_weight: 0.0 }
        else
          # Default: distinct tenants with in-flight OR queued (ready) work.
          ring_members, inflight = with do |r|
            r.pipelined do |pipe|
              pipe.zrange(RING, 0, -1)
              pipe.hgetall(INFLIGHT)
            end
          end
          active_ids = ring_members.to_a
          inflight.each { |t, n| active_ids << t if n.to_i > 0 }
          active_ids.uniq!
          sum_w = @weighted == 1 ? active_ids.sum { |t| weight_for(t) } : 0.0
          { count: active_ids.size, sum_weight: sum_w }
        end
      rescue => e
        KafkaBatch.logger.warn("[KafkaBatch][Scheduler] active_view compute failed: #{e.message}")
        { count: 0, sum_weight: 0.0 }
      end

      # Count of ingest-topic partitions with lag > 0 (≈ tenants with Kafka-side
      # backlog). Requires the Karafka Admin API; returns 0 if unavailable.
      def ingest_active_count
        return 0 unless defined?(KafkaBatch::Lag) && KafkaBatch::Lag.available?

        group = KafkaBatch.dispatch_consumer_group
        topic = KafkaBatch.config.fairness_ingest_topic
        data  = KafkaBatch::Lag.read_group(group, [topic])
        parts = (data[group] || {})[topic] || {}
        parts.values.count { |i| i[:lag].to_i > 0 }
      rescue => e
        KafkaBatch.logger.warn("[KafkaBatch][Scheduler] ingest_active_count failed: #{e.message}")
        0
      end

      def monotonic
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      # ── Weight backend helpers ────────────────────────────────────────────────

      def mysql_weight_backend?
        @weight_backend == :mysql
      end

      # Read all custom weight overrides from the configured backend.
      # MySQL path: SELECT * FROM kafka_batch_tenant_weights (small table).
      # Redis path: HGETALL kafka_batch:fair:weight.
      # Returns Hash{ tenant_id_string => Float }.
      def fetch_all_weights_from_backend
        if mysql_weight_backend?
          weight_record_class.all.each_with_object({}) do |r, h|
            h[r.tenant_id.to_s] = r.weight.to_f
          end
        else
          with { |r| r.hgetall(WEIGHT) }.transform_values(&:to_f)
        end
      rescue => e
        KafkaBatch.logger.warn("[KafkaBatch][Scheduler] fetch_all_weights failed: #{e.message}")
        {}
      end

      # Persist a single weight override to the configured backend.
      # MySQL: raw ON DUPLICATE KEY UPDATE — bypasses AR's index-reflection so the
      #   anonymous model works correctly in all environments (AR's upsert/unique_by
      #   can fail to reflect indexes on anonymous models in some AR versions).
      #   Does NOT touch Redis — the process cache is the only caching layer.
      # Redis: HSET kafka_batch:fair:weight — also serves CHECKOUT_LUA_COUNT.
      def write_weight_to_backend(tenant_id, weight)
        if mysql_weight_backend?
          conn = weight_record_class.connection
          qt   = conn.quote(tenant_id.to_s)
          qw   = conn.quote(weight.to_f)
          qnow = conn.quote(Time.now.utc.strftime("%Y-%m-%d %H:%M:%S"))
          conn.execute(<<~SQL)
            INSERT INTO kafka_batch_tenant_weights (tenant_id, weight, updated_at)
            VALUES (#{qt}, #{qw}, #{qnow})
            ON DUPLICATE KEY UPDATE weight = VALUES(weight), updated_at = VALUES(updated_at)
          SQL
          # Mirror to the Redis WEIGHT hash so the WFQ Lua (checkout) can read the
          # weight directly. Without this, weighting (selection order AND weighted
          # concurrency) would silently no-op on the MySQL backend. Best-effort.
          mirror_weight_to_redis(tenant_id, weight)
        else
          with { |r| r.hset(WEIGHT, tenant_id, weight) }
        end
      rescue => e
        KafkaBatch.logger.error("[KafkaBatch][Scheduler] write_weight failed for #{tenant_id}: #{e.message}")
        raise
      end

      # Remove a weight override from the configured backend.
      def remove_weight_from_backend(tenant_id)
        if mysql_weight_backend?
          weight_record_class.where(tenant_id: tenant_id).delete_all
          mirror_weight_delete_to_redis(tenant_id)
        else
          with { |r| r.hdel(WEIGHT, tenant_id) }
        end
      rescue => e
        KafkaBatch.logger.error("[KafkaBatch][Scheduler] remove_weight failed for #{tenant_id}: #{e.message}")
        raise
      end

      # Mirror a custom weight into the Redis WEIGHT hash (MySQL backend only, so
      # the Lua can read it). Failures are logged, not fatal — the durable value
      # is already in MySQL.
      def mirror_weight_to_redis(tenant_id, weight)
        with { |r| r.hset(WEIGHT, tenant_id, weight) }
      rescue => e
        KafkaBatch.logger.warn("[KafkaBatch][Scheduler] weight Redis mirror failed for #{tenant_id}: #{e.message}")
      end

      def mirror_weight_delete_to_redis(tenant_id)
        with { |r| r.hdel(WEIGHT, tenant_id) }
      rescue => e
        KafkaBatch.logger.warn("[KafkaBatch][Scheduler] weight Redis unmirror failed for #{tenant_id}: #{e.message}")
      end

      # Anonymous ActiveRecord model for kafka_batch_tenant_weights.
      # Only instantiated when @weight_backend == :mysql.
      # inheritance_column = nil suppresses STI activation (same pattern used
      # throughout the rest of the codebase).
      def weight_record_class
        @weight_record_class ||= Class.new(ActiveRecord::Base) do
          self.table_name         = "kafka_batch_tenant_weights"
          self.inheritance_column = nil
        end
      end

      def with(&block)
        @pool.with(&block)
      end
    end
  end
end
