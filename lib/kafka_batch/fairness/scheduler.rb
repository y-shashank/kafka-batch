require "redis"
require "connection_pool"
require "securerandom"

module KafkaBatch
  module Fairness
    # Redis-backed Weighted Fair Queuing (WFQ) scheduler for one FAIRNESS LANE.
    # Two independent fairness lanes run simultaneously, one instance each, selected per-worker via
    # `fairness_type` (:time | :throughput):
    #
    #   * the durable backlog stays in Kafka (the lane's ingest topic);
    #   * a dispatcher copies jobs into a BOUNDED per-tenant "ready" window here;
    #   * a forwarder pulls the next job via #checkout (fair across tenants,
    #     global concurrency budget) onto the lane's ready topic.
    #
    # ── Lane / fairness type ───────────────────────────────────────────────────
    #
    # :time (recommended for uneven runtimes)
    #   Virtual time advances at job *completion* by actual_seconds / weight, so
    #   each tenant gets roughly equal (weighted) slot-time. Callers MUST pass
    #   `duration:` to #complete.
    #
    # :throughput (original job-count behaviour)
    #   Virtual time advances by 1/weight at *dispatch*. Fair over job count.
    #
    # Each lane keeps its state under its own Redis namespace
    # (kafka_batch:fair_time:* / kafka_batch:fair_throughput:*) and its own tenant
    # weights (per-lane Redis WEIGHT hash), so the two lanes never interfere.
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
    # config.fairness_weight_cache_ttl seconds, populated from the lane's WEIGHT hash.
    #
    class Scheduler
      # Minimum in-flight lease TTL (seconds). A lease shorter than a job's runtime
      # would be reclaimed mid-run (concurrency overshoot); a value of 0 would make
      # every lease expire on the next checkout, silently DISABLING the concurrency
      # budget. So fairness_lease_ttl is floored to this.
      LEASE_TTL_FLOOR = 60.0

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

      # ── Job-count (throughput) fairness checkout ───────────────────────────
      # Vtime advances by 1/weight at dispatch. Fair over dispatch count.
      # Weight is looked up from the WEIGHT hash inside Lua (one HGET per
      # successful checkout — acceptable since the Lua call is already one
      # Redis round-trip).
      # ── In-flight accounting: authoritative TTL leases (no drift-prone counter) ──
      # In-flight is the set of live LEASES, not an incremented counter. Each slot
      # is a ZSET member (slot_id) scored by its expiry (now + lease_ttl), held in
      # BOTH a global ZSET (KEYS: leases → authoritative budget) and a per-tenant
      # ZSET (lprefix..tenant → authoritative fair-share cap). `ZREMRANGEBYSCORE …
      # -inf now` drops expired leases (a dead consumer's slot), then `ZCARD` is the
      # live count. Because the count is DERIVED from the leases every checkout, a
      # leaked slot (SIGKILL/OOM) or a pre-upgrade counter can NEVER permanently pin
      # the lane — it self-heals the moment its lease expires. Completion removes the
      # slot by id, so it is idempotent and immune to double-release.
      #
      # ARGV[5] = fetch_n (bounded scan depth; avoids full ZRANGE on large rings)
      # ARGV[6] = weighted (1 = weight-proportional per-tenant cap, 0 = equal share)
      # ARGV[9]=now  ARGV[10]=lease_ttl  ARGV[11]=slot_id  ARGV[12]=lease_prefix
      CHECKOUT_LUA_COUNT = <<~LUA.freeze
        local ring     = KEYS[1]
        local vth      = KEYS[2]
        local gl       = KEYS[3]   -- global leases ZSET (authoritative budget)
        local wh       = KEYS[4]
        local budget   = tonumber(ARGV[1])
        local cap      = tonumber(ARGV[2])
        local rprefix  = ARGV[3]
        local dw       = tonumber(ARGV[4])
        local fetch_n  = tonumber(ARGV[5]) - 1
        local weighted = tonumber(ARGV[6])
        local ahint    = tonumber(ARGV[7]) or 0   -- smoothed active-tenant count (cached in Ruby)
        local shint    = tonumber(ARGV[8]) or 0   -- smoothed sum of active weights (cached in Ruby)
        local now      = tonumber(ARGV[9])  or 0
        local ttl      = tonumber(ARGV[10]) or 0
        local slot_id  = ARGV[11]
        local lprefix  = ARGV[12]

        -- Authoritative global budget: drop expired leases, then count the live ones.
        redis.call('ZREMRANGEBYSCORE', gl, '-inf', now)
        local total = redis.call('ZCARD', gl)
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
            local t  = members[i]
            local lk = lprefix .. t
            -- Per-tenant live in-flight: prune this tenant's expired leases, then
            -- ZCARD. Authoritative (never drifts) and self-heals leaked slots.
            redis.call('ZREMRANGEBYSCORE', lk, '-inf', now)
            local tin = redis.call('ZCARD', lk)

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
                -- Record the lease (same slot_id) in the global AND per-tenant sets.
                local exp = now + ttl
                redis.call('ZADD', gl, exp, slot_id)
                redis.call('ZADD', lk, exp, slot_id)
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
      # (COMPLETE_LUA_TIME_LEASE) using the actual job duration. The ring ordering
      # reflects accumulated slot-time consumed, not dispatch count.
      # WEIGHT hash (KEYS[4]) + dw (ARGV[5]) are used only for the weighted cap.
      # ARGV[6] = weighted (1 = weight-proportional per-tenant cap, 0 = equal share)
      # ARGV[9]=now  ARGV[10]=lease_ttl  ARGV[11]=slot_id  ARGV[12]=lease_prefix
      CHECKOUT_LUA_TIME = <<~LUA.freeze
        local ring     = KEYS[1]
        local gl       = KEYS[2]   -- global leases ZSET (authoritative budget)
        local wh       = KEYS[3]
        local budget   = tonumber(ARGV[1])
        local cap      = tonumber(ARGV[2])
        local rprefix  = ARGV[3]
        local fetch_n  = tonumber(ARGV[4]) - 1
        local dw       = tonumber(ARGV[5])
        local weighted = tonumber(ARGV[6])
        local ahint    = tonumber(ARGV[7]) or 0   -- smoothed active-tenant count (cached in Ruby)
        local shint    = tonumber(ARGV[8]) or 0   -- smoothed sum of active weights (cached in Ruby)
        local now      = tonumber(ARGV[9])  or 0
        local ttl      = tonumber(ARGV[10]) or 0
        local slot_id  = ARGV[11]
        local lprefix  = ARGV[12]

        -- Authoritative global budget: drop expired leases, then count the live ones.
        redis.call('ZREMRANGEBYSCORE', gl, '-inf', now)
        local total = redis.call('ZCARD', gl)
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
            local t  = members[i]
            local lk = lprefix .. t
            -- Per-tenant live in-flight: prune expired leases, then ZCARD.
            redis.call('ZREMRANGEBYSCORE', lk, '-inf', now)
            local tin = redis.call('ZCARD', lk)

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
                local exp = now + ttl
                redis.call('ZADD', gl, exp, slot_id)
                redis.call('ZADD', lk, exp, slot_id)
                return {1, t, job}
              else
                redis.call('ZREM', ring, t)  -- stale entry: self-heal
              end
            end
          end
        end
        return {0, 'none'}
      LUA

      # ── Completion: release the lease (authoritative, idempotent) ───────────
      # Release removes THIS job's slot_id from the global AND per-tenant lease
      # ZSETs. Because in-flight is DERIVED from the leases (ZCARD), there is no
      # counter to under/over-shoot: a redelivered completion, or one for a slot
      # already reclaimed by expiry, removes nothing and is a harmless no-op.
      #   KEYS[1]=global leases  KEYS[2]=per-tenant lease zset   ARGV[1]=slot_id
      COMPLETE_LUA_COUNT_LEASE = <<~LUA.freeze
        redis.call('ZREM', KEYS[1], ARGV[1])
        redis.call('ZREM', KEYS[2], ARGV[1])
        return 1
      LUA

      # Time-lane completion: release the lease, then advance vtime by
      # (duration / weight) — but ONLY when this call actually released a live
      # lease (per-tenant ZREM == 1). Gating vtime on the release makes completion
      # idempotent: an at-least-once redelivery (or a completion after the lease
      # already expired) neither double-charges the tenant nor double-releases.
      #   KEYS[1]=global leases KEYS[2]=per-tenant lease KEYS[3]=vtime KEYS[4]=ring
      #   ARGV[1]=tenant ARGV[2]=vtime_increment ARGV[3]=ready_prefix ARGV[4]=slot_id
      COMPLETE_LUA_TIME_LEASE = <<~LUA.freeze
        local t       = ARGV[1]
        local inc     = tonumber(ARGV[2])
        local rprefix = ARGV[3]

        redis.call('ZREM', KEYS[1], ARGV[4])
        if redis.call('ZREM', KEYS[2], ARGV[4]) == 1 then
          -- VTIME is the durable ledger; it persists even when the tenant has no
          -- ready jobs and is absent from the RING, so a returning tenant is
          -- correctly charged for work it did earlier.
          local vt = tonumber(redis.call('HGET', KEYS[3], t) or '0') + inc
          redis.call('HSET', KEYS[3], t, vt)
          -- Re-add / update ring score only if the tenant still has ready jobs.
          local rk = rprefix .. t
          if redis.call('LLEN', rk) > 0 then
            redis.call('ZADD', KEYS[4], vt, t)
          end
        end

        return 1
      LUA

      # Legacy time-lane completion for a message forwarded BEFORE the lease
      # upgrade (no _fair_slot_id → no lease exists). There is no slot to release
      # — just advance vtime so the tenant is charged for the time it consumed.
      # Throughput-lane legacy messages need no completion at all (their vtime
      # advanced at checkout), so Ruby skips the Redis call entirely for those.
      #   KEYS[1]=vtime KEYS[2]=ring  ARGV[1]=tenant ARGV[2]=vtime_increment ARGV[3]=ready_prefix
      COMPLETE_LUA_TIME_LEGACY = <<~LUA.freeze
        local t   = ARGV[1]
        local inc = tonumber(ARGV[2])
        local vt  = tonumber(redis.call('HGET', KEYS[1], t) or '0') + inc
        redis.call('HSET', KEYS[1], t, vt)
        local rk = ARGV[3] .. t
        if redis.call('LLEN', rk) > 0 then
          redis.call('ZADD', KEYS[2], vt, t)
        end
        return 1
      LUA

      # @param type [Symbol] :time | :throughput — the fairness lane this
      #   scheduler instance drives. Determines the Redis namespace, the vtime
      #   accounting mode, and which tenant-weight set is used.
      def initialize(type: :time, pool: nil)
        cfg               = KafkaBatch.config
        @type             = type.to_sym
        @time_mode        = (@type == :time)
        @fairness_type    = @type.to_s

        # Per-lane Redis namespace + keys.
        @ns               = "kafka_batch:fair_#{@type}"
        @ring             = "#{@ns}:ring"            # ZSET tenant => vtime (ready tenants only)
        @vtime            = "#{@ns}:vtime"           # HASH tenant => remembered vtime
        @weight           = "#{@ns}:weight"          # HASH tenant => weight override
        @ready_prefix     = "#{@ns}:ready:"          # LIST per tenant
        # In-flight is tracked as TTL LEASES, not a counter — the count is derived
        # from these ZSETs (ZCARD of live members), so it can never drift or pin.
        @leases           = "#{@ns}:leases"          # ZSET slot_id => expiry (authoritative GLOBAL in-flight)
        @lease_prefix     = "#{@ns}:lease:"          # ZSET per tenant: slot_id => expiry (authoritative per-tenant in-flight)
        @reclaim_lock     = "#{@ns}:reclaim_lock"    # single-flight lock for the periodic sweep

        @pool             = pool || ConnectionPool.new(size: cfg.redis_pool_size, timeout: 5) do
          KafkaBatch::RedisClient.new(cfg) || raise(ConfigurationError, "Redis is not configured")
        end
        @window           = cfg.fairness_ready_window.to_i
        @budget           = cfg.fairness_global_concurrency.to_i
        @cap              = cfg.fairness_max_inflight_per_tenant.to_i
        raw_lease_ttl     = cfg.fairness_lease_ttl.to_f
        @lease_ttl        = raw_lease_ttl >= LEASE_TTL_FLOOR ? raw_lease_ttl : LEASE_TTL_FLOOR
        if raw_lease_ttl < LEASE_TTL_FLOOR
          KafkaBatch.logger.warn(
            "[KafkaBatch][Scheduler] fairness_lease_ttl=#{raw_lease_ttl}s is below the " \
            "#{LEASE_TTL_FLOOR.to_i}s floor; using #{LEASE_TTL_FLOOR.to_i}s. Set it above your " \
            "longest job runtime to avoid premature in-flight slot reclaim."
          )
        end
        @default_weight   = cfg.fairness_default_weight.to_f
        @weighted         = cfg.fairness_weighted_concurrency ? 1 : 0
        @active_count_ttl    = cfg.fairness_active_count_ttl.to_f
        @active_count_source = cfg.fairness_active_count_source.to_sym
        @weight_cache_ttl = cfg.fairness_weight_cache_ttl.to_f

        @fetch_n          = [(@budget * 3), 60].max

        @weights_mutex    = Mutex.new
        @weights_cache    = nil
        @weights_cache_at = 0.0

        @active_mutex     = Mutex.new
        @active_view      = nil
        @active_view_at   = 0.0
      end

      # The fairness lane this instance drives (:time | :throughput).
      attr_reader :type

      # Per-lane Redis key names (exposed for tooling / tests).
      attr_reader :ns, :ring, :vtime, :weight, :ready_prefix, :leases, :lease_prefix

      # Smoothed active-tenant view used as the cap denominator in #checkout.
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
            keys: [@ring, @vtime],
            argv: [tenant_id.to_s, payload, @window.to_s, @ready_prefix])
        end
        ok == 1 ? :ok : :full
      end

      # Pull the next job fairly, respecting the global budget and optional
      # per-tenant inflight cap. Returns a hash or nil when budget exhausted /
      # nothing ready.
      #
      # Every successful checkout writes an in-flight LEASE (slot_id => now+ttl)
      # in the selected tenant's lease ZSET. The slot_id is returned so the
      # Forwarder can carry it on the ready message; Scheduler#complete removes
      # exactly that lease. If the consumer dies before completing, the lease
      # expires and the reclaim pre-pass frees the slot automatically.
      # @return [Hash{tenant_id:, payload:, slot_id:}, nil]
      def checkout
        view    = active_view                     # smoothed, cached (see active_view)
        ahint   = view[:count].to_s
        shint   = view[:sum_weight].to_s
        slot_id = SecureRandom.uuid
        now     = Time.now.to_f.to_s
        res = with do |r|
          if @time_mode
            r.eval(CHECKOUT_LUA_TIME,
              keys: [@ring, @leases, @weight],
              argv: [@budget.to_s, @cap.to_s, @ready_prefix, @fetch_n.to_s, @default_weight.to_s, @weighted.to_s, ahint, shint,
                     now, @lease_ttl.to_s, slot_id, @lease_prefix])
          else
            r.eval(CHECKOUT_LUA_COUNT,
              keys: [@ring, @vtime, @leases, @weight],
              argv: [@budget.to_s, @cap.to_s, @ready_prefix, @default_weight.to_s, @fetch_n.to_s, @weighted.to_s, ahint, shint,
                     now, @lease_ttl.to_s, slot_id, @lease_prefix])
          end
        end
        code, a, b = res
        code == 1 ? { tenant_id: a, payload: b, slot_id: slot_id } : nil
      end

      # Release the in-flight slot held by a tenant's finished job.
      #
      # In the :time lane, pass `duration:` (actual wall-clock seconds the job
      # ran). vtime advances by duration/weight. In the :throughput lane duration
      # is ignored — vtime was already advanced at checkout.
      #
      # @param tenant_id [String]
      # @param slot_id   [String, nil]   the lease id carried on the ready message
      #   (_fair_slot_id). When present, only the matching lease is released, so a
      #   slot already reclaimed by expiry or a redelivered completion is a no-op.
      #   nil → legacy path (message forwarded before the lease upgrade).
      # @param duration  [Numeric, nil]  seconds the job ran (required in :time)
      def complete(tenant_id, slot_id: nil, duration: nil)
        t     = tenant_id.to_s
        lease = slot_id.to_s
        if @time_mode
          dur = (duration || 0).to_f
          w   = weight_for(t)
          inc = w > 0 ? dur / w : dur
          with do |r|
            if lease.empty?
              # Pre-upgrade message (no lease): no slot to release, just charge vtime.
              r.eval(COMPLETE_LUA_TIME_LEGACY,
                keys: [@vtime, @ring],
                argv: [t, inc.to_s, @ready_prefix])
            else
              r.eval(COMPLETE_LUA_TIME_LEASE,
                keys: [@leases, "#{@lease_prefix}#{t}", @vtime, @ring],
                argv: [t, inc.to_s, @ready_prefix, lease])
            end
          end
        elsif !lease.empty?
          with do |r|
            r.eval(COMPLETE_LUA_COUNT_LEASE,
              keys: [@leases, "#{@lease_prefix}#{t}"],
              argv: [lease])
          end
        end
        # Throughput-lane legacy (no lease): nothing to do — vtime advanced at
        # checkout and there is no lease to release. Skip the Redis round-trip.
        nil
      end

      # Drop expired leases from the global set and every per-tenant set, freeing
      # memory for tenants that leaked slots and then went idle. This is only a
      # memory/hygiene sweep: the budget and per-tenant caps are ALREADY
      # authoritative on every checkout (they ZREMRANGEBYSCORE + ZCARD the live
      # leases), so nothing is ever pinned waiting for this. Single-flighted via a
      # short Redis lock so N forwarder pods don't all sweep the keyspace at once.
      # @return [Integer] number of expired global leases reclaimed (0 if another
      #   pod holds the sweep lock or Redis is unavailable)
      def reclaim_expired_leases!
        now = Time.now.to_f
        with do |r|
          # nx+ex: only one pod sweeps per interval; the lock self-expires.
          return 0 unless r.set(@reclaim_lock, "1", nx: true, ex: 25)

          reclaimed = r.zremrangebyscore(@leases, "-inf", now)
          cursor    = "0"
          loop do
            cursor, keys = r.scan(cursor, match: "#{@lease_prefix}*", count: 500)
            keys.each { |lk| r.zremrangebyscore(lk, "-inf", now) }
            break if cursor == "0"
          end
          reclaimed
        end
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Scheduler] reclaim_expired_leases! failed: #{e.message}")
        0
      end

      # Set a per-tenant weight override for THIS lane.
      def set_weight(tenant_id, weight)
        write_weight_to_backend(tenant_id.to_s, weight.to_f)
        bust_weight_cache!
        nil
      end

      # Remove a tenant's custom weight override for THIS lane.
      def delete_weight(tenant_id)
        remove_weight_from_backend(tenant_id.to_s)
        bust_weight_cache!
        nil
      end

      # Register tenant IDs as "seen" without touching WFQ mechanics (HSETNX so
      # existing accumulated vtime is never overwritten). Called by the Dispatcher
      # so tenants appear on the weights page on their first job.
      def touch_tenants(tenant_ids)
        return if tenant_ids.empty?

        with do |r|
          r.pipelined do |pipe|
            tenant_ids.each { |t| pipe.hsetnx(@vtime, t.to_s, 0) }
          end
        end
      end

      # All tenants known to THIS lane — union of custom weights, in-flight,
      # queued, and touched tenants — enriched with runtime state.
      # @return [Array<Hash>]
      def all_tenants
        backend_weights = fetch_all_weights_from_backend

        ring_members, vtimes = with do |r|
          r.pipelined do |pipe|
            pipe.zrange(@ring, 0, -1)
            pipe.hgetall(@vtime)
          end
        end
        inflight = inflight_by_tenant   # live per-tenant lease counts (separate call)

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
        with { |r| r.llen("#{@ready_prefix}#{tenant_id}") }
      end

      # Snapshot for dashboards / metrics. inflight_total is the number of LIVE
      # leases (expiry in the future) in the global set — authoritative, never a
      # stale counter. Read-only: it does not prune (checkout/sweep do that).
      def stats
        now = Time.now.to_f
        active, live = with { |r| r.multi { |m| m.zcard(@ring); m.zcount(@leases, "(#{now}", "+inf") } }
        {
          type:           @type,
          active_tenants: active.to_i,
          inflight_total: live.to_i,
          budget:         @budget,
          window:         @window
        }
      end

      # Live in-flight count per tenant (for the fairness dashboard), derived from
      # each tenant's lease ZSET. Counts only leases whose expiry is still in the
      # future, so a dead pod's leaked slots never show as in-flight. Read-only.
      def inflight_by_tenant
        now = Time.now.to_f
        out = {}
        with do |r|
          cursor = "0"
          loop do
            cursor, keys = r.scan(cursor, match: "#{@lease_prefix}*", count: 500)
            unless keys.empty?
              counts = r.pipelined { |p| keys.each { |k| p.zcount(k, "(#{now}", "+inf") } }
              keys.each_with_index { |k, i| out[k.sub(@lease_prefix, "")] = counts[i] if counts[i].to_i.positive? }
            end
            break if cursor == "0"
          end
        end
        out
      end

      # Configured default weight (used when a tenant has no override).
      attr_reader :default_weight

      # Remove all scheduler state for THIS lane (tests / full reset).
      def reset!
        with do |r|
          keys = r.scan_each(match: "#{@ns}*").to_a
          r.del(*keys) unless keys.empty?
        end
        bust_weight_cache!
      end

      private

      # ── Weight cache ─────────────────────────────────────────────────────────
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
        @weights_mutex.synchronize do
          @weights_cache    = nil
          @weights_cache_at = 0.0
        end
      end

      # ── Active-tenant view ─────────────────────────────────────────────────
      def compute_active_view
        if @active_count_source == :ingest_lag
          { count: ingest_active_count, sum_weight: 0.0 }
        else
          # Active = tenants with ready jobs (ring) OR live in-flight leases. Two
          # sequential calls (NOT nested `with`) so we never hold two pool conns.
          ring_members = with { |r| r.zrange(@ring, 0, -1) }
          active_ids   = (ring_members.to_a + inflight_by_tenant.keys).uniq
          sum_w = @weighted == 1 ? active_ids.sum { |t| weight_for(t) } : 0.0
          { count: active_ids.size, sum_weight: sum_w }
        end
      rescue => e
        KafkaBatch.logger.warn("[KafkaBatch][Scheduler] active_view compute failed: #{e.message}")
        { count: 0, sum_weight: 0.0 }
      end

      # Count of this lane's ingest-topic partitions with lag > 0.
      def ingest_active_count
        return 0 unless defined?(KafkaBatch::Lag) && KafkaBatch::Lag.available?

        group = KafkaBatch.dispatch_consumer_group(@type)
        topic = KafkaBatch.config.fairness_ingest_topic(@type)
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

      # ── Weight helpers (per-lane Redis WEIGHT hash) ────────────────────────

      def fetch_all_weights_from_backend
        with { |r| r.hgetall(@weight) }.transform_values(&:to_f)
      rescue => e
        KafkaBatch.logger.warn("[KafkaBatch][Scheduler] fetch_all_weights failed: #{e.message}")
        {}
      end

      def write_weight_to_backend(tenant_id, weight)
        with { |r| r.hset(@weight, tenant_id, weight) }
      rescue => e
        KafkaBatch.logger.error("[KafkaBatch][Scheduler] write_weight failed for #{tenant_id}: #{e.message}")
        raise
      end

      def remove_weight_from_backend(tenant_id)
        with { |r| r.hdel(@weight, tenant_id) }
      rescue => e
        KafkaBatch.logger.error("[KafkaBatch][Scheduler] remove_weight failed for #{tenant_id}: #{e.message}")
        raise
      end

      def with(&block)
        @pool.with(&block)
      end
    end
  end
end
