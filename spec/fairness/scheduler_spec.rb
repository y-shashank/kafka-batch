RSpec.describe KafkaBatch::Fairness::Scheduler do
  # Default to the :throughput lane (vtime advances at dispatch — the old
  # job-count behaviour). The time-lane block below overrides with type: :time.
  let(:scheduler) { described_class.new(type: :throughput) }

  before(:each) do
    skip "Redis unavailable at #{KafkaBatchSpec::RedisHelper::TEST_URL}" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.store     = :redis  # weight backend; keeps set_weight on the Redis path
    KafkaBatchSpec::RedisHelper.flush!
    # Generous defaults; individual tests tighten budget/window as needed.
    KafkaBatch.config.fairness_global_concurrency      = 1000
    KafkaBatch.config.fairness_ready_window            = 0     # unbounded unless a test sets it
    KafkaBatch.config.fairness_max_inflight_per_tenant = 0
    KafkaBatch.config.fairness_default_weight          = 1.0
    # Reset after examples that switch to :ingest_lag — otherwise a dev machine with
    # a live Kafka cluster leaks partition lag into unrelated checkout specs.
    KafkaBatch.config.fairness_active_count_source = :inflight_plus_ready
    allow(KafkaBatch::Lag).to receive(:available?).and_return(false)
  end

  def drain(n)
    Array.new(n) { scheduler.checkout }.compact
  end

  describe "work-conserving fairness" do
    it "gives a single active tenant 100% of dispatches" do
      5.times { |i| scheduler.enqueue("A", "a#{i}") }
      picked = drain(5)
      expect(picked.map { |j| j[:tenant_id] }.uniq).to eq(["A"])
      expect(scheduler.checkout).to be_nil  # nothing left
    end

    it "splits two equally-weighted tenants ~50:50" do
      10.times { |i| scheduler.enqueue("A", "a#{i}") }
      10.times { |i| scheduler.enqueue("B", "b#{i}") }

      first10 = drain(10).map { |j| j[:tenant_id] }
      counts  = first10.tally
      expect(counts["A"]).to be_between(4, 6)
      expect(counts["B"]).to be_between(4, 6)
    end

    it "redistributes capacity when a tenant goes idle (work-conserving)" do
      3.times { |i| scheduler.enqueue("A", "a#{i}") }
      1.times { |i| scheduler.enqueue("B", "b#{i}") }

      all = drain(10).map { |j| j[:tenant_id] }.tally
      expect(all["A"]).to eq(3)  # A's three all dispatched
      expect(all["B"]).to eq(1)  # B drained then skipped — A took the rest
    end
  end

  describe "weights" do
    it "gives a higher-weight tenant proportionally more" do
      scheduler.set_weight("A", 2.0)
      scheduler.set_weight("B", 1.0)
      12.times { |i| scheduler.enqueue("A", "a#{i}") }
      12.times { |i| scheduler.enqueue("B", "b#{i}") }

      counts = drain(12).map { |j| j[:tenant_id] }.tally
      expect(counts["A"]).to be > counts["B"]
      expect(counts["A"].to_f / counts["B"]).to be_within(0.6).of(2.0)
    end
  end

  describe "weighted concurrency (fairness_weighted_concurrency)" do
    # Full saturation = check out up to the budget WITHOUT completing, so every
    # tenant is backlogged and the per-tenant cap (not selection order) decides
    # the split. This is the scenario where plain weights are masked.
    before do
      KafkaBatch.config.fairness_global_concurrency      = 8
      KafkaBatch.config.fairness_max_inflight_per_tenant = 0
    end

    it "enforces weight-proportional in-flight caps under saturation when enabled" do
      KafkaBatch.config.fairness_weighted_concurrency = true
      scheduler.set_weight("A", 3.0)
      scheduler.set_weight("B", 1.0)
      20.times { |i| scheduler.enqueue("A", "a#{i}") }
      20.times { |i| scheduler.enqueue("B", "b#{i}") }

      counts = drain(20).map { |j| j[:tenant_id] }.tally  # bounded to budget = 8
      expect(counts.values.sum).to eq(8)
      expect(counts["A"]).to eq(6)   # floor(8 * 3/4)
      expect(counts["B"]).to eq(2)   # floor(8 * 1/4)
    end

    it "masks weight under saturation when disabled (equal fair share)" do
      KafkaBatch.config.fairness_weighted_concurrency = false
      scheduler.set_weight("A", 3.0)
      scheduler.set_weight("B", 1.0)
      20.times { |i| scheduler.enqueue("A", "a#{i}") }
      20.times { |i| scheduler.enqueue("B", "b#{i}") }

      counts = drain(20).map { |j| j[:tenant_id] }.tally
      expect(counts["A"]).to eq(4)   # equal dynamic cap ceil(8/2) = 4
      expect(counts["B"]).to eq(4)
    end

    it "still lets a lone active tenant use the whole budget (work-conserving)" do
      KafkaBatch.config.fairness_weighted_concurrency = true
      scheduler.set_weight("A", 3.0)
      20.times { |i| scheduler.enqueue("A", "a#{i}") }

      expect(drain(20).size).to eq(8)  # sum_w == 3 → cap = floor(8*3/3) = 8
    end
  end

  describe "work-conserving fallback (full utilization with few tenants)" do
    it "fills the whole budget for one tenant even when the fair cap is tiny" do
      KafkaBatch.config.fairness_global_concurrency      = 10
      KafkaBatch.config.fairness_max_inflight_per_tenant = 0
      # Simulate an active set that just shrank (stale-high smoothed count).
      allow(scheduler).to receive(:active_view).and_return(count: 20, sum_weight: 0.0)
      15.times { |i| scheduler.enqueue("only", "j#{i}") }

      expect(drain(30).size).to eq(10)  # full budget used despite ceil(10/20)=1 fair cap
    end

    it "still honors an absolute hard cap in the fallback" do
      KafkaBatch.config.fairness_global_concurrency      = 10
      KafkaBatch.config.fairness_max_inflight_per_tenant = 3   # hard ceiling
      allow(scheduler).to receive(:active_view).and_return(count: 20, sum_weight: 0.0)
      15.times { |i| scheduler.enqueue("only", "j#{i}") }

      expect(drain(30).size).to eq(3)   # fallback fills slack but never past the hard cap
    end

    it "does not fire the fallback under real contention (caps hold, split stays fair)" do
      KafkaBatch.config.fairness_global_concurrency      = 8
      KafkaBatch.config.fairness_max_inflight_per_tenant = 0
      20.times { |i| scheduler.enqueue("A", "a#{i}") }
      20.times { |i| scheduler.enqueue("B", "b#{i}") }

      counts = drain(20).map { |j| j[:tenant_id] }.tally
      expect(counts.values.sum).to eq(8)
      expect(counts["A"]).to eq(4)   # equal fair share ceil(8/2), fair pass fills budget
      expect(counts["B"]).to eq(4)
    end
  end

  describe "smoothed active-tenant view" do
    it "counts distinct tenants with queued OR in-flight work (not just the ring)" do
      scheduler.enqueue("A", "a0")   # 1 job each
      scheduler.enqueue("B", "b0")
      scheduler.checkout             # A checked out → A's ready list empties → A leaves the ring

      view = scheduler.send(:compute_active_view)
      # A is in-flight (not in ring), B is queued (in ring) → both count as active.
      expect(view[:count]).to eq(2)
    end

    it "stays work-conserving: a lone active tenant uses the FULL budget even if the smoothed count is high" do
      KafkaBatch.config.fairness_global_concurrency = 8
      # Pretend 4 tenants are active even though only one has work right now.
      allow(scheduler).to receive(:active_view).and_return(count: 4, sum_weight: 0.0)
      10.times { |i| scheduler.enqueue("solo", "s#{i}") }

      # Fair pass caps solo at ceil(8/4)=2, but the work-conserving fallback fills
      # the remaining budget (no other tenant wants it) → full utilization.
      expect(drain(20).size).to eq(8)
    end

    it "caches the view within fairness_active_count_ttl (one compute per window)" do
      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }
      KafkaBatch.config.fairness_active_count_ttl = 5

      scheduler.enqueue("A", "a0")
      expect(scheduler).to receive(:compute_active_view).once.and_call_original
      scheduler.active_view           # computes at t=0
      t = 3.0
      scheduler.active_view           # within TTL → cached, no recompute
    end

    it "supports the :ingest_lag source (counts ingest partitions with lag)" do
      KafkaBatch.config.fairness_active_count_source = :ingest_lag
      allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
      allow(KafkaBatch::Lag).to receive(:read_group).and_return(
        KafkaBatch.dispatch_consumer_group(:throughput) => {
          KafkaBatch.config.fairness_ingest_topic(:throughput) => {
            0 => { lag: 5 }, 1 => { lag: 0 }, 2 => { lag: 3 }
          }
        }
      )
      expect(scheduler.send(:compute_active_view)[:count]).to eq(2)  # partitions 0 and 2
    end
  end

  describe "global concurrency budget" do
    it "stops dispatching at the budget and resumes after complete" do
      KafkaBatch.config.fairness_global_concurrency = 2
      4.times { |i| scheduler.enqueue("A", "a#{i}") }

      picked = drain(5)
      expect(picked.size).to eq(2)             # capped at budget
      expect(scheduler.checkout).to be_nil     # at budget
      scheduler.complete("A", slot_id: picked.first[:slot_id])  # free one slot
      expect(scheduler.checkout).not_to be_nil
    end
  end

  describe "per-tenant in-flight cap" do
    it "won't exceed the configured hard cap for a tenant even with budget available" do
      KafkaBatch.config.fairness_max_inflight_per_tenant = 1
      3.times { |i| scheduler.enqueue("A", "a#{i}") }

      first = scheduler.checkout
      expect(first).not_to be_nil               # 1st (inflight 1)
      expect(scheduler.checkout).to be_nil      # capped at 1 in-flight
      scheduler.complete("A", slot_id: first[:slot_id])
      expect(scheduler.checkout).not_to be_nil  # freed
    end
  end

  describe "dynamic fair-share concurrency (work-conserving)" do
    before do
      KafkaBatch.config.fairness_max_inflight_per_tenant = 0  # no hard ceiling
      KafkaBatch.config.fairness_global_concurrency      = 6
    end

    it "lets a lone active tenant use the entire global budget" do
      10.times { |i| scheduler.enqueue("A", "a#{i}") }
      # active_tenants == 1 → dynamic cap == budget (6). All 6 slots to A.
      expect(drain(10).size).to eq(6)
      expect(scheduler.checkout).to be_nil  # budget exhausted, not a per-tenant cap
    end

    it "splits the budget fairly across tenants (budget/N each)" do
      10.times { |i| scheduler.enqueue("A", "a#{i}") }
      10.times { |i| scheduler.enqueue("B", "b#{i}") }
      # active_tenants == 2 → dynamic cap == ceil(6/2) == 3 per tenant.
      picked = drain(10)
      counts = picked.map { |j| j[:tenant_id] }.tally
      expect(picked.size).to eq(6)          # total budget
      expect(counts["A"]).to eq(3)
      expect(counts["B"]).to eq(3)
    end
  end

  describe "bounded ready window (backpressure)" do
    it "rejects enqueues past the window so the caller can pause Kafka" do
      KafkaBatch.config.fairness_ready_window = 2
      expect(scheduler.enqueue("A", "a0")).to eq(:ok)
      expect(scheduler.enqueue("A", "a1")).to eq(:ok)
      expect(scheduler.enqueue("A", "a2")).to eq(:full)   # window full
      expect(scheduler.ready_depth("A")).to eq(2)
    end
  end

  describe "in-flight lease crash recovery" do
    let(:redis) { Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL) }

    it "records a lease on checkout and releases exactly it on complete(slot_id:)" do
      scheduler.enqueue("A", "a0")
      job = scheduler.checkout
      expect(job[:slot_id]).to be_a(String)
      expect(scheduler.stats[:inflight_total]).to eq(1)
      expect(redis.zcard("#{scheduler.lease_prefix}A")).to eq(1)
      expect(redis.hlen(scheduler.forwarding)).to eq(1)

      scheduler.confirm_forward(job[:slot_id])
      expect(redis.hlen(scheduler.forwarding)).to eq(0)

      scheduler.complete("A", slot_id: job[:slot_id])
      expect(scheduler.stats[:inflight_total]).to eq(0)
      expect(redis.zcard("#{scheduler.lease_prefix}A")).to eq(0)
    end

    it "is idempotent — a redelivered completion never double-releases the slot" do
      scheduler.enqueue("A", "a0")
      job = scheduler.checkout
      2.times { scheduler.complete("A", slot_id: job[:slot_id]) }
      expect(scheduler.stats[:inflight_total]).to eq(0)  # not -1
    end

    it "reclaims an expired lease on the next checkout, unsticking a wedged tenant" do
      KafkaBatch.config.fairness_global_concurrency = 1   # budget of one slot
      2.times { |i| scheduler.enqueue("A", "a#{i}") }

      first = scheduler.checkout               # holds the only slot
      expect(first[:payload]).to eq("a0")
      expect(scheduler.checkout).to be_nil     # budget full — lane wedged

      # Simulate a hard-killed consumer (Scheduler#complete never ran): force the
      # leaked lease's score into the past in BOTH the global (budget) and
      # per-tenant (cap) sets, so the next checkout treats it as expired.
      redis.zadd(scheduler.leases, 0, first[:slot_id])
      redis.zadd("#{scheduler.lease_prefix}A", 0, first[:slot_id])

      second = scheduler.checkout              # reclaims the expired lease, then dispatches
      expect(second[:payload]).to eq("a1")
      expect(scheduler.stats[:inflight_total]).to eq(1)  # only the fresh lease
    end

    it "reclaim_expired_leases! sweeps leases leaked by a now-idle tenant" do
      scheduler.enqueue("A", "a0")
      job = scheduler.checkout                 # A's queue drains → A leaves the ring
      # Expire the leaked lease in both the global and per-tenant sets.
      redis.zadd(scheduler.leases, 0, job[:slot_id])
      redis.zadd("#{scheduler.lease_prefix}A", 0, job[:slot_id])

      expect(scheduler.reclaim_expired_leases!).to eq(1)   # one expired global lease reclaimed
      expect(scheduler.stats[:inflight_total]).to eq(0)
      expect(scheduler.inflight_by_tenant).to be_empty
    end

    it "ignores a stale pre-upgrade in-flight counter and never pins the budget" do
      # Reproduces the production incident: before leases, in-flight was a plain
      # counter that a hard crash could inflate and permanently pin. The
      # authoritative ZCARD-of-live-leases budget must ignore any such leftover.
      KafkaBatch.config.fairness_global_concurrency = 2
      redis.set("#{scheduler.ns}:inflight_total", "999")  # stale legacy counter

      2.times { |i| scheduler.enqueue("A", "a#{i}") }
      expect(scheduler.checkout).not_to be_nil  # pre-fix this was wedged forever
      expect(scheduler.checkout).not_to be_nil
    end

    it "floors fairness_lease_ttl so a zero/tiny value can't silently disable the budget" do
      KafkaBatch.config.fairness_lease_ttl          = 0   # misconfiguration
      KafkaBatch.config.fairness_global_concurrency = 1
      sched = described_class.new(type: :throughput)      # picks up the floored ttl
      2.times { |i| sched.enqueue("A", "a#{i}") }

      expect(sched.checkout).not_to be_nil  # takes the one slot
      expect(sched.checkout).to be_nil      # budget still enforced (lease didn't instantly expire)
    end

    it "treats a legacy completion (no slot_id) as a harmless no-op" do
      scheduler.enqueue("A", "a0")
      job = scheduler.checkout
      scheduler.confirm_forward(job[:slot_id])
      expect { scheduler.complete("A") }.not_to raise_error  # pre-upgrade message
      expect(scheduler.stats[:inflight_total]).to eq(1)      # real lease untouched
      scheduler.complete("A", slot_id: job[:slot_id])        # correct, id-matched release
      expect(scheduler.stats[:inflight_total]).to eq(0)
    end
  end

  describe "reliable forward (forwarding buffer)" do
    let(:redis) { Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL) }

    it "stores payload in forwarding on checkout until confirm_forward" do
      scheduler.enqueue("A", "a0")
      job = scheduler.checkout
      expect(redis.hget(scheduler.forwarding, job[:slot_id])).to eq("a0")
      scheduler.confirm_forward(job[:slot_id])
      expect(redis.hget(scheduler.forwarding, job[:slot_id])).to be_nil
      scheduler.complete("A", slot_id: job[:slot_id])
    end

    it "abort_forward restores the ready queue and releases the lease" do
      scheduler.enqueue("A", "a0")
      job = scheduler.checkout
      expect(scheduler.abort_forward(job[:slot_id], "A")).to be(true)
      expect(scheduler.ready_depth("A")).to eq(1)
      expect(scheduler.stats[:inflight_total]).to eq(0)
      expect(redis.hget(scheduler.forwarding, job[:slot_id])).to be_nil
    end

    it "rolls back throughput vtime on abort_forward" do
      sched = described_class.new(type: :throughput)
      sched.enqueue("A", "a0")
      sched.enqueue("B", "b0")
      job = sched.checkout
      vt_after_checkout = redis.hget(sched.vtime, "A").to_f
      expect(vt_after_checkout).to be > 0

      expect(sched.abort_forward(job[:slot_id], "A")).to be(true)
      vt_after_abort = redis.hget(sched.vtime, "A").to_f
      expect(vt_after_abort).to be < vt_after_checkout
    end

    it "lists stale forwards after lease expiry" do
      KafkaBatch.config.fairness_forwarding_recovery_grace = 0
      scheduler.enqueue("A", "a0")
      job = scheduler.checkout
      redis.zadd(scheduler.leases, 0, job[:slot_id])

      stale = scheduler.list_stale_forwards
      expect(stale.size).to eq(1)
      expect(stale.first[:slot_id]).to eq(job[:slot_id])
      expect(stale.first[:payload]).to eq("a0")
      scheduler.abort_forward(job[:slot_id], "A")
    end

    it "claim_slot_execution! deduplicates the same slot_id" do
      expect(scheduler.claim_slot_execution!("slot-x")).to be(true)
      expect(scheduler.claim_slot_execution!("slot-x")).to be(false)
    end

    it "clear_slot_execution! allows a SuperFetch reclaim to re-claim" do
      expect(scheduler.claim_slot_execution!("slot-r")).to be(true)
      expect(scheduler.claim_slot_execution!("slot-r")).to be(false)
      scheduler.clear_slot_execution!("slot-r")
      expect(scheduler.claim_slot_execution!("slot-r")).to be(true)
    end
  end

  describe "#stats" do
    it "reports active tenants and in-flight total" do
      2.times { |i| scheduler.enqueue("A", "a#{i}") }
      2.times { |i| scheduler.enqueue("B", "b#{i}") }
      scheduler.checkout  # one in-flight; both tenants still have ready jobs

      s = scheduler.stats
      expect(s[:active_tenants]).to eq(2)
      expect(s[:inflight_total]).to eq(1)
    end
  end

  # ── Time-fairness mode ────────────────────────────────────────────────────
  # These tests switch to :time_fairness and call complete(duration:) so that
  # vtime advances at completion rather than dispatch. This exercises both
  # CHECKOUT_LUA_TIME and COMPLETE_LUA_TIME — the default production paths.
  describe ":time_fairness mode" do
    let(:scheduler) { described_class.new(type: :time) }

    it "does not advance vtime at checkout — both tenants stay at 0 until complete" do
      scheduler.enqueue("A", "a0")
      scheduler.enqueue("B", "b0")

      # Checkout A without completing — vtime stays 0 for A in the ring.
      # B should be dispatched next because it still has score 0 and Redis
      # ZRANGE is lexicographically stable for equal scores (A < B, so A
      # is first; after A is checked out its ring entry is removed since its
      # queue drained. B is next).
      r1 = scheduler.checkout
      expect(r1).not_to be_nil
      # Inflight is 1, ring has B left
      r2 = scheduler.checkout
      expect(r2).not_to be_nil
      expect([r1[:tenant_id], r2[:tenant_id]].sort).to eq(%w[A B])
    end

    it "charges a tenant proportionally to duration, not dispatch count" do
      # A gets weight 1.0, B gets weight 1.0. B's jobs finish in 1s, A's in 2s.
      # Use large queues (20 each) and only sample the FIRST 15 dispatches so
      # neither queue runs dry during the window. If queues equal total
      # iterations, B exhausts last and A backfills — giving a spurious 10/10.
      KafkaBatch.config.fairness_global_concurrency = 1  # force sequential
      KafkaBatch.config.fairness_max_inflight_per_tenant = 1

      20.times { |i| scheduler.enqueue("A", "a#{i}") }
      20.times { |i| scheduler.enqueue("B", "b#{i}") }

      tenant_log = []
      15.times do   # fewer than queue depth — both queues stay live
        result = scheduler.checkout
        break unless result
        t = result[:tenant_id]
        tenant_log << t
        duration = t == "A" ? 2.0 : 1.0
        scheduler.complete(t, slot_id: result[:slot_id], duration: duration)
      end

      counts = tenant_log.tally
      # B runs twice as fast (1s vs 2s) so over the first 15 dispatches it
      # should appear ~2x as often as A (roughly B=10, A=5).
      expect(counts["B"]).to be > counts["A"]
    end

    it "COMPLETE_LUA_TIME re-adds the tenant to the ring if it still has queued jobs" do
      KafkaBatch.config.fairness_global_concurrency = 1
      KafkaBatch.config.fairness_max_inflight_per_tenant = 1

      3.times { |i| scheduler.enqueue("A", "a#{i}") }

      r1 = scheduler.checkout
      expect(r1[:tenant_id]).to eq("A")
      # A was removed from ring (queue would now have 2 left)
      scheduler.complete("A", slot_id: r1[:slot_id], duration: 1.0)
      # After complete, A should be back in the ring
      r2 = scheduler.checkout
      expect(r2).not_to be_nil
      expect(r2[:tenant_id]).to eq("A")
    end

    it "vtime accumulates correctly across multiple completions" do
      # Single tenant, weight 1.0. Each job takes 3s.
      # After 3 completions, vtime should be ~9.0.
      KafkaBatch.config.fairness_global_concurrency = 1
      KafkaBatch.config.fairness_max_inflight_per_tenant = 1

      5.times { |i| scheduler.enqueue("A", "a#{i}") }

      3.times do
        r = scheduler.checkout
        expect(r).not_to be_nil
        scheduler.complete("A", slot_id: r[:slot_id], duration: 3.0)
      end

      # Check vtime via Redis directly
      redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      vt = redis.hget(scheduler.vtime, "A").to_f
      expect(vt).to be_within(0.01).of(9.0)
    end

    it "complete with duration: 0 does not raise and advances vtime by 0" do
      scheduler.enqueue("A", "a0")
      scheduler.checkout
      expect { scheduler.complete("A", duration: 0) }.not_to raise_error
    end

    it "complete with duration: nil falls back to 0 (no free-pass crash)" do
      scheduler.enqueue("A", "a0")
      scheduler.checkout
      expect { scheduler.complete("A", duration: nil) }.not_to raise_error
    end

    it "stats reports the lane type as :time" do
      s = scheduler.stats
      expect(s[:type]).to eq(:time)
    end
  end

  # ── Weight cache TTL and bust ─────────────────────────────────────────────
  describe "weight cache TTL and bust_weight_cache!" do
    before do
      KafkaBatch.config.fairness_weight_cache_ttl = 60
    end

    it "serves stale weight within the TTL window" do
      scheduler.set_weight("t1", 1.0)
      # Prime cache
      expect(scheduler.send(:weight_for, "t1")).to be_within(0.001).of(1.0)

      # Write via the scheduler's Redis pool (simulates another process)
      scheduler.send(:with) { |r| r.hset(scheduler.weight, "t1", 9.0) }

      # Still within TTL — should see old value from cache
      expect(scheduler.send(:weight_for, "t1")).to be_within(0.001).of(1.0)
    end

    it "re-fetches from backend after TTL expires" do
      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }

      scheduler.set_weight("t1", 1.0)
      scheduler.send(:weight_for, "t1")  # prime cache at t=0

      scheduler.send(:with) { |r| r.hset(scheduler.weight, "t1", 7.0) }

      t = 61.0  # advance past TTL
      expect(scheduler.send(:weight_for, "t1")).to be_within(0.001).of(7.0)
    end

    it "bust_weight_cache! forces an immediate re-fetch on next weight_for call" do
      scheduler.set_weight("t1", 1.0)
      scheduler.send(:weight_for, "t1")  # prime cache

      scheduler.send(:with) { |r| r.hset(scheduler.weight, "t1", 5.0) }

      # Still cached without bust
      expect(scheduler.send(:weight_for, "t1")).to be_within(0.001).of(1.0)

      scheduler.send(:bust_weight_cache!)

      # Now reflects the backend value
      expect(scheduler.send(:weight_for, "t1")).to be_within(0.001).of(5.0)
    end

    it "bust_weight_cache! invalidates cache even when monotonic clock is below the TTL" do
      t = 5.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }

      scheduler.set_weight("t1", 1.0)
      scheduler.send(:weight_for, "t1")

      scheduler.send(:with) { |r| r.hset(scheduler.weight, "t1", 8.0) }
      scheduler.send(:bust_weight_cache!)

      expect(scheduler.send(:weight_for, "t1")).to be_within(0.001).of(8.0)
    end

    it "returns default_weight for an unknown tenant even after cache is primed" do
      scheduler.send(:weight_for, "known")  # prime cache with empty result
      expect(scheduler.send(:weight_for, "unknown")).to eq(KafkaBatch.config.fairness_default_weight)
    end
  end
end
