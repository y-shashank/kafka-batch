RSpec.describe KafkaBatch::Fairness::Scheduler do
  let(:scheduler) { described_class.new }

  before(:each) do
    skip "Redis unavailable at #{KafkaBatchSpec::RedisHelper::TEST_URL}" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatchSpec::RedisHelper.flush!
    # Generous defaults; individual tests tighten budget/window as needed.
    KafkaBatch.config.fairness_global_concurrency      = 1000
    KafkaBatch.config.fairness_ready_window            = 0     # unbounded unless a test sets it
    KafkaBatch.config.fairness_max_inflight_per_tenant = 0
    KafkaBatch.config.fairness_default_weight          = 1.0
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

  describe "global concurrency budget" do
    it "stops dispatching at the budget and resumes after complete" do
      KafkaBatch.config.fairness_global_concurrency = 2
      4.times { |i| scheduler.enqueue("A", "a#{i}") }

      expect(drain(5).size).to eq(2)          # capped at budget
      expect(scheduler.checkout).to be_nil     # at budget
      scheduler.complete("A")                  # free one slot
      expect(scheduler.checkout).not_to be_nil
    end
  end

  describe "per-tenant in-flight cap" do
    it "won't exceed the cap for a tenant even with budget available" do
      KafkaBatch.config.fairness_max_inflight_per_tenant = 1
      3.times { |i| scheduler.enqueue("A", "a#{i}") }

      expect(scheduler.checkout).not_to be_nil  # 1st (inflight 1)
      expect(scheduler.checkout).to be_nil       # capped at 1 in-flight
      scheduler.complete("A")
      expect(scheduler.checkout).not_to be_nil   # freed
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
end
