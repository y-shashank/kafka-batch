# Tenant-guard integration tests against a REAL Redis.
#
# These prove the load-bearing invariants of the per-tenant guard end-to-end
# through actual Redis (not mocks): the shared control keys, the reuse of the
# existing partition-pause + weight levers, exact weight restore, auto-release,
# and — most importantly — that a paused/throttled tenant does NOT perturb batch
# counting or completion.
#
# Opt in with KAFKA_BATCH_INTEGRATION=1. Needs only Redis (no broker): static
# tenant→partition mapping resolves without a Kafka admin call.
require "securerandom"

RSpec.describe "Tenant guard (integration, real Redis)", :integration do
  TID = "acme-guard"

  def opted_in?
    ENV["KAFKA_BATCH_INTEGRATION"] == "1"
  end

  def group = KafkaBatch.dispatch_consumer_group(:time)
  def topic = KafkaBatch.config.fairness_ingest_topic(:time)

  def partition_paused?
    snap = KafkaBatch::ConsumptionControl.snapshot(refresh: true)
    KafkaBatch::ConsumptionControl.partition_paused?(snap, group, topic, 0)
  end

  before(:each) do
    skip "set KAFKA_BATCH_INTEGRATION=1 to run" unless opted_in?
    skip "no Redis at #{KafkaBatchSpec::RedisHelper::TEST_URL}" unless KafkaBatchSpec::RedisHelper.available?

    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.logger    = Logger.new(File::NULL)
      c.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      # Static ingest partition so pause! resolves without a Kafka admin call.
      c.fairness_tenant_partitions = { TID => 0 }
    end
    KafkaBatchSpec::RedisHelper.flush!
    # spec_helper stubs ConsumptionControl.available? => false globally; this is
    # a REAL-Redis integration test, so restore the real method.
    allow(KafkaBatch::ConsumptionControl).to receive(:available?).and_call_original
    KafkaBatch::TenantGuard.reset!
    KafkaBatch::TenantGuard.update_settings(
      "enabled" => true, "mitigation" => "throttle", "throttle_weight" => 0.1,
      "error_rate_pct" => 25.0, "window_seconds" => 300, "min_samples" => 5
    )
  end

  after(:each) do
    KafkaBatch::TenantGuard.reset! rescue nil
    KafkaBatch.reset! rescue nil
  end

  it "keeps batch counting + completion correct across a mid-flight throttle→pause" do
    store = KafkaBatch.store
    n = 20
    store.create_batch(id: "b1", total_jobs: n, on_success: "Cb.on_success",
                       on_complete: "Cb.on_complete", tenant_id: TID)

    def complete(store, seq)
      store.record_completion_by_offset(
        batch_id: "b1", job_id: "j#{seq}", source_topic: "t",
        source_partition: 0, source_offset: seq, status: "success", batch_seq: seq
      )
    end

    outcomes = []

    # Phase 1: THROTTLE the tenant, count the first half under a weight override.
    KafkaBatch::TenantGuard.throttle!(TID, lane: :time)
    expect(KafkaBatch.scheduler(:time).weight_override(TID)).to eq(0.1)
    outcomes += (1..10).map { |seq| complete(store, seq) }

    # Phase 2: escalate to PAUSE (supersedes the throttle → weight restored to
    # default), count the second half under a paused ingest partition.
    KafkaBatch::TenantGuard.pause!(TID, lane: :time)
    expect(partition_paused?).to be(true)
    expect(KafkaBatch.scheduler(:time).weight_override(TID)).to be_nil # throttle superseded
    outcomes += (11..20).map { |seq| complete(store, seq) }

    b = store.find_batch("b1")
    expect(b[:completed_count]).to eq(n)   # counters exact despite throttle+pause
    expect(b[:failed_count]).to eq(0)      # neither lever inflated failures/DLT
    expect(b[:touched_count]).to eq(n)

    # on_success/on_complete fire EXACTLY once: exactly one terminal :done.
    dones = outcomes.select { |o| o[:status] == :done }
    expect(dones.size).to eq(1)
    expect(dones.first[:outcome]).to eq("success")

    # The pause/throttle themselves recorded no tenant errors.
    expect(KafkaBatch::TenantGuard::Recorder.window_counts(TID)).to eq(ok: 0, fail: 0, retry: 0)
  end

  it "idempotent re-count: redelivered completion events do not double-count under a control" do
    store = KafkaBatch.store
    store.create_batch(id: "b2", total_jobs: 3, on_success: "Cb.s", on_complete: "Cb.c", tenant_id: TID)
    KafkaBatch::TenantGuard.pause!(TID, lane: :time)

    3.times do |i|
      seq = i + 1
      2.times do # deliver each completion twice (same offset ⇒ dedup)
        store.record_completion_by_offset(
          batch_id: "b2", job_id: "j#{seq}", source_topic: "t",
          source_partition: 0, source_offset: seq, status: "success", batch_seq: seq
        )
      end
    end
    b = store.find_batch("b2")
    expect(b[:completed_count]).to eq(3)   # not 6
  end

  it "reset restores the weight (to default) and resumes the partition" do
    KafkaBatch::TenantGuard.throttle!(TID, lane: :time) # no prior override
    KafkaBatch::TenantGuard.pause!(TID, lane: :time)
    expect(partition_paused?).to be(true)

    KafkaBatch::TenantGuard.release!(TID)

    expect(KafkaBatch.scheduler(:time).weight_override(TID)).to be_nil # back to default
    expect(partition_paused?).to be(false)
    expect(KafkaBatch::TenantGuard.status(TID)).to be_nil
  end

  it "restores a pre-existing weight override exactly on reset" do
    KafkaBatch.scheduler(:time).set_weight(TID, 2.0) # operator baseline
    KafkaBatch::TenantGuard.throttle!(TID, lane: :time, weight: 0.05)
    expect(KafkaBatch.scheduler(:time).weight_override(TID)).to eq(0.05)
    KafkaBatch::TenantGuard.release!(TID)
    expect(KafkaBatch.scheduler(:time).weight_override(TID)).to eq(2.0) # exact restore
  end

  it "auto-releases an expired control on the reconciler tick" do
    KafkaBatch::TenantGuard.throttle!(TID, lane: :time, until_ts: Time.now.to_i - 5)
    expect(KafkaBatch.scheduler(:time).weight_override(TID)).to eq(0.1)

    summary = KafkaBatch::TenantGuard.reconcile_once!
    expect(summary[:expired]).to eq(1)
    expect(KafkaBatch.scheduler(:time).weight_override(TID)).to be_nil
    expect(KafkaBatch::TenantGuard.status(TID)).to be_nil
  end

  it "reconciler releases externally-resumed drift without re-pausing" do
    KafkaBatch::TenantGuard.pause!(TID, lane: :time)
    # Operator resumes the partition directly (existing /lag button).
    KafkaBatch::ConsumptionControl.resume_partition(group: group, topic: topic, partition: 0)

    summary = KafkaBatch::TenantGuard.reconcile_once!
    expect(summary[:drift_released]).to eq(1)
    expect(partition_paused?).to be(false)     # not re-paused
    expect(KafkaBatch::TenantGuard.status(TID)).to be_nil
  end

  it "auto-mitigates a breaching tenant end-to-end (recorder → mitigation → throttle)" do
    5.times  { KafkaBatch::TenantGuard::Recorder.record_ok(TID) }
    15.times { KafkaBatch::TenantGuard::Recorder.record_fail(TID) } # 75% over 20 samples

    summary = KafkaBatch::TenantGuard.mitigate_once!
    expect(summary[:acted]).to eq(1)

    st = KafkaBatch::TenantGuard.status(TID)
    expect(st["state"]).to eq("throttled")
    expect(st["source"]).to eq("error_rate_guard")
    expect(KafkaBatch.scheduler(:time).weight_override(TID)).to eq(0.1)
  end
end
