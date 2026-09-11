# Proves the Redis-driven ON/OFF fix: a control plane that BOOTS WITH THE GUARD
# DISABLED must still pick up an enable made later through the Redis settings key
# (i.e. by the /tenant_guard page), with no redeploy and no restart.
RSpec.describe "Tenant guard runtime toggle via Redis (integration)", :integration do
  TTID = "acme-toggle"

  def opted_in? = ENV["KAFKA_BATCH_INTEGRATION"] == "1"

  before(:each) do
    skip "set KAFKA_BATCH_INTEGRATION=1 to run" unless opted_in?
    skip "no Redis" unless KafkaBatchSpec::RedisHelper.available?

    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.logger    = Logger.new(File::NULL)
      c.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      c.fairness_tenant_partitions = { TTID => 0 }
      # BOOT STATE: guard OFF. This is the case that used to strand the toggle.
      c.tenant_guard_enabled = false
    end
    KafkaBatchSpec::RedisHelper.flush!
    allow(KafkaBatch::ConsumptionControl).to receive(:available?).and_call_original
    KafkaBatch::TenantGuard.reset!
  end

  after(:each) do
    KafkaBatch::TenantGuard.reset! rescue nil
    KafkaBatch.reset! rescue nil
  end

  # Drive enough failures for `tid` to breach the threshold.
  def breach!(tid, fails: 10)
    fails.times { KafkaBatch::TenantGuard::Recorder.record_fail(tid) }
  end

  it "starts the control loop even though the guard booted disabled" do
    expect(KafkaBatch::TenantGuard.enabled?).to be(false)
    expect(KafkaBatch::TenantGuard.should_run_reconciler?).to be(true),
      "loop must be eligible to start while disabled — that is the whole fix"

    # The regression itself: the THREAD must exist while the guard is off, so a
    # later Redis flip has something running to observe it.
    thread = KafkaBatch::TenantGuard.start_reconciler!
    expect(thread).to be_a(Thread)
    expect(thread.alive?).to be(true)
    # Idempotent — a second call must not spawn a rival loop.
    expect(KafkaBatch::TenantGuard.start_reconciler!).to equal(thread)
  end

  it "does not act while the Redis flag is off" do
    breach!(TTID)
    KafkaBatch::TenantGuard.mitigate_once!
    expect(KafkaBatch.scheduler(:time).weight_override(TTID)).to be_nil
  end

  it "acts as soon as the Redis flag flips on — no restart" do
    breach!(TTID)

    # This is exactly what PUT /api/tenant_guard/settings does from the page.
    KafkaBatch::TenantGuard.update_settings(
      "enabled" => true, "mitigation" => "throttle", "throttle_weight" => 0.1,
      "error_rate_pct" => 25.0, "window_seconds" => 300, "min_samples" => 5
    )

    expect(KafkaBatch::TenantGuard.enabled?).to be(true)
    summary = KafkaBatch::TenantGuard.mitigate_once!
    expect(summary[:acted]).to eq(1)
    expect(KafkaBatch.scheduler(:time).weight_override(TTID)).to eq(0.1)
  end

  it "stops acting when the flag flips back off, and still auto-releases" do
    KafkaBatch::TenantGuard.update_settings(
      "enabled" => true, "mitigation" => "throttle", "throttle_weight" => 0.1,
      "error_rate_pct" => 25.0, "window_seconds" => 300, "min_samples" => 5,
      "auto_release_seconds" => 1
    )
    breach!(TTID)
    KafkaBatch::TenantGuard.mitigate_once!
    expect(KafkaBatch.scheduler(:time).weight_override(TTID)).to eq(0.1)

    # Operator disables the guard on the page while a control is still engaged.
    KafkaBatch::TenantGuard.update_settings("enabled" => false)
    expect(KafkaBatch::TenantGuard.enabled?).to be(false)

    # Reconciliation must STILL run so the live control auto-releases rather
    # than being stranded by the disable.
    KafkaBatch::TenantGuard.reconcile_once!(at: Time.now + 5)
    expect(KafkaBatch.scheduler(:time).weight_override(TTID)).to be_nil
  end
end
