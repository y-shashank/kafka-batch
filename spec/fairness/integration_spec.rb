require "oj"

# End-to-end fairness: drives the REAL default path against a REAL Redis —
#   ingest → Fairness::Dispatcher (enqueue into the WFQ window)
#          → Fairness::Forwarder  (checkout the fairest job → ready topic)
#          → JobConsumer          (perform → Scheduler#complete)
#
# These assert the pieces actually fit together: the fair-slot marker set by the
# Forwarder is honoured by the JobConsumer, in-flight slots are released so the
# budget recovers, weighting flows through, and there are no slot leaks.
#
# NOTE: the process-wide scheduler is built lazily on first use, capturing config
# at that moment — so each test sets any config overrides BEFORE touching the
# pipeline (no eager `let!`).
RSpec.describe "Fairness end-to-end (Dispatcher → Forwarder → JobConsumer)", :fairness_integration do
  before(:each) do
    skip "Redis unavailable at #{KafkaBatchSpec::RedisHelper::TEST_URL}" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.store     = :redis
    KafkaBatchSpec::RedisHelper.flush!

    KafkaBatch.config.fair_time_ingest_topic           = "test.time.ingest"
    KafkaBatch.config.fair_time_ready_topic            = "test.time.ready"
    KafkaBatch.config.fair_time_ready_go_topic         = ""
    KafkaBatch.config.fair_time_ready_ruby_topic       = ""
    KafkaBatch.config.fair_throughput_ingest_topic     = "test.tp.ingest"
    KafkaBatch.config.fair_throughput_ready_topic      = "test.tp.ready"
    KafkaBatch.config.fair_throughput_ready_go_topic   = ""
    KafkaBatch.config.fair_throughput_ready_ruby_topic = ""
    KafkaBatch.config.fairness_global_concurrency      = 4
    KafkaBatch.config.fairness_max_inflight_per_tenant = 0    # rely on the dynamic fair share
    KafkaBatch.config.fairness_ready_window            = 100
    KafkaBatch.config.fairness_default_weight          = 1.0

    # Never spin up the real background thread — we pump the forwarder by hand
    # so the pipeline is deterministic.
    allow(KafkaBatch::Fairness::Forwarder).to receive(:ensure_running!)
    KafkaBatchSpec::WorkerRuns.reset!
  end

  # The fairness lane under test (:time by default; the throughput example sets
  # @fair_type = :throughput before driving the pipeline).
  def fair_type
    @fair_type || :time
  end

  # Lazily-built per-lane singleton (captures whatever config the test set first).
  def scheduler
    KafkaBatch.scheduler(fair_type)
  end

  # ── Pipeline drivers ───────────────────────────────────────────────────────

  def ingest_message(tenant:, job_id:, offset:)
    FakeMessage.new(
      topic:   KafkaBatch.config.fairness_ingest_topic(fair_type),
      offset:  offset,
      payload: {
        "job_id"       => job_id,
        "batch_id"     => nil,
        "worker_class" => "FairWorker",
        "payload"      => { "job_id" => job_id, "tenant" => tenant },
        "attempt"      => 0,
        "tenant_id"    => tenant
      }
    )
  end

  # Push a set of ingest messages through the Dispatcher (ingest → Redis window).
  def dispatch(messages)
    d = build_consumer(KafkaBatch::Fairness::Dispatcher)
    allow(d).to receive(:messages).and_return(messages)
    d.consume
    d
  end

  # Forward every currently-checkoutable job (until the budget is full or the
  # ring is empty), returning how many were forwarded to the ready topic.
  def forward_all(limit = 1000)
    fwd = KafkaBatch::Fairness::Forwarder.new(fair_type)
    n = 0
    n += 1 while n < limit && fwd.forward_once
    n
  end

  # Run a single ready-topic message through the JobConsumer (perform + complete).
  def run_ready(produced)
    jc = build_consumer(KafkaBatch::Consumers::JobConsumer)
    fm = FakeMessage.new(topic: KafkaBatch.config.fairness_ready_topic(fair_type), payload: produced.payload, offset: 0)
    jc.send(:process_message, fm)
  end

  def ready_messages
    FakeProducer.for_topic(KafkaBatch.config.fairness_ready_topic(fair_type))
  end

  def tenant_of(produced)
    Oj.load(produced.payload)["tenant_id"]
  end

  # Fully drain the pipeline, executing each forwarded job exactly once. Returns
  # the ordered list of executed tenant_ids.
  def drain_pipeline
    executed = []
    idx = 0
    loop do
      forward_all
      batch = ready_messages[idx..] || []
      break if batch.empty?
      idx += batch.size
      batch.each do |m|
        run_ready(m)
        executed << tenant_of(m)
      end
    end
    executed
  end

  # Execute the first +n+ jobs the pipeline chooses (a prefix sample — used to
  # observe weighting, which affects order/rate, not the total count of a full
  # drain). Requires a small budget so selection is (near) sequential.
  def sample(n)
    out = []
    idx = 0
    while out.size < n
      forward_all
      batch = ready_messages[idx..] || []
      break if batch.empty?
      idx += batch.size
      batch.each do |m|
        out << tenant_of(m)
        run_ready(m)
        break if out.size >= n
      end
    end
    out
  end

  # ── Tests ────────────────────────────────────────────────────────────────

  it "carries a job through ingest → ready → perform, releasing the in-flight slot" do
    dispatch([ingest_message(tenant: "acme", job_id: "j1", offset: 1)])

    expect(scheduler.ready_depth("acme")).to eq(1)   # enqueued into the WFQ window

    expect(forward_all).to eq(1)                      # forwarded to ready
    forwarded = ready_messages.first
    decoded   = Oj.load(forwarded.payload)
    expect(decoded["_fair_slot"]).to be(true)
    expect(decoded["tenant_id"]).to eq("acme")
    expect(scheduler.stats[:inflight_total]).to eq(1) # holds one slot

    run_ready(forwarded)                              # perform + Scheduler#complete
    fair_runs = KafkaBatchSpec::WorkerRuns.runs.count { |r| r[:name] == :fair }
    expect(fair_runs).to eq(1)
    expect(scheduler.stats[:inflight_total]).to eq(0) # slot released — no leak
  end

  it "reclaims a stale forwarding entry after lease expiry (crash between checkout and produce)" do
    KafkaBatch.config.fairness_forwarding_recovery_grace = 0
    dispatch([ingest_message(tenant: "acme", job_id: "j-stale", offset: 1)])
    expect(scheduler.ready_depth("acme")).to eq(1)

    job = scheduler.checkout
    expect(job[:payload]).to include("j-stale")
    expect(scheduler.stats[:forwarding_depth]).to eq(1)
    expect(FakeProducer.for_topic(KafkaBatch.config.fairness_ready_topic(:time))).to be_empty

    redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    redis.zadd(scheduler.leases, 0, job[:slot_id])

    fwd = KafkaBatch::Fairness::Forwarder.new(:time)
    fwd.send(:reclaim_stale_forward!, scheduler, {
      slot_id: job[:slot_id], tenant_id: "acme", payload: job[:payload]
    })

    expect(scheduler.stats[:forwarding_depth]).to eq(0)
    expect(FakeProducer.for_topic(KafkaBatch.config.fairness_ready_topic(:time)).size).to eq(1)
    expect(scheduler.stats[:inflight_total]).to eq(1)

    run_ready(ready_messages.last)
    expect(scheduler.stats[:inflight_total]).to eq(0)
  end

  it "interleaves two equally-weighted tenants fairly and leaks no slots" do
    6.times { |i| dispatch([ingest_message(tenant: "A", job_id: "a#{i}", offset: i)]) }
    6.times { |i| dispatch([ingest_message(tenant: "B", job_id: "b#{i}", offset: 100 + i)]) }

    # First forwarding round is bounded by the global window (4) and the dynamic
    # per-tenant share ceil(4/2)=2 → exactly 2 from each tenant before any run.
    forward_all
    first_round = ready_messages.map { |m| tenant_of(m) }.tally
    expect(first_round["A"]).to eq(2)
    expect(first_round["B"]).to eq(2)

    executed = drain_pipeline                          # processes every job exactly once

    counts = executed.tally
    expect(counts["A"]).to eq(6)
    expect(counts["B"]).to eq(6)
    # Fairly interleaved, not "all A then all B": neither tenant runs more than
    # its fair share ahead of the other in any window of 4.
    expect(executed.first(4).tally.values).to all(be <= 2)
    expect(scheduler.stats[:inflight_total]).to eq(0)  # every slot released
  end

  it "gives a higher-weight tenant proportionally more throughput end-to-end (throughput lane)" do
    @fair_type = :throughput  # drive the whole pipeline on the throughput lane
    KafkaBatch.config.fairness_global_concurrency = 1   # near-sequential → weight drives selection order
    scheduler.set_weight("A", 1.0)
    scheduler.set_weight("B", 2.0)

    20.times { |i| dispatch([ingest_message(tenant: "A", job_id: "a#{i}", offset: i)]) }
    20.times { |i| dispatch([ingest_message(tenant: "B", job_id: "b#{i}", offset: 100 + i)]) }

    # Sample the first 15 dispatches (fewer than either queue) so both stay live.
    executed = sample(15)
    counts   = executed.tally

    expect(counts["B"]).to be > counts["A"]             # weight 2 wins more slots
    expect(counts["B"].to_f / counts["A"]).to be_within(0.8).of(2.0)
  end

  it "does not advance virtual time at checkout in :time_fairness (only at completion)" do
    dispatch([ingest_message(tenant: "acme", job_id: "j1", offset: 1)])
    forward_all
    # Checked out but not completed → vtime must still be 0 (time mode).
    vt_before = scheduler.all_tenants.find { |t| t[:tenant_id] == "acme" }[:vtime]
    expect(vt_before).to eq(0.0)

    run_ready(ready_messages.first)                     # completes → vtime += duration/weight
    vt_after = scheduler.all_tenants.find { |t| t[:tenant_id] == "acme" }[:vtime]
    expect(vt_after).to be >= vt_before
    expect(scheduler.stats[:inflight_total]).to eq(0)
  end

  it "applies backpressure through the Dispatcher when the WFQ window is full" do
    KafkaBatch.config.fairness_ready_window = 2

    d = dispatch([
      ingest_message(tenant: "acme", job_id: "a0", offset: 1),
      ingest_message(tenant: "acme", job_id: "a1", offset: 2),
      ingest_message(tenant: "acme", job_id: "a2", offset: 3)
    ])

    expect(scheduler.ready_depth("acme")).to eq(2)                       # window capped
    expect(d).to have_received(:pause).with(3, kind_of(Integer))         # paused at the full message
    expect(d).to have_received(:mark_as_consumed!).with(have_attributes(offset: 2))  # progress committed
  end
end
