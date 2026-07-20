RSpec.describe KafkaBatch::Fairness::Forwarder do
  let(:scheduler) { instance_double(KafkaBatch::Fairness::Scheduler) }

  before do
    # Ready topics are always runtime-split; unregistered payloads default to .ruby.
    KafkaBatch.config.fair_time_ready_go_topic = "test.ready.go"
    KafkaBatch.config.fair_time_ready_ruby_topic = "test.ready.ruby"
    allow(KafkaBatch).to receive(:scheduler).and_return(scheduler)
    allow(scheduler).to receive(:confirm_forward)
    allow(scheduler).to receive(:reclaim_expired_leases!).and_return(0)
    allow(scheduler).to receive(:list_stale_forwards).and_return([])
  end

  after { described_class.reset! }

  describe "#forward_once" do
    subject(:forwarder) { described_class.new }

    it "routes go handlers to the .go ready topic" do
      raw = Oj.dump({ "job_id" => "j-go", "job_type" => "segment.export", "tenant_id" => "acme", "payload" => {} }, mode: :compat)
      definition = KafkaBatch::HandlerDefinition.new(job_type: "segment.export", runtime: :go, topic: "segment.exports")
      KafkaBatch::HandlerRegistry.register_definition(definition)
      allow(scheduler).to receive(:checkout).and_return({ tenant_id: "acme", payload: raw, slot_id: "slot-go" })

      expect(forwarder.forward_once).to be(true)
      expect(FakeProducer.for_topic("test.ready.go").size).to eq(1)
      expect(FakeProducer.for_topic("test.ready.ruby")).to be_empty
    end

    it "checks out a fair job and forwards it to the ready topic with the fair-slot marker" do
      raw = Oj.dump({ "job_id" => "j1", "tenant_id" => "acme", "payload" => {} }, mode: :compat)
      allow(scheduler).to receive(:checkout).and_return({ tenant_id: "acme", payload: raw, slot_id: "slot-1" })

      expect(forwarder.forward_once).to be(true)

      produced = FakeProducer.for_topic("test.ready.ruby")
      expect(produced.size).to eq(1)
      decoded = Oj.load(produced.first.payload)
      expect(decoded["_fair_slot"]).to be(true)
      expect(decoded["tenant_id"]).to eq("acme")
      expect(produced.first.key).to eq("j1")  # spread by job_id
    end

    it "backfills tenant_id from the checkout result when the payload lacks it" do
      raw = Oj.dump({ "job_id" => "j2", "payload" => {} }, mode: :compat)
      allow(scheduler).to receive(:checkout).and_return({ tenant_id: "globex", payload: raw, slot_id: "slot-2" })

      forwarder.forward_once

      decoded = Oj.load(FakeProducer.for_topic("test.ready.ruby").first.payload)
      expect(decoded["tenant_id"]).to eq("globex")
    end

    it "returns false and forwards nothing when checkout is empty (budget full / idle)" do
      allow(scheduler).to receive(:checkout).and_return(nil)

      expect(forwarder.forward_once).to be(false)
      expect(FakeProducer.for_topic("test.ready.ruby")).to be_empty
    end

    it "releases the lease and re-enqueues when produce to the ready topic fails" do
      raw = Oj.dump({ "job_id" => "j1", "tenant_id" => "acme", "payload" => {} }, mode: :compat)
      job = { tenant_id: "acme", payload: raw, slot_id: "slot-1" }
      allow(scheduler).to receive(:checkout).and_return(job)
      allow(scheduler).to receive(:abort_forward).and_return(true)
      allow(KafkaBatch::Producer).to receive(:produce_sync).and_raise(
        KafkaBatch::ProducerError, "broker down"
      )

      expect(forwarder.forward_once).to be(false)
      expect(FakeProducer.for_topic("test.ready.ruby")).to be_empty
      expect(scheduler).to have_received(:abort_forward).with("slot-1", "acme")
    end

    it "confirms the forward after a successful produce" do
      raw = Oj.dump({ "job_id" => "j1", "tenant_id" => "acme", "payload" => {} }, mode: :compat)
      job = { tenant_id: "acme", payload: raw, slot_id: "slot-1" }
      allow(scheduler).to receive(:checkout).and_return(job)
      allow(scheduler).to receive(:confirm_forward)

      expect(forwarder.forward_once).to be(true)
      expect(scheduler).to have_received(:confirm_forward).with("slot-1")
    end
  end

  describe "#maybe_reset_vtime_idle" do
    subject(:forwarder) { described_class.new }

    let(:idle_stats) { { active_tenants: 0, inflight_total: 0, forwarding_depth: 0 } }
    let(:busy_stats) { { active_tenants: 1, inflight_total: 0, forwarding_depth: 0 } }

    before do
      KafkaBatch.config.fairness_reset_vtime_when_idle    = true
      KafkaBatch.config.fairness_vtime_idle_reset_debounce = 15.0
    end

    it "resets vtime after the lane stays idle for the debounce window" do
      t = 10.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }
      allow(scheduler).to receive(:stats).and_return(idle_stats)
      allow(scheduler).to receive(:ingest_pending?).and_return(false)
      allow(scheduler).to receive(:reset_vtime_if_quiescent!).and_return(true)

      forwarder.send(:maybe_reset_vtime_idle)  # arms the debounce
      expect(scheduler).not_to have_received(:reset_vtime_if_quiescent!)

      t = 30.0                                 # past both the 5s check interval and 15s debounce
      forwarder.send(:maybe_reset_vtime_idle)
      expect(scheduler).to have_received(:reset_vtime_if_quiescent!).once
    end

    it "does not reset while ingest still has backlog" do
      t = 10.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }
      allow(scheduler).to receive(:stats).and_return(idle_stats)
      allow(scheduler).to receive(:ingest_pending?).and_return(true)
      allow(scheduler).to receive(:reset_vtime_if_quiescent!)

      forwarder.send(:maybe_reset_vtime_idle)
      t = 30.0
      forwarder.send(:maybe_reset_vtime_idle)
      expect(scheduler).not_to have_received(:reset_vtime_if_quiescent!)
    end

    it "re-arms the debounce when activity reappears" do
      t = 10.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }
      allow(scheduler).to receive(:ingest_pending?).and_return(false)
      allow(scheduler).to receive(:reset_vtime_if_quiescent!).and_return(true)

      allow(scheduler).to receive(:stats).and_return(idle_stats)
      forwarder.send(:maybe_reset_vtime_idle)  # arm at t=10

      t = 16.0
      allow(scheduler).to receive(:stats).and_return(busy_stats)  # activity → re-arm
      forwarder.send(:maybe_reset_vtime_idle)

      t = 40.0
      allow(scheduler).to receive(:stats).and_return(idle_stats)  # arms again, debounce restarts
      forwarder.send(:maybe_reset_vtime_idle)
      expect(scheduler).not_to have_received(:reset_vtime_if_quiescent!)
    end

    it "does nothing when disabled" do
      KafkaBatch.config.fairness_reset_vtime_when_idle = false
      allow(scheduler).to receive(:stats)
      forwarder.send(:maybe_reset_vtime_idle)
      expect(scheduler).not_to have_received(:stats)
    end
  end

  describe "lifecycle" do
    it "ensure_running! is idempotent and running? reflects the thread state" do
      allow(scheduler).to receive(:checkout).and_return(nil)  # idle loop
      described_class.ensure_running!
      t1 = described_class.thread
      described_class.ensure_running!
      expect(described_class.thread).to be(t1)  # not restarted
      expect(described_class.running?).to be(true)

      described_class.stop!
      expect(described_class.running?).to be(false)
    end
  end
end
