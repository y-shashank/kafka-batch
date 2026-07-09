RSpec.describe KafkaBatch::Fairness::Forwarder do
  let(:scheduler) { instance_double(KafkaBatch::Fairness::Scheduler) }

  before do
    KafkaBatch.config.fair_time_ready_topic = "test.ready"
    KafkaBatch.config.fair_time_ready_go_topic = ""
    KafkaBatch.config.fair_time_ready_ruby_topic = ""
    allow(KafkaBatch).to receive(:scheduler).and_return(scheduler)
    allow(scheduler).to receive(:confirm_forward)
  end

  after { described_class.reset! }

  describe "#forward_once" do
    subject(:forwarder) { described_class.new }

    it "routes go handlers to the .go ready topic when runtime split is enabled" do
      KafkaBatch.config.fair_time_ready_go_topic = "test.ready.go"
      KafkaBatch.config.fair_time_ready_ruby_topic = "test.ready.ruby"
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

      produced = FakeProducer.for_topic("test.ready")
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

      decoded = Oj.load(FakeProducer.for_topic("test.ready").first.payload)
      expect(decoded["tenant_id"]).to eq("globex")
    end

    it "returns false and forwards nothing when checkout is empty (budget full / idle)" do
      allow(scheduler).to receive(:checkout).and_return(nil)

      expect(forwarder.forward_once).to be(false)
      expect(FakeProducer.for_topic("test.ready")).to be_empty
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
      expect(FakeProducer.for_topic("test.ready")).to be_empty
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
