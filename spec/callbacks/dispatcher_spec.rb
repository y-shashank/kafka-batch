# frozen_string_literal: true

RSpec.describe KafkaBatch::Callbacks::Dispatcher do
  let(:batch_id) { SecureRandom.uuid }
  let(:batch) do
    {
      id:              batch_id,
      total_jobs:      2,
      completed_count: 2,
      failed_count:    0,
      meta:            { "k" => "v" },
      description:     "test batch"
    }
  end

  before do
    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.fair_time_ready_go_topic = ""
      c.fair_time_ready_ruby_topic = ""
    end
    allow(KafkaBatch.store).to receive(:claim_callback).and_return(true)
  end

  describe "job callbacks (Sidekiq-style)" do
    before do
      definition = KafkaBatch::HandlerDefinition.new(
        job_type: "segment.export.on_success",
        runtime:  :go,
        topic:    "segment.exports.callbacks"
      )
      KafkaBatch::HandlerRegistry.register_definition(definition)
    end

    it "claims dispatch BEFORE enqueueing on_success to the handler topic" do
      batch[:on_success] = KafkaBatch::Callback.dump(
        KafkaBatch::Callback.job("segment.export.on_success", topic: "segment.exports.callbacks")
      )
      batch[:callback_args] = { "run_id" => "99" }
      claim_order = []
      allow(KafkaBatch.store).to receive(:claim_callback) do |*|
        claim_order << :claim
        true
      end
      allow(FakeProducer).to receive(:record).and_wrap_original do |orig, **kwargs|
        claim_order << :produce
        orig.call(**kwargs)
      end

      mode = described_class.dispatch!(batch: batch, outcome: "success")

      expect(mode).to eq(:job_only)
      expect(claim_order).to eq(%i[claim produce])
      produced = FakeProducer.for_topic("segment.exports.callbacks")
      expect(produced.size).to eq(1)
      payload = produced.first.payload
      expect(payload["job_type"]).to eq("segment.export.on_success")
      expect(payload["payload"]["callback_args"]).to eq("run_id" => "99")
      expect(payload["payload"]).not_to have_key("meta")
      expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)).to be_empty
      expect(KafkaBatch.store).to have_received(:claim_callback).with(batch_id, KafkaBatch.node_id, "success")
    end

    it "skips job enqueue when the claim is already taken" do
      batch[:on_success] = KafkaBatch::Callback.dump(
        KafkaBatch::Callback.job("segment.export.on_success", topic: "segment.exports.callbacks")
      )
      allow(KafkaBatch.store).to receive(:claim_callback).and_return(false)

      mode = described_class.dispatch!(batch: batch, outcome: "success")

      expect(mode).to eq(:none)
      expect(FakeProducer.for_topic("segment.exports.callbacks")).to be_empty
    end

    it "skips claim when preclaimed and fires success_only as on_success only" do
      batch[:on_success] = KafkaBatch::Callback.dump(
        KafkaBatch::Callback.job("segment.export.on_success", topic: "segment.exports.callbacks")
      )
      batch[:on_complete] = KafkaBatch::Callback.dump(
        KafkaBatch::Callback.job("segment.export.on_complete", topic: "segment.exports.callbacks")
      )
      definition = KafkaBatch::HandlerDefinition.new(
        job_type: "segment.export.on_complete",
        runtime:  :go,
        topic:    "segment.exports.callbacks"
      )
      KafkaBatch::HandlerRegistry.register_definition(definition)

      mode = described_class.dispatch!(batch: batch, outcome: "success_only", preclaimed: true)

      expect(mode).to eq(:job_only)
      expect(KafkaBatch.store).not_to have_received(:claim_callback)
      produced = FakeProducer.for_topic("segment.exports.callbacks")
      expect(produced.map { |m| m.payload["job_id"] }).to eq(["#{batch_id}:on_success"])
    end

    it "enqueues on_complete with a distinct job_id" do
      batch[:on_complete] = KafkaBatch::Callback.dump(
        KafkaBatch::Callback.job("segment.export.on_complete", topic: "segment.exports.callbacks")
      )
      definition = KafkaBatch::HandlerDefinition.new(
        job_type: "segment.export.on_complete",
        runtime:  :go,
        topic:    "segment.exports.callbacks"
      )
      KafkaBatch::HandlerRegistry.register_definition(definition)

      described_class.dispatch!(batch: batch, outcome: "complete")

      produced = FakeProducer.for_topic("segment.exports.callbacks")
      expect(produced.map { |m| m.payload["job_id"] }).to eq(["#{batch_id}:on_complete"])
      expect(KafkaBatch.store).to have_received(:claim_callback).with(batch_id, KafkaBatch.node_id, "complete")
    end
  end

  describe "legacy Ruby class callbacks" do
    it "produces to callbacks_topic and does not claim" do
      batch[:on_complete] = "RecordingCallback"

      mode = described_class.dispatch!(batch: batch, outcome: "success")

      expect(mode).to eq(:legacy_only)
      cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
      expect(cb.size).to eq(1)
      expect(cb.first.payload["on_complete"]).to eq("RecordingCallback")
      expect(KafkaBatch.store).not_to have_received(:claim_callback)
    end
  end
end
