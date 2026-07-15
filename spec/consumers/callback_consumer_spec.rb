RSpec.describe KafkaBatch::Consumers::CallbackConsumer do
  let(:consumer) { build_consumer(described_class) }

  def seed_batch(id: SecureRandom.uuid, total: 1, **opts)
    KafkaBatch.store.create_batch(id: id, total_jobs: total, **opts)
    id
  end

  def callback_message(id:, outcome:, on_success: nil, on_complete: nil)
    FakeMessage.new(
      topic:   KafkaBatch.config.callbacks_topic,
      payload: {
        "batch_id"        => id,
        "outcome"         => outcome,
        "total_jobs"      => 1,
        "completed_count" => outcome == "success" ? 1 : 0,
        "failed_count"    => outcome == "success" ? 0 : 1,
        "on_success"      => on_success,
        "on_complete"     => on_complete
      }
    )
  end

  # Regression for fix #1: a nil callback field must not raise NoMethodError.
  describe "single-callback batches (the nil-callback regression)" do
    it "fires on_complete when on_success is nil" do
      id = seed_batch(on_complete: "RecordingCallback")
      msg = callback_message(id: id, outcome: "complete", on_complete: "RecordingCallback")

      expect { consumer.send(:process_callback, msg) }.not_to raise_error
      expect(KafkaBatchSpec::CallbackDoubles.invocations.map { |i| i[:name] }).to eq([:on_complete])
    end

    it "fires on_success when on_complete is nil" do
      id = seed_batch(on_success: "RecordingCallback")
      msg = callback_message(id: id, outcome: "success", on_success: "RecordingCallback")

      expect { consumer.send(:process_callback, msg) }.not_to raise_error
      expect(KafkaBatchSpec::CallbackDoubles.invocations.map { |i| i[:name] }).to eq([:on_success])
    end
  end

  describe "outcome gating" do
    it "does not fire on_success when the outcome is complete (a job failed)" do
      id = seed_batch(on_success: "RecordingCallback", on_complete: "RecordingCallback")
      msg = callback_message(id: id, outcome: "complete",
                             on_success: "RecordingCallback", on_complete: "RecordingCallback")

      consumer.send(:process_callback, msg)
      names = KafkaBatchSpec::CallbackDoubles.invocations.map { |i| i[:name] }
      expect(names).to eq([:on_complete])
    end
  end

  describe "single-winner claim-before-invoke" do
    it "claims dispatch BEFORE the callback has run" do
      id = seed_batch(on_complete: "OrderCheckingCallback")
      msg = callback_message(id: id, outcome: "complete", on_complete: "OrderCheckingCallback")

      consumer.send(:process_callback, msg)

      flag = KafkaBatchSpec::CallbackDoubles.invocations.find { |i| i[:name] == :dispatched_at_invocation }
      expect(flag[:args]).to be(true) # claimed before invoke (Redis fence)
      expect(KafkaBatch.store.callback_dispatched?(id)).to be(true)
    end

    it "suppresses a duplicate callback message" do
      id = seed_batch(on_complete: "RecordingCallback")
      msg = callback_message(id: id, outcome: "complete", on_complete: "RecordingCallback")

      consumer.send(:process_callback, msg)
      consumer.send(:process_callback, msg) # redelivery

      expect(KafkaBatchSpec::CallbackDoubles.invocations.size).to eq(1)
    end

    it "invokes only once when two consumers race the claim" do
      id = seed_batch(on_complete: "RecordingCallback")
      msg = callback_message(id: id, outcome: "complete", on_complete: "RecordingCallback")
      other = build_consumer(described_class)

      threads = [
        Thread.new { consumer.send(:process_callback, msg) },
        Thread.new { other.send(:process_callback, msg) }
      ]
      threads.each(&:join)

      expect(KafkaBatchSpec::CallbackDoubles.invocations.size).to eq(1)
    end
  end

  describe "error handling" do
    it "forwards an unresolvable callback class to the DLT" do
      id = seed_batch(on_complete: "NoSuchCallbackClass")
      msg = callback_message(id: id, outcome: "complete", on_complete: "NoSuchCallbackClass")

      consumer.send(:process_callback, msg)
      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.size).to eq(1)
      expect(dlt.first.payload["dlt_type"]).to eq("callback")
    end

    it "forwards a resolvable class with a missing method to the DLT" do
      id = seed_batch(on_complete: "MethodlessCallback")
      msg = callback_message(id: id, outcome: "complete", on_complete: "MethodlessCallback")

      consumer.send(:process_callback, msg)
      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.size).to eq(1)
      expect(dlt.first.payload["dlt_type"]).to eq("callback")
      expect(dlt.first.payload["dlt_error_class"]).to eq("NoMethodError")
    end

    it "forwards a raising callback to the DLT with dlt_type callback_error" do
      id = seed_batch(on_complete: "ExplodingCallback")
      msg = callback_message(id: id, outcome: "complete", on_complete: "ExplodingCallback")

      consumer.send(:process_callback, msg)
      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.first.payload["dlt_type"]).to eq("callback_error")
    end

    it "routes malformed JSON to the DLT" do
      msg = FakeMessage.new(topic: KafkaBatch.config.callbacks_topic, payload: "{not json")
      consumer.send(:process_callback, msg)

      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.first.payload["dlt_type"]).to eq("malformed_callback")
    end

    it "skips messages with no batch_id" do
      msg = FakeMessage.new(topic: KafkaBatch.config.callbacks_topic, payload: { "outcome" => "success" })
      expect { consumer.send(:process_callback, msg) }.not_to raise_error
      expect(FakeProducer.messages).to be_empty
    end
  end
end
