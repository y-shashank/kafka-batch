RSpec.describe KafkaBatch::Consumers::EventConsumer do
  let(:consumer) { build_consumer(described_class) }

  # EventConsumer#consume fires a detached maybe_reconcile background thread that
  # can outlive an example and produce a duplicate callback into a later one
  # (order-dependent flake). Neutralize it for the non-watchdog examples by
  # resetting the process-global gate and stubbing the reconciler body to a no-op.
  # The "#maybe_reconcile dead-thread watchdog" describe re-stubs Reconciler.run
  # and resets this state for its own tests, so it is unaffected.
  before do
    described_class.last_reconcile_at  = nil
    described_class.reconciler_running = false
    described_class.reconciler_thread  = nil
    allow(KafkaBatch::Reconciler).to receive(:run)
  end

  # Completion events carry the job message's source coordinates and are
  # deduplicated by job_id.
  def event(id:, status:, src_offset:, src_partition: 0, src_topic: "wt")
    FakeMessage.new(
      topic:   KafkaBatch.config.events_topic,
      payload: {
        "batch_id"      => id,
        "job_id"        => "j#{src_offset}",
        "status"        => status,
        "occurred_at"   => Time.now.iso8601,
        "src_topic"     => src_topic,
        "src_partition" => src_partition,
        "src_offset"    => src_offset
      }
    )
  end

  it "increments the batch and produces a callback when the batch finishes" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2, on_complete: "RecordingCallback")

    consumer.send(:process_event, event(id: id, status: "success", src_offset: 10))
    expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)).to be_empty

    consumer.send(:process_event, event(id: id, status: "success", src_offset: 11))
    cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
    expect(cb.size).to eq(1)
    expect(cb.first.payload["outcome"]).to eq("success")
    expect(cb.first.payload["on_complete"]).to eq("RecordingCallback")
  end

  it "applies a whole poll in one batch, counting every event exactly once" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 3, on_complete: "RecordingCallback")

    msgs = [
      event(id: id, status: "success", src_offset: 10),
      event(id: id, status: "success", src_offset: 11),
      event(id: id, status: "success", src_offset: 10),  # duplicate job_id – must not double-count
      event(id: id, status: "failed",  src_offset: 12)
    ]
    allow(consumer).to receive(:messages).and_return(msgs)

    consumer.consume

    b = KafkaBatch.store.find_batch(id)
    expect(b[:completed_count]).to eq(2)  # job j10 counted once
    expect(b[:failed_count]).to eq(1)

    cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
    expect(cb.size).to eq(1)
    expect(cb.first.payload["outcome"]).to eq("complete")
  end

  it "commits the poll exactly once (marks only the last message consumed)" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2)
    msgs = [event(id: id, status: "success", src_offset: 1), event(id: id, status: "success", src_offset: 2)]
    allow(consumer).to receive(:messages).and_return(msgs)

    consumer.consume
    expect(consumer).to have_received(:mark_as_consumed!).with(msgs.last).once
  end

  it "produces a complete (not success) callback when a job failed" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2)

    consumer.send(:process_event, event(id: id, status: "success", src_offset: 10))
    consumer.send(:process_event, event(id: id, status: "failed", src_offset: 11))

    expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic).first.payload["outcome"]).to eq("complete")
  end

  it "deduplicates a re-produced/redelivered event by source offset" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 2)

    2.times { consumer.send(:process_event, event(id: id, status: "success", src_offset: 10)) }
    expect(KafkaBatch.store.find_batch(id)[:completed_count]).to eq(1)
  end

  it "routes malformed JSON to the DLT" do
    msg = FakeMessage.new(topic: KafkaBatch.config.events_topic, payload: "{bad")
    consumer.send(:process_event, msg)
    expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).first.payload["dlt_type"]).to eq("malformed_event")
  end

  it "skips events with missing batch_id/status without producing anything" do
    msg = FakeMessage.new(topic: KafkaBatch.config.events_topic, payload: { "batch_id" => "x" })
    consumer.send(:process_event, msg)
    expect(FakeProducer.messages).to be_empty
  end

  it "skips events missing source coordinates" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 1)
    msg = FakeMessage.new(topic: KafkaBatch.config.events_topic,
                          payload: { "batch_id" => id, "status" => "success" })

    consumer.send(:process_event, msg)
    expect(KafkaBatch.store.find_batch(id)[:completed_count]).to eq(0)
  end

  # ── ProducerError re-raise leaves offset uncommitted ─────────────────────
  describe "#apply re-raise on ProducerError" do
    it "propagates ProducerError so the offset stays uncommitted and Karafka redelivers" do
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")

      # Make produce_sync raise ProducerError when targeting the callbacks topic.
      allow(KafkaBatch::Producer).to receive(:produce_sync) do |topic:, **|
        raise KafkaBatch::ProducerError, "broker timeout" if topic == KafkaBatch.config.callbacks_topic
      end

      msgs = [event(id: id, status: "success", src_offset: 1)]
      allow(consumer).to receive(:messages).and_return(msgs)

      expect { consumer.consume }.to raise_error(KafkaBatch::ProducerError, /broker timeout/)
    end

    it "does NOT mark any message consumed when ProducerError is raised" do
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")

      allow(KafkaBatch::Producer).to receive(:produce_sync) do |topic:, **|
        raise KafkaBatch::ProducerError, "broker timeout" if topic == KafkaBatch.config.callbacks_topic
      end

      msgs = [event(id: id, status: "success", src_offset: 1)]
      allow(consumer).to receive(:messages).and_return(msgs)

      begin
        consumer.consume
      rescue KafkaBatch::ProducerError
        nil
      end

      expect(consumer).not_to have_received(:mark_as_consumed!)
    end

    it "logs a clear error before re-raising so operators see the failure immediately" do
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")

      allow(KafkaBatch::Producer).to receive(:produce_sync) do |topic:, **|
        raise KafkaBatch::ProducerError, "broker timeout" if topic == KafkaBatch.config.callbacks_topic
      end

      expect(KafkaBatch.logger).to receive(:error).with(/Failed to produce callback/)

      # apply takes raw event hashes — same format as record_completions_batch input.
      events = [{
        batch_id:         id,
        job_id:           "j1",
        source_topic:     "wt",
        source_partition: 0,
        source_offset:    1,
        status:           "success"
      }]
      begin
        consumer.send(:apply, events)
      rescue KafkaBatch::ProducerError
        nil
      end
    end
  end

  # ── maybe_reconcile dead-thread self-heal ─────────────────────────────────
  describe "#maybe_reconcile dead-thread watchdog" do
    before do
      # Reset class-level reconciler state between tests
      described_class.last_reconcile_at  = nil
      described_class.reconciler_running = false
      described_class.reconciler_thread  = nil
      KafkaBatch.config.reconciliation_interval = 0  # always trigger
    end

    after do
      described_class.last_reconcile_at  = nil
      described_class.reconciler_running = false
      described_class.reconciler_thread  = nil
    end

    it "resets reconciler_running and spawns a new thread when the stored thread is dead" do
      # Simulate a thread that died before its ensure block ran
      dead_thread = Thread.new { raise "unexpected death" }
      dead_thread.join rescue nil  # let it die

      described_class.reconciler_running = true
      described_class.reconciler_thread  = dead_thread

      stub_reconciler = -> { nil }
      allow(KafkaBatch::Reconciler).to receive(:run, &stub_reconciler)

      consumer.send(:maybe_reconcile)

      # Watchdog should have reset the flag and spawned a fresh thread
      sleep(0.05)  # give the thread a moment to start
      expect(KafkaBatch::Reconciler).to have_received(:run)
    end

    it "does not spawn a second thread when a live thread is already running" do
      # Use a Queue to block thread 1 so it stays alive (reconciler_running=true)
      # while we make the second maybe_reconcile call.
      blocker = Queue.new
      allow(KafkaBatch::Reconciler).to receive(:run) { blocker.pop }

      consumer.send(:maybe_reconcile)  # spawns thread 1, reconciler_running=true
      sleep(0.02)  # let thread 1 start and block on blocker.pop

      consumer.send(:maybe_reconcile)  # sees reconciler_running=true → no-ops

      blocker.push(:done)  # unblock thread 1 so it can finish
      described_class.reconciler_thread&.join(1)

      expect(KafkaBatch::Reconciler).to have_received(:run).once
    end

    it "clears reconciler_running after the thread finishes (via ensure)" do
      allow(KafkaBatch::Reconciler).to receive(:run)

      consumer.send(:maybe_reconcile)

      # Wait for the reconciler thread to finish
      described_class.reconciler_thread&.join(1)
      expect(described_class.reconciler_running).to be(false)
    end

    it "clears reconciler_running when the reconciler thread itself raises" do
      allow(KafkaBatch::Reconciler).to receive(:run).and_raise(StandardError, "db down")

      consumer.send(:maybe_reconcile)
      described_class.reconciler_thread&.join(1)

      expect(described_class.reconciler_running).to be(false)
    end
  end
end
