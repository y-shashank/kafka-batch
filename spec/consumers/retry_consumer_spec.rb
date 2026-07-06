RSpec.describe KafkaBatch::Consumers::RetryConsumer do
  let(:consumer) { build_consumer(described_class) }

  def retry_message(retry_after:, retry_to: "test.success", offset: 5)
    FakeMessage.new(
      topic:   KafkaBatch.config.retry_topic,
      offset:  offset,
      payload: {
        "job_id"      => "j1",
        "attempt"     => 1,
        "payload"     => { "x" => 1 },
        "retry_after" => retry_after,
        "retry_to"    => retry_to
      }
    )
  end

  it "re-enqueues a due message to its original topic, stripping retry metadata" do
    consumer.send(:process_retry, retry_message(retry_after: (Time.now - 10).iso8601))

    msg = FakeProducer.for_topic("test.success").first
    expect(msg).not_to be_nil
    expect(msg.payload).not_to have_key("retry_after")
    expect(msg.payload).not_to have_key("retry_to")
    expect(msg.payload["attempt"]).to eq(1)
    expect(msg.key).to eq("j1")
  end

  it "pauses the partition (no produce) when the message is not yet due" do
    msg = retry_message(retry_after: (Time.now + 120).iso8601, offset: 9)
    consumer.send(:process_retry, msg)

    expect(consumer).to have_received(:pause).with(9, kind_of(Integer))
    expect(FakeProducer.for_topic("test.success")).to be_empty
  end

  it "stops the batch at the first not-yet-due message and never skips it (regression)" do
    not_due = retry_message(retry_after: (Time.now + 120).iso8601, offset: 9)
    later   = retry_message(retry_after: (Time.now - 10).iso8601,  offset: 10)
    allow(consumer).to receive(:messages).and_return([not_due, later])

    consumer.consume

    # Paused on the not-due head; must NOT advance past it by handling `later`.
    expect(consumer).to have_received(:pause).with(9, kind_of(Integer))
    expect(FakeProducer.for_topic("test.success")).to be_empty
    expect(consumer).not_to have_received(:mark_as_consumed!)
  end

  it "re-enqueues immediately when retry_after is missing (treats as due now)" do
    msg = retry_message(retry_after: nil, retry_to: "test.success")
    consumer.send(:process_retry, msg)

    expect(FakeProducer.for_topic("test.success").size).to eq(1)
    expect(consumer).not_to have_received(:pause)
  end

  it "fails the batch job AND DLTs an unroutable message (missing retry_to) so the batch can finish" do
    id = SecureRandom.uuid
    msg = FakeMessage.new(
      topic:   KafkaBatch.config.retry_topic,
      payload: {
        "job_id"        => "j1",
        "batch_id"      => id,
        "batch_seq"     => 1,
        "worker_class"  => "SuccessfulWorker",
        "payload"       => {}
      }
    )
    KafkaBatch.store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
    consumer.send(:process_retry, msg)

    evt = FakeProducer.for_topic(KafkaBatch.config.events_topic).first
    expect(evt).not_to be_nil
    expect(evt.payload["status"]).to eq("failed")
    expect(evt.payload["batch_id"]).to eq(id)
    expect(evt.payload["batch_seq"]).to eq(1)

    event_consumer = build_consumer(KafkaBatch::Consumers::EventConsumer)
    event_consumer.send(:process_event, FakeMessage.new(
      topic: KafkaBatch.config.events_topic,
      payload: evt.payload
    ))
    expect(KafkaBatch.store.find_batch(id)[:failed_count]).to eq(1)

    dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
    expect(dlt.first.payload["dlt_type"]).to eq("retry_routing")
  end

  it "does not emit an event for an unroutable standalone job (no batch_id)" do
    msg = FakeMessage.new(topic: KafkaBatch.config.retry_topic, payload: { "job_id" => "j1" })
    consumer.send(:process_retry, msg)

    expect(FakeProducer.for_topic(KafkaBatch.config.events_topic)).to be_empty
    expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).first.payload["dlt_type"]).to eq("retry_routing")
  end

  # ── retry_max_pause_seconds cap ──────────────────────────────────────────
  describe "retry_max_pause_seconds cap" do
    it "caps the pause duration at config.retry_max_pause_seconds when the wait is very long" do
      KafkaBatch.config.retry_max_pause_seconds = 10
      msg = retry_message(retry_after: (Time.now + 300).iso8601, offset: 7)
      consumer.send(:process_retry, msg)

      # pause_ms must be capped at 10_000 (10s), not 300_000
      expect(consumer).to have_received(:pause).with(7, 10_000)
      expect(FakeProducer.for_topic("test.success")).to be_empty
    end

    it "does not apply the cap when the wait is shorter than retry_max_pause_seconds" do
      KafkaBatch.config.retry_max_pause_seconds = 30
      # Use iso8601(3) for millisecond precision — plain iso8601 has only second
      # resolution, making wait_seconds vary up to ±1s depending on when in the
      # second the test runs. With (3) the parse round-trip is ~1ms accurate.
      msg = retry_message(retry_after: (Time.now + 5).iso8601(3), offset: 3)
      consumer.send(:process_retry, msg)

      # Pause must be positive and strictly less than the 30s cap (30_000ms).
      expect(consumer).to have_received(:pause).with(3, be_between(1, 29_999))
    end

    it "falls back to MAX_PAUSE_SECONDS (30) when retry_max_pause_seconds is 0" do
      KafkaBatch.config.retry_max_pause_seconds = 0
      msg = retry_message(retry_after: (Time.now + 120).iso8601, offset: 2)
      consumer.send(:process_retry, msg)

      # The constant fallback is 30s = 30_000ms
      expect(consumer).to have_received(:pause).with(2, 30_000)
    end
  end

  # ── Malformed JSON in retry topic ─────────────────────────────────────────
  describe "malformed JSON" do
    it "routes raw bytes to the DLT, advances the offset, and does not raise" do
      msg = FakeMessage.new(
        topic:   KafkaBatch.config.retry_topic,
        payload: "{this is not valid json",
        offset:  4
      )

      expect { consumer.send(:process_retry, msg) }.not_to raise_error

      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.size).to eq(1)
      expect(dlt.first.payload).to have_key("dlt_raw_payload")  # raw bytes preserved
      expect(dlt.first.payload).to have_key("dlt_parse_error")  # error message captured
      expect(consumer).to have_received(:mark_as_consumed!)     # offset advanced
    end

    it "does not produce to any job or event topic on parse failure" do
      msg = FakeMessage.new(topic: KafkaBatch.config.retry_topic, payload: "not-json")
      consumer.send(:process_retry, msg)

      expect(FakeProducer.for_topic("test.success")).to be_empty
      expect(FakeProducer.for_topic(KafkaBatch.config.events_topic)).to be_empty
    end
  end

  # ── ProducerError on re-enqueue leaves offset uncommitted ─────────────────
  describe "ProducerError on re-enqueue" do
    it "re-raises ProducerError so the offset stays uncommitted (Karafka redelivers)" do
      allow(KafkaBatch::Producer).to receive(:produce_sync)
        .and_raise(KafkaBatch::ProducerError, "broker down")

      msg = retry_message(retry_after: (Time.now - 10).iso8601)
      expect { consumer.send(:process_retry, msg) }.to raise_error(KafkaBatch::ProducerError)
    end

    it "does NOT mark the message consumed when re-enqueue fails" do
      allow(KafkaBatch::Producer).to receive(:produce_sync)
        .and_raise(KafkaBatch::ProducerError, "broker down")

      msg = retry_message(retry_after: (Time.now - 10).iso8601)
      begin
        consumer.send(:process_retry, msg)
      rescue KafkaBatch::ProducerError
        nil
      end

      expect(consumer).not_to have_received(:mark_as_consumed!)
    end
  end
end
