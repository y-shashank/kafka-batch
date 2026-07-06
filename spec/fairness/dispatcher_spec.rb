RSpec.describe KafkaBatch::Fairness::Dispatcher do
  let(:consumer) { build_consumer(described_class) }
  let(:scheduler) { instance_double(KafkaBatch::Fairness::Scheduler) }

  before do
    KafkaBatch.config.fair_time_ingest_topic = "test.ingest"
    # Isolate from any real forwarder thread / scheduler.
    allow(KafkaBatch::Fairness::Forwarder).to receive(:ensure_running!)
    allow(KafkaBatch).to receive(:scheduler).and_return(scheduler)
  end

  def msg(offset:, tenant: "A", job_id: "j#{offset}", batch_id: nil)
    payload = { "job_id" => job_id, "worker_class" => "W", "payload" => {} }
    payload["tenant_id"] = tenant if tenant
    payload["batch_id"]  = batch_id if batch_id
    FakeMessage.new(topic: KafkaBatch.config.fair_time_ingest_topic, offset: offset, payload: payload)
  end

  def stamped_raw(message)
    data = Oj.load(message.raw_payload)
    Oj.dump(KafkaBatch::JobExpiry.stamp_source!(
      data, topic: message.topic, partition: message.partition, offset: message.offset
    ))
  end

  it "starts the forwarder and enqueues each ingest job into the scheduler keyed by tenant" do
    m = [msg(offset: 1, tenant: "acme"), msg(offset: 2, tenant: "globex")]
    allow(consumer).to receive(:messages).and_return(m)
    allow(scheduler).to receive(:enqueue).and_return(:ok)

    consumer.consume

    expect(KafkaBatch::Fairness::Forwarder).to have_received(:ensure_running!)
    expect(scheduler).to have_received(:enqueue).with("acme",   stamped_raw(m[0]))
    expect(scheduler).to have_received(:enqueue).with("globex", stamped_raw(m[1]))
    expect(consumer).to have_received(:mark_as_consumed!).with(m[1])  # last committed
  end

  it "applies backpressure (pause + partial commit) when a tenant window is full" do
    m = [msg(offset: 1, tenant: "acme"), msg(offset: 2, tenant: "acme"), msg(offset: 3, tenant: "acme")]
    allow(consumer).to receive(:messages).and_return(m)
    # First enqueue ok, second reports the window is full.
    allow(scheduler).to receive(:enqueue).and_return(:ok, :full)

    consumer.consume

    expect(consumer).to have_received(:mark_as_consumed!).with(m[0])       # progress committed
    expect(consumer).to have_received(:pause).with(2, kind_of(Integer))    # retry from the full msg
    # The third message must NOT have been enqueued (we bailed out).
    expect(scheduler).to have_received(:enqueue).twice
  end

  it "falls back to batch_id then job_id when tenant_id is absent" do
    m = [msg(offset: 1, tenant: nil, batch_id: "batch-7"), msg(offset: 2, tenant: nil, batch_id: nil, job_id: "solo")]
    allow(consumer).to receive(:messages).and_return(m)
    allow(scheduler).to receive(:enqueue).and_return(:ok)

    consumer.consume

    expect(scheduler).to have_received(:enqueue).with("batch-7", stamped_raw(m[0]))
    expect(scheduler).to have_received(:enqueue).with("solo",    stamped_raw(m[1]))
  end

  it "pauses without committing when the scheduler is unavailable" do
    allow(KafkaBatch).to receive(:scheduler).and_return(nil)
    m = [msg(offset: 5)]
    allow(consumer).to receive(:messages).and_return(m)

    consumer.consume

    expect(consumer).to have_received(:pause).with(5, kind_of(Integer))
    expect(consumer).not_to have_received(:mark_as_consumed!)
  end

  it "routes malformed ingest JSON to the DLT and does not enqueue garbage" do
    bad = FakeMessage.new(topic: KafkaBatch.config.fair_time_ingest_topic, offset: 4, payload: "{bad")
    good = msg(offset: 5, tenant: "acme")
    allow(consumer).to receive(:messages).and_return([bad, good])
    allow(scheduler).to receive(:enqueue).and_return(:ok)

    consumer.consume

    dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
    expect(dlt.size).to eq(1)
    expect(dlt.first.payload["dlt_type"]).to eq("malformed_ingest")
    expect(scheduler).to have_received(:enqueue).once.with("acme", stamped_raw(good))
    expect(consumer).to have_received(:mark_as_consumed!).with(good)
  end
end
