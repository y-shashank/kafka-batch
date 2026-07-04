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

  it "starts the forwarder and enqueues each ingest job into the scheduler keyed by tenant" do
    m = [msg(offset: 1, tenant: "acme"), msg(offset: 2, tenant: "globex")]
    allow(consumer).to receive(:messages).and_return(m)
    allow(scheduler).to receive(:enqueue).and_return(:ok)

    consumer.consume

    expect(KafkaBatch::Fairness::Forwarder).to have_received(:ensure_running!)
    expect(scheduler).to have_received(:enqueue).with("acme",   m[0].raw_payload)
    expect(scheduler).to have_received(:enqueue).with("globex", m[1].raw_payload)
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

    expect(scheduler).to have_received(:enqueue).with("batch-7", m[0].raw_payload)
    expect(scheduler).to have_received(:enqueue).with("solo",    m[1].raw_payload)
  end

  it "pauses without committing when the scheduler is unavailable" do
    allow(KafkaBatch).to receive(:scheduler).and_return(nil)
    m = [msg(offset: 5)]
    allow(consumer).to receive(:messages).and_return(m)

    consumer.consume

    expect(consumer).to have_received(:pause).with(5, kind_of(Integer))
    expect(consumer).not_to have_received(:mark_as_consumed!)
  end
end
