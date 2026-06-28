RSpec.describe KafkaBatch::Fairness::Dispatcher do
  let(:consumer) { build_consumer(described_class) }

  before do
    KafkaBatch.config.fairness_ready_topic    = "test.ready"
    KafkaBatch.config.fairness_ready_lag_high = 100
    KafkaBatch.config.fairness_ready_lag_low  = 10
  end

  def msg(offset:, tenant: "A", job_id: "j#{offset}")
    FakeMessage.new(
      topic:   KafkaBatch.config.fairness_ingest_topic,
      offset:  offset,
      payload: { "job_id" => job_id, "tenant_id" => tenant, "worker_class" => "W", "payload" => {} }
    )
  end

  it "forwards ingest jobs to the ready topic verbatim when not throttled" do
    allow(consumer).to receive(:cached_ready_lag).and_return(0)
    m = [msg(offset: 1, job_id: "j1"), msg(offset: 2, job_id: "j2")]
    allow(consumer).to receive(:messages).and_return(m)

    consumer.consume

    produced = FakeProducer.for_topic("test.ready")
    expect(produced.size).to eq(2)
    expect(produced.first.payload).to eq(m[0].raw_payload)  # raw bytes unchanged
    expect(produced.first.key).to eq("j1")                  # spread by job_id
    expect(consumer).to have_received(:mark_as_consumed!).with(m[1])
  end

  it "pauses and forwards nothing while the ready topic is too deep" do
    allow(consumer).to receive(:cached_ready_lag).and_return(100)  # >= high watermark
    m = [msg(offset: 5)]
    allow(consumer).to receive(:messages).and_return(m)

    consumer.consume

    expect(FakeProducer.for_topic("test.ready")).to be_empty
    expect(consumer).to have_received(:pause).with(5, kind_of(Integer))
  end

  it "hysteresis: stays paused until the depth falls below the low watermark" do
    allow(consumer).to receive(:messages).and_return([msg(offset: 1)])

    allow(consumer).to receive(:cached_ready_lag).and_return(150)  # above high → throttle
    consumer.consume
    expect(FakeProducer.for_topic("test.ready")).to be_empty

    allow(consumer).to receive(:cached_ready_lag).and_return(50)   # between → still throttled
    consumer.consume
    expect(FakeProducer.for_topic("test.ready")).to be_empty

    allow(consumer).to receive(:cached_ready_lag).and_return(5)    # below low → resume
    consumer.consume
    expect(FakeProducer.for_topic("test.ready").size).to eq(1)
  end
end
