# frozen_string_literal: true

RSpec.describe KafkaBatch::JobExpiry do
  describe ".normalize_valid_till" do
    it "returns ISO8601 UTC for Time values" do
      t = Time.utc(2026, 1, 15, 12, 0, 0)
      expect(described_class.normalize_valid_till(t)).to eq("2026-01-15T12:00:00Z")
    end

    it "returns nil for blank input" do
      expect(described_class.normalize_valid_till(nil)).to be_nil
      expect(described_class.normalize_valid_till("")).to be_nil
    end
  end

  describe ".expired?" do
    it "is false when valid_till is absent" do
      expect(described_class.expired?({ "job_id" => "x" })).to eq(false)
    end

    it "is true when now is past valid_till" do
      data = { "valid_till" => 1.hour.ago.iso8601 }
      expect(described_class.expired?(data)).to eq(true)
    end

    it "is false when valid_till is in the future" do
      data = { "valid_till" => 1.hour.from_now.iso8601 }
      expect(described_class.expired?(data)).to eq(false)
    end

    it "is true when valid_till is unparseable (poison pill → DLT path)" do
      expect(described_class.expired?({ "valid_till" => "not-a-timestamp" })).to eq(true)
    end
  end
end

RSpec.describe "valid_till enqueue + consumption" do
  before do
    KafkaBatchSpec::WorkerRuns.reset!
    FakeProducer.reset!
  end

  it "embeds valid_till on immediate enqueue" do
    till = 2.hours.from_now
    KafkaBatch::Batch.enqueue(SuccessfulWorker, { "id" => 1 }, valid_till: till)

    msg = FakeProducer.messages.last
    expect(msg[:payload]["valid_till"]).to eq(KafkaBatch::JobExpiry.normalize_valid_till(till))
  end

  it "JobConsumer sends expired jobs to DLT without running perform" do
    KafkaBatch::Batch.enqueue(
      SuccessfulWorker,
      { "id" => 99 },
      valid_till: 1.minute.ago
    )

    raw = Oj.dump(FakeProducer.messages.last[:payload])
    consumer = build_consumer(KafkaBatch::Consumers::JobConsumer)
    msg = FakeMessage.new(
      topic: "test.success", partition: 0, offset: 1, payload: raw
    )

    consumer.send(:process_message, msg)

    expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
    dlt = FakeProducer.messages.find { |m| m[:topic] == KafkaBatch.config.dead_letter_topic }
    expect(dlt[:payload]["dlt_type"]).to eq("expired")
    expect(dlt[:payload]["job_id"]).to eq(FakeProducer.messages.first[:payload]["job_id"])
  end

  it "JobConsumer DLTs jobs with unparseable valid_till instead of stalling the partition" do
    consumer = build_consumer(KafkaBatch::Consumers::JobConsumer)
    msg = FakeMessage.new(
      topic: "test.success", partition: 0, offset: 9,
      payload: {
        "job_id" => "j-bad-till", "worker_class" => SuccessfulWorker.name,
        "payload" => {}, "valid_till" => "not-a-timestamp"
      }
    )

    expect { consumer.send(:process_message, msg) }.not_to raise_error
    expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
    dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).first
    expect(dlt.payload["dlt_type"]).to eq("expired")
  end
end
