# frozen_string_literal: true

RSpec.describe KafkaBatch::RetryCancel do
  before { described_class.reset! }
  after  { described_class.reset! }

  it "cancels job ids and acknowledges them" do
    expect(described_class.cancel!(%w[j1 j2 j1])).to eq(2)
    expect(described_class.cancelled?("j1")).to eq(true)
    expect(described_class.cancelled?("j3")).to eq(false)

    described_class.acknowledge!("j1")
    expect(described_class.cancelled?("j1")).to eq(false)
    expect(described_class.cancelled?("j2")).to eq(true)
  end

  it "stores skip watermarks and clears the cancel set on delete-all" do
    described_class.cancel!(%w[a b])
    described_class.set_skip_watermarks!(
      "kafka_batch.jobs.retry.short" => { 0 => 10, 1 => 5 }
    )
    described_class.clear_cancel_set!

    expect(described_class.cancelled?("a")).to eq(false)
    expect(described_class.should_skip?(
      topic: "kafka_batch.jobs.retry.short", partition: 0, offset: 10, job_id: "x"
    )).to eq(true)
    expect(described_class.should_skip?(
      topic: "kafka_batch.jobs.retry.short", partition: 0, offset: 11, job_id: "x"
    )).to eq(false)

    described_class.cancel!(["z"])
    expect(described_class.should_skip?(
      topic: "kafka_batch.jobs.retry.short", partition: 0, offset: 11, job_id: "z"
    )).to eq(true)
  end
end
