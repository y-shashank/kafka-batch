# frozen_string_literal: true

require "oj"

RSpec.describe KafkaBatch::Browse::Reader do
  let(:consumer) { instance_double(Rdkafka::Consumer) }
  let(:reader) { described_class.new(consumer: consumer) }

  def msg(partition:, offset:, payload:)
    instance_double(
      Rdkafka::Consumer::Message,
      topic: "jobs",
      partition: partition,
      offset: offset,
      timestamp: Time.at(1_700_000_000),
      payload: payload
    )
  end

  before do
    allow(KafkaBatch::Lag).to receive(:available?).and_return(true)
    allow(KafkaBatch::Lag).to receive(:read_group).and_return(
      "cg" => { "jobs" => { 0 => { offset: 10, lag: 3 } } }
    )
    allow(reader).to receive(:partition_ids).with("jobs").and_return([0])
    allow(consumer).to receive(:assign)
    allow(consumer).to receive(:close)
    allow(consumer).to receive(:query_watermark_offsets).with("jobs", 0, anything).and_return([0, 13])
  end

  after { reader.close }

  it "reads only unprocessed messages at/after the committed offset" do
    payloads = [
      msg(partition: 0, offset: 10, payload: Oj.dump({ "job_id" => "a", "worker_class" => "W", "args" => [1] })),
      msg(partition: 0, offset: 11, payload: Oj.dump({ "job_id" => "b", "worker_class" => "W" })),
      msg(partition: 0, offset: 12, payload: Oj.dump({ "job_id" => "c", "worker_class" => "W" })),
      nil
    ]
    allow(consumer).to receive(:poll) { payloads.shift }

    page = reader.fetch_page(topic: "jobs", group: "cg", limit: 50)
    expect(page[:messages].map { |m| m[:job_id] }).to eq(%w[a b c])
    expect(page[:messages].map { |m| m[:offset] }).to all(be >= 10)
    expect(page[:has_next]).to eq(false)
    expect(page[:commits]["0"]).to eq(10)
  end

  it "does not dump the log from the beginning when commits are unknown" do
    allow(KafkaBatch::Lag).to receive(:read_group).and_return({})
    allow(consumer).to receive(:poll).and_raise("should not poll")

    page = reader.fetch_page(topic: "jobs", group: "cg", limit: 50)
    expect(page[:messages]).to eq([])
    expect(page[:has_next]).to eq(false)
  end

  it "skips partitions that are caught up (committed >= high watermark)" do
    allow(KafkaBatch::Lag).to receive(:read_group).and_return(
      "cg" => { "jobs" => { 0 => { offset: 13, lag: 0 } } }
    )
    allow(consumer).to receive(:poll).and_raise("should not poll")

    page = reader.fetch_page(topic: "jobs", group: "cg", limit: 50)
    expect(page[:messages]).to eq([])
  end

  it "does not scan never-consumed partitions (lag page shows 0 even if the log has history)" do
    allow(KafkaBatch::Lag).to receive(:read_group).and_return(
      "cg" => { "jobs" => { 0 => { offset: -1, lag: -1 } } }
    )
    allow(consumer).to receive(:query_watermark_offsets).with("jobs", 0, anything).and_return([0, 50])
    allow(consumer).to receive(:poll).and_raise("should not poll")

    page = reader.fetch_page(topic: "jobs", group: "cg", limit: 50)
    expect(page[:messages]).to eq([])
  end

  it "does not scan from offset 0 when committed is caught up even if the log retains old messages" do
    # low=0, high=50, committed=50 → lag 0; must not return offsets 0..49
    allow(KafkaBatch::Lag).to receive(:read_group).and_return(
      "cg" => { "jobs" => { 0 => { offset: 50, lag: 0 } } }
    )
    allow(consumer).to receive(:query_watermark_offsets).with("jobs", 0, anything).and_return([0, 50])
    allow(consumer).to receive(:poll).and_raise("should not poll")

    page = reader.fetch_page(topic: "jobs", group: "cg", limit: 50)
    expect(page[:messages]).to eq([])
  end

  it "lists topic lag from the same admin map as the Kafka lag page" do
    allow(KafkaBatch::Lag).to receive(:partitions).and_return(
      [
        { group: "cg", topic: "jobs", partition: 0, committed: 10, end_offset: 10, lag: 0, never_consumed: false },
        { group: "cg", topic: "jobs", partition: 1, committed: nil, end_offset: nil, lag: 0, never_consumed: true }
      ]
    )
    topics = reader.list_topics
    expect(topics.size).to eq(1)
    expect(topics.first[:lag]).to eq(0)
    expect(topics.first[:partition_meta].map { |p| p[:lag] }).to eq([0, 0])
  end

  it "honors partition + start_offset and paginates with a cursor" do
    allow(consumer).to receive(:query_watermark_offsets).with("jobs", 0, anything).and_return([0, 17])
    first = (10...15).map do |off|
      msg(partition: 0, offset: off, payload: Oj.dump({ "job_id" => "j#{off}", "args" => [] }))
    end + [nil]
    second = (15...17).map do |off|
      msg(partition: 0, offset: off, payload: Oj.dump({ "job_id" => "j#{off}", "args" => [] }))
    end + [nil]

    polls = first
    allow(consumer).to receive(:poll) { polls.shift }

    page1 = reader.fetch_page(
      topic: "jobs", group: "cg", partition: 0, start_offset: 10, limit: 5
    )
    expect(page1[:messages].size).to eq(5)
    expect(page1[:has_next]).to eq(true)
    expect(page1[:cursor]).to be_a(String)

    polls = second
    page2 = reader.fetch_page(
      topic: "jobs", group: "cg", partition: 0, start_offset: 10,
      cursor: page1[:cursor], limit: 5
    )
    expect(page2[:messages].map { |m| m[:offset] }).to eq([15, 16])
    expect(page2[:has_next]).to eq(false)
  end
end
