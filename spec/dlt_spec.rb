# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Dlt do
  it "publishes to the dead-letter topic and instruments dlt.published" do
    payload = { "job_id" => "j1", "batch_id" => "b1", "dlt_type" => "job" }

    expect(KafkaBatch::Producer).to receive(:produce_sync).with(
      topic:   KafkaBatch.config.dead_letter_topic,
      payload: payload,
      key:     "j1"
    )
    expect(KafkaBatch::Instrumentation).to receive(:dlt_published).with(
      batch_id:     "b1",
      job_id:       "j1",
      dlt_type:     "job",
      source_topic: "test.topic"
    )

    described_class.publish(
      payload:      payload,
      dlt_type:     "job",
      source_topic: "test.topic",
      key:          "j1"
    )
  end

  it "does not instrument when produce fails" do
    allow(KafkaBatch::Producer).to receive(:produce_sync)
      .and_raise(KafkaBatch::ProducerError, "broker down")
    expect(KafkaBatch::Instrumentation).not_to receive(:dlt_published)

    expect {
      described_class.publish(
        payload:      { "dlt_type" => "job" },
        dlt_type:     "job",
        source_topic: "t"
      )
    }.to raise_error(KafkaBatch::ProducerError)
  end

  it "falls back to a random key when job_id and batch_id are absent" do
    expect(KafkaBatch::Producer).to receive(:produce_sync) do |**kwargs|
      expect(kwargs[:key]).not_to be_nil
      expect(kwargs[:key]).not_to be_empty
    end
    allow(KafkaBatch::Instrumentation).to receive(:dlt_published)

    described_class.publish(
      payload:      { "dlt_type" => "retry_routing" },
      dlt_type:     "retry_routing",
      source_topic: "retry.short"
    )
  end
end
