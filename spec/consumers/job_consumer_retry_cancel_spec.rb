# frozen_string_literal: true

RSpec.describe "JobConsumer retry cancel" do
  let(:consumer) { build_consumer(KafkaBatch::Consumers::JobConsumer) }

  before { KafkaBatch::RetryCancel.reset! }
  after  { KafkaBatch::RetryCancel.reset! }

  it "skips a cancelled job_id, emits failed, and acknowledges" do
    id = SecureRandom.uuid
    KafkaBatch.store.create_batch(id: id, total_jobs: 1, on_complete: "RecordingCallback")
    KafkaBatch::RetryCancel.cancel!(["jc1"])

    msg = FakeMessage.new(
      topic: "test.success",
      payload: {
        "job_id" => "jc1",
        "batch_id" => id,
        "batch_seq" => 1,
        "worker_class" => "SuccessfulWorker",
        "job_type" => "successful",
        "payload" => {}
      }
    )
    consumer.send(:process_message, msg)

    expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
    expect(KafkaBatch::RetryCancel.cancelled?("jc1")).to eq(false)
    evt = FakeProducer.for_topic(KafkaBatch.config.events_topic).first
    expect(evt.payload["status"]).to eq("failed")
    expect(evt.payload["job_id"]).to eq("jc1")
  end
end
