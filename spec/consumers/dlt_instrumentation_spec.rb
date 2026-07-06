# frozen_string_literal: true

require "spec_helper"

RSpec.describe "DLT instrumentation across consumers" do
  before do
    allow(KafkaBatch::Instrumentation).to receive(:dlt_published)
    KafkaBatchSpec::WorkerRuns.reset!
    FakeProducer.reset!
  end

  def expect_dlt(type, source_topic:)
    expect(KafkaBatch::Instrumentation).to have_received(:dlt_published).with(
      hash_including(dlt_type: type, source_topic: source_topic)
    )
  end

  it "JobConsumer emits dlt.published on exhausted retries" do
    consumer = build_consumer(KafkaBatch::Consumers::JobConsumer)
    msg = FakeMessage.new(
      topic:   FailingWorker.kafka_topic,
      payload: {
        "job_id" => "j1", "batch_id" => "b1", "worker_class" => "FailingWorker",
        "payload" => {}, "attempt" => 2, "max_retries" => 2
      }
    )
    consumer.send(:process_message, msg)
    expect_dlt("job", source_topic: FailingWorker.kafka_topic)
  end

  it "JobConsumer emits dlt.published on malformed JSON" do
    consumer = build_consumer(KafkaBatch::Consumers::JobConsumer)
    consumer.send(:process_message, FakeMessage.new(topic: "test.success", payload: "{bad"))
    expect_dlt("job", source_topic: "test.success")
  end

  it "EventConsumer emits dlt.published on malformed JSON" do
    consumer = build_consumer(KafkaBatch::Consumers::EventConsumer)
    consumer.send(:process_event, FakeMessage.new(topic: KafkaBatch.config.events_topic, payload: "{bad"))
    expect_dlt("malformed_event", source_topic: KafkaBatch.config.events_topic)
  end

  it "CallbackConsumer emits dlt.published on unresolvable callback class" do
    consumer = build_consumer(KafkaBatch::Consumers::CallbackConsumer)
    summary = {
      "batch_id" => "b1", "outcome" => "success", "total_jobs" => 1,
      "completed_count" => 1, "failed_count" => 0,
      "on_success" => "DefinitelyNotARealCallbackClass"
    }
    msg = FakeMessage.new(topic: KafkaBatch.config.callbacks_topic, payload: summary)
    consumer.send(:process_callback, msg)
    expect_dlt("callback", source_topic: KafkaBatch.config.callbacks_topic)
  end

  it "RetryConsumer emits dlt.published on malformed JSON" do
    consumer = build_consumer(KafkaBatch::Consumers::RetryConsumer)
    topic = KafkaBatch.config.retry_topic_for(:short)
    consumer.send(:process_retry, FakeMessage.new(topic: topic, payload: "{bad"))
    expect_dlt("retry_routing", source_topic: topic)
  end

  it "JobExpiry emits dlt.published on expired jobs" do
    KafkaBatch::Batch.enqueue(SuccessfulWorker, { "id" => 1 }, valid_till: 1.minute.ago)
    raw = Oj.dump(FakeProducer.messages.last[:payload])
    consumer = build_consumer(KafkaBatch::Consumers::JobConsumer)
    consumer.send(:process_message, FakeMessage.new(topic: "test.success", partition: 0, offset: 1, payload: raw))
    expect_dlt("expired", source_topic: "test.success")
  end
end
