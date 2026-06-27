RSpec.describe KafkaBatch::Consumers::JobConsumer do
  let(:consumer) { build_consumer(described_class) }

  def job_message(worker:, batch_id:, attempt: 0, job_id: "j1", topic: nil, max_retries: nil)
    FakeMessage.new(
      topic:   topic || worker.kafka_topic,
      payload: {
        "job_id"        => job_id,
        "batch_id"      => batch_id,
        "worker_class"  => worker.name,
        "payload"       => { "x" => 1 },
        "attempt"       => attempt,
        "max_retries"   => max_retries || worker.max_retries,
        "retry_backoff" => worker.retry_backoff
      }
    )
  end

  describe "successful job" do
    it "runs the worker and emits a success event for batch jobs" do
      consumer.send(:process_message, job_message(worker: SuccessfulWorker, batch_id: "b1"))

      expect(KafkaBatchSpec::WorkerRuns.runs.map { |r| r[:name] }).to eq([:success])
      events = FakeProducer.for_topic(KafkaBatch.config.events_topic)
      expect(events.size).to eq(1)
      expect(events.first.payload).to include("batch_id" => "b1", "status" => "success")
    end

    it "does not emit an event for a standalone (batch-less) job" do
      consumer.send(:process_message, job_message(worker: SuccessfulWorker, batch_id: nil))

      expect(KafkaBatchSpec::WorkerRuns.runs.size).to eq(1)
      expect(FakeProducer.for_topic(KafkaBatch.config.events_topic)).to be_empty
    end
  end

  describe "failing job with retries remaining" do
    it "schedules a retry on the retry topic with incremented attempt" do
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 0))

      retries = FakeProducer.for_topic(KafkaBatch.config.retry_topic)
      expect(retries.size).to eq(1)
      payload = retries.first.payload
      expect(payload["attempt"]).to eq(1)
      expect(payload["retry_to"]).to eq("test.fail")
      expect(payload["retry_after"]).to be_a(String)
    end
  end

  describe "failing job that has exhausted retries" do
    it "emits a failed event and forwards to the DLT" do
      # max_retries is 2; attempt 2 means no retries remain.
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 2))

      events = FakeProducer.for_topic(KafkaBatch.config.events_topic)
      expect(events.first.payload["status"]).to eq("failed")

      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.first.payload["dlt_type"]).to eq("job")
      expect(FakeProducer.for_topic(KafkaBatch.config.retry_topic)).to be_empty
    end
  end

  describe "malformed payload" do
    it "forwards unparseable JSON to the DLT and does not run a worker" do
      msg = FakeMessage.new(topic: "test.success", payload: "{not json")
      consumer.send(:process_message, msg)

      expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
      expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).size).to eq(1)
    end
  end

  describe "cancellation gate" do
    it "skips the worker and emits no event when the batch is cancelled" do
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 2)
      KafkaBatch::Batch.cancel(id)

      consumer.send(:process_message, job_message(worker: SuccessfulWorker, batch_id: id))

      expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
      expect(FakeProducer.for_topic(KafkaBatch.config.events_topic)).to be_empty
    end

    it "still runs the worker for a non-cancelled batch" do
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 2)

      consumer.send(:process_message, job_message(worker: SuccessfulWorker, batch_id: id))
      expect(KafkaBatchSpec::WorkerRuns.runs.size).to eq(1)
    end

    it "does not run the cancellation check when skip_cancelled_jobs is false" do
      KafkaBatch.config.skip_cancelled_jobs = false
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 2)
      KafkaBatch::Batch.cancel(id)

      consumer.send(:process_message, job_message(worker: SuccessfulWorker, batch_id: id))
      expect(KafkaBatchSpec::WorkerRuns.runs.size).to eq(1)
    end
  end

  describe "in-job batch context (batch.push without passing the id)" do
    it "lets a running job add a child job to its own open batch" do
      batch = KafkaBatch::Batch.create   # open batch
      batch.push(FanoutWorker, { "id" => 1 }) # parent job (total -> 1)

      msg = FakeMessage.new(
        topic:     FanoutWorker.kafka_topic,
        partition: 0, offset: 0,
        payload: {
          "job_id" => "j1", "batch_id" => batch.id, "worker_class" => "FanoutWorker",
          "payload" => { "id" => 1 }, "attempt" => 0
        }
      )
      consumer.send(:process_message, msg)

      # The fanout job ran and pushed a child SuccessfulWorker into the same batch.
      expect(KafkaBatchSpec::WorkerRuns.runs.map { |r| r[:name] }).to include(:fanout)
      child = FakeProducer.for_topic("test.success").last
      expect(child).not_to be_nil
      expect(child.payload["batch_id"]).to eq(batch.id)
      # total grew from 1 (parent) to 2 (parent + child)
      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(2)
    end
  end

  describe "completion event shape" do
    it "emits source coordinates and keys the event by source partition" do
      msg = FakeMessage.new(
        topic:     SuccessfulWorker.kafka_topic,
        partition: 4,
        offset:    99,
        payload:   {
          "job_id" => "j1", "batch_id" => "b1", "worker_class" => "SuccessfulWorker",
          "payload" => {}, "attempt" => 0
        }
      )

      consumer.send(:process_message, msg)

      event = FakeProducer.for_topic(KafkaBatch.config.events_topic).first
      expect(event.payload).to include(
        "src_topic" => "test.success", "src_partition" => 4, "src_offset" => 99
      )
      expect(event.key).to eq("test.success/4")
    end
  end

  describe "configurable event-emission retries (fix #5)" do
    it "retries event emission config.event_emit_retries times, then re-raises" do
      KafkaBatch.config.event_emit_retries = 2
      KafkaBatch.config.event_emit_backoff = 0 # no real sleeping in tests

      attempts = 0
      allow(KafkaBatch::Producer).to receive(:produce_sync) do |topic:, **_|
        if topic == KafkaBatch.config.events_topic
          attempts += 1
          raise KafkaBatch::ProducerError, "events down"
        end
        true
      end

      msg = FakeMessage.new(topic: "test.success", payload: {}, partition: 0, offset: 1)
      expect {
        consumer.send(:emit_event_with_retry, batch_id: "b1", job_id: "j1",
                                              status: "success", worker_class: SuccessfulWorker, message: msg)
      }.to raise_error(KafkaBatch::ProducerError)

      # 1 initial attempt + 2 configured retries
      expect(attempts).to eq(3)
    end

    it "does not attempt emission at all for a standalone (batch-less) job" do
      called = false
      allow(KafkaBatch::Producer).to receive(:produce_sync) { called = true }

      msg = FakeMessage.new(topic: "test.success", payload: {}, partition: 0, offset: 1)
      consumer.send(:emit_event_with_retry, batch_id: nil, job_id: "j1",
                                            status: "success", worker_class: SuccessfulWorker, message: msg)
      expect(called).to be(false)
    end
  end

  describe "unresolvable worker class (poison pill)" do
    def unknown_worker_message(batch_id:)
      FakeMessage.new(
        topic:   "test.success",
        payload: {
          "job_id"       => "j1",
          "batch_id"     => batch_id,
          "worker_class" => "GhostWorker",
          "payload"      => {},
          "attempt"      => 0
        }
      )
    end

    it "routes to the DLT instead of raising (which would block the partition)" do
      expect {
        consumer.send(:process_message, unknown_worker_message(batch_id: "b1"))
      }.not_to raise_error

      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.size).to eq(1)
      expect(dlt.first.payload["worker_class"]).to eq("GhostWorker")
    end

    it "emits a failed event so the batch can still complete" do
      consumer.send(:process_message, unknown_worker_message(batch_id: "b1"))

      events = FakeProducer.for_topic(KafkaBatch.config.events_topic)
      expect(events.size).to eq(1)
      expect(events.first.payload).to include("batch_id" => "b1", "status" => "failed")
    end

    it "does not emit an event for a standalone unresolvable job" do
      consumer.send(:process_message, unknown_worker_message(batch_id: nil))
      expect(FakeProducer.for_topic(KafkaBatch.config.events_topic)).to be_empty
      expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).size).to eq(1)
    end
  end
end
