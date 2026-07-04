RSpec.describe KafkaBatch::Consumers::JobConsumer do
  let(:consumer) { build_consumer(described_class) }

  def job_message(worker:, batch_id:, attempt: 0, job_id: "j1", topic: nil, max_retries: nil,
                  complete_after_retries: nil, batch_counted: false, offset: 0)
    payload = {
      "job_id"        => job_id,
      "batch_id"      => batch_id,
      "worker_class"  => worker.name,
      "payload"       => { "x" => 1 },
      "attempt"       => attempt,
      "max_retries"   => max_retries || worker.max_retries
    }
    payload["complete_after_retries"] = complete_after_retries unless complete_after_retries.nil?
    payload["batch_counted"]          = batch_counted if batch_counted
    payload["retry_tier"]             = worker.retry_tier.to_s if worker.respond_to?(:retry_tier) && worker.retry_tier
    FakeMessage.new(topic: topic || worker.kafka_topic, offset: offset, payload: payload)
  end

  def events_for(job_id)
    FakeProducer.for_topic(KafkaBatch.config.events_topic).select { |m| m.payload["job_id"] == job_id }
  end

  # Search across all tier retry topics (short/medium/large).
  def retry_for(job_id)
    KafkaBatch.config.retry_topics.flat_map { |t| FakeProducer.for_topic(t) }
              .find { |m| m.payload["job_id"] == job_id }
  end

  def retry_topic_used(job_id)
    KafkaBatch.config.retry_topics.find do |t|
      FakeProducer.for_topic(t).any? { |m| m.payload["job_id"] == job_id }
    end
  end

  describe "early batch completion (complete_after_retries)" do
    it "counts a still-failing job toward the batch after complete_after_retries, and keeps retrying" do
      # max_retries 5, threshold 2 → at attempt 2 it counts (failed) but still retries
      consumer.send(:process_message, job_message(
        worker: FailingWorker, batch_id: "b1", attempt: 2,
        max_retries: 5, complete_after_retries: 2, job_id: "je1"
      ))

      evt = events_for("je1").first
      expect(evt).not_to be_nil
      expect(evt.payload["status"]).to eq("failed")  # counted toward batch

      rmsg = retry_for("je1")
      expect(rmsg).not_to be_nil               # still scheduled to retry
      expect(rmsg.payload["attempt"]).to eq(3)
      expect(rmsg.payload["batch_counted"]).to eq(true)
    end

    it "does not double-count once batch_counted (later retries emit no event)" do
      consumer.send(:process_message, job_message(
        worker: FailingWorker, batch_id: "b1", attempt: 3,
        max_retries: 5, complete_after_retries: 2, batch_counted: true, job_id: "je2"
      ))

      expect(events_for("je2")).to be_empty           # no second count
      expect(retry_for("je2")).not_to be_nil          # but keeps retrying
    end

    it "does not emit the final failed event at exhaustion if already counted" do
      consumer.send(:process_message, job_message(
        worker: FailingWorker, batch_id: "b1", attempt: 5,
        max_retries: 5, complete_after_retries: 2, batch_counted: true, job_id: "je3"
      ))
      expect(events_for("je3")).to be_empty           # exhausted, but already counted
    end

    it "emits no success event for a job already counted that later succeeds" do
      consumer.send(:process_message, job_message(
        worker: SuccessfulWorker, batch_id: "b1", attempt: 4, batch_counted: true, job_id: "je4"
      ))
      expect(events_for("je4")).to be_empty
    end

    it "does not early-count when complete_after_retries >= max_retries (default behaviour)" do
      # FailingWorker: max_retries 2, threshold default 3 → never early; still retrying at attempt 1
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 1, job_id: "je5"))
      expect(events_for("je5")).to be_empty           # no completion event yet
      expect(retry_for("je5")).not_to be_nil
    end
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
    it "schedules a retry on the short tier topic with incremented attempt" do
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 0))

      retries = FakeProducer.for_topic(KafkaBatch.config.retry_topic_for(:short))
      expect(retries.size).to eq(1)
      payload = retries.first.payload
      expect(payload["attempt"]).to eq(1)
      expect(payload["retry_to"]).to eq("test.fail")
      expect(payload["retry_after"]).to be_a(String)
    end

    it "walks the tier progression: 1st→short, 2nd→medium, 3rd→large" do
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 0, max_retries: 5, job_id: "p1"))
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 1, max_retries: 5, job_id: "p2"))
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 2, max_retries: 5, job_id: "p3"))

      expect(retry_topic_used("p1")).to eq(KafkaBatch.config.retry_topic_for(:short))
      expect(retry_topic_used("p2")).to eq(KafkaBatch.config.retry_topic_for(:medium))
      expect(retry_topic_used("p3")).to eq(KafkaBatch.config.retry_topic_for(:large))
    end

    it "pins all retries to the worker's retry_tier override" do
      consumer.send(:process_message, job_message(worker: TierPinnedWorker, batch_id: "b1", attempt: 0, job_id: "tp1", topic: "test.tier_pinned"))
      msg = retry_for("tp1")
      expect(msg.payload["retry_tier"]).to eq("large")
      expect(retry_topic_used("tp1")).to eq(KafkaBatch.config.retry_topic_for(:large))
    end

    it "records the failure as 'retrying' on the first failed attempt" do
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 0, job_id: "jr"))

      f = KafkaBatch.store.list_failures("b1").first
      expect(f).not_to be_nil
      expect(f[:status]).to eq("retrying")
      expect(f[:job_id]).to eq("jr")
      expect(f[:error_class]).to eq("RuntimeError")
      expect(f[:next_retry_at]).not_to be_nil  # tiered retry schedule
    end

    it "schedules the first retry within the short tier (~30s)" do
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 0, job_id: "jr2"))
      msg = retry_for("jr2")
      retry_after = Time.parse(msg.payload["retry_after"])
      expect(retry_after).to be > Time.now
      expect(retry_after).to be < Time.now + 60  # ~30s, not minutes/hours
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
      expect(KafkaBatch.config.retry_topics.flat_map { |t| FakeProducer.for_topic(t) }).to be_empty
    end

    it "records the failure for the batch (always-on failure tracking)" do
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 2, job_id: "jx"))

      failures = KafkaBatch.store.list_failures("b1")
      expect(failures.size).to eq(1)
      expect(failures.first[:job_id]).to eq("jx")
      expect(failures.first[:error_class]).to eq("RuntimeError")
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

  describe "clearing failures on a successful retry" do
    it "removes a prior 'retrying' failure record once the job succeeds" do
      KafkaBatch.store.record_failure(
        batch_id: "b1", job_id: "jx", worker_class: "SuccessfulWorker",
        error_class: "RuntimeError", error_message: "transient", attempt: 0, status: "retrying"
      )
      expect(KafkaBatch.store.list_failures("b1").size).to eq(1)

      # re-run (attempt 1) succeeds → failure record should be cleared
      consumer.send(:process_message, job_message(worker: SuccessfulWorker, batch_id: "b1", attempt: 1, job_id: "jx"))

      expect(KafkaBatch.store.list_failures("b1")).to be_empty
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

  # ── Fair-lane in-flight slot release (Scheduler#complete) ──────────────────
  describe "fair-lane slot release" do
    let(:scheduler) { instance_double(KafkaBatch::Fairness::Scheduler) }

    before do
      allow(KafkaBatch).to receive(:scheduler).and_return(scheduler)
      allow(scheduler).to receive(:complete)
    end

    def fair_message(worker:, tenant:, batch_id: nil, attempt: 0, job_id: "j1")
      FakeMessage.new(
        topic:   KafkaBatch.config.fairness_ready_topic(:time),
        offset:  1,
        payload: {
          "job_id"       => job_id,
          "batch_id"     => batch_id,
          "worker_class" => worker.name,
          "payload"      => {},
          "attempt"      => attempt,
          "max_retries"  => worker.max_retries,
          "tenant_id"    => tenant,
          "_fair_slot"   => true
        }
      )
    end

    it "releases the tenant's slot with a duration after a successful fair job" do
      consumer.send(:process_message, fair_message(worker: SuccessfulWorker, tenant: "acme"))
      expect(scheduler).to have_received(:complete).with("acme", hash_including(:duration)).once
    end

    it "releases the slot when a fair job fails and is scheduled for retry" do
      consumer.send(:process_message, fair_message(worker: FailingWorker, tenant: "globex", attempt: 0))
      expect(scheduler).to have_received(:complete).with("globex", hash_including(:duration)).once
    end

    it "strips _fair_slot from the retry message so it is not released twice" do
      consumer.send(:process_message, fair_message(worker: FailingWorker, tenant: "globex", job_id: "jx"))
      retried = retry_for("jx")
      expect(retried).not_to be_nil
      expect(retried.payload).not_to have_key("_fair_slot")
    end

    it "does NOT call complete for a plain (non-fair) message" do
      consumer.send(:process_message, job_message(worker: SuccessfulWorker, batch_id: "b1"))
      expect(scheduler).not_to have_received(:complete)
    end
  end
end
