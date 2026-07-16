RSpec.describe KafkaBatch::Consumers::JobConsumer do
  let(:consumer) { build_consumer(described_class) }

  def job_message(worker:, batch_id:, attempt: 0, job_id: "j1", topic: nil, max_retries: nil,
                  batch_counted: false, offset: 0, job_type: nil)
    payload = {
      "job_id"        => job_id,
      "batch_id"      => batch_id,
      "job_type"      => job_type || worker.job_type,
      "worker_class"  => worker.name,
      "payload"       => { "x" => 1 },
      "attempt"       => attempt,
      "max_retries"   => max_retries || worker.max_retries
    }
    payload["batch_counted"] = batch_counted if batch_counted
    payload["retry_tier"]    = worker.retry_tier.to_s if worker.respond_to?(:retry_tier) && worker.retry_tier
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

  describe "first-touch batch completion (Sidekiq-style)" do
    it "emits executed on first fail-with-retry and keeps retrying" do
      consumer.send(:process_message, job_message(
        worker: FailingWorker, batch_id: "b1", attempt: 0,
        max_retries: 5, job_id: "je1"
      ))

      evt = events_for("je1").first
      expect(evt).not_to be_nil
      expect(evt.payload["status"]).to eq("executed")

      rmsg = retry_for("je1")
      expect(rmsg).not_to be_nil
      expect(rmsg.payload["attempt"]).to eq(1)
      expect(rmsg.payload["batch_counted"]).to eq(true)
    end

    it "does not re-emit executed once batch_counted (later retries emit no event)" do
      consumer.send(:process_message, job_message(
        worker: FailingWorker, batch_id: "b1", attempt: 3,
        max_retries: 5, batch_counted: true, job_id: "je2"
      ))

      expect(events_for("je2")).to be_empty
      expect(retry_for("je2")).not_to be_nil
    end

    it "emits failed at exhaustion even if already touched via executed" do
      consumer.send(:process_message, job_message(
        worker: FailingWorker, batch_id: "b1", attempt: 5,
        max_retries: 5, batch_counted: true, job_id: "je3"
      ))
      expect(events_for("je3").map { |e| e.payload["status"] }).to eq(["failed"])
    end

    it "emits success for a job already touched that later succeeds" do
      consumer.send(:process_message, job_message(
        worker: SuccessfulWorker, batch_id: "b1", attempt: 4, batch_counted: true, job_id: "je4"
      ))
      expect(events_for("je4").map { |e| e.payload["status"] }).to eq(["success"])
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

    it "does not cache retrying failures in Redis (listed from Kafka retry topics)" do
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 0, job_id: "jr"))

      expect(KafkaBatch.store.list_failures("b1")).to be_empty
      msg = retry_for("jr")
      expect(msg).not_to be_nil
      expect(msg.payload["retry_after"]).not_to be_nil
    end

    it "schedules the first retry within the short tier (~30s)" do
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 0, job_id: "jr2"))
      msg = retry_for("jr2")
      retry_after = Time.parse(msg.payload["retry_after"])
      expect(retry_after).to be > Time.now
      expect(retry_after).to be < Time.now + 60  # ~30s, not minutes/hours
    end
  end

  describe "handler registry (job_type on wire)" do
    it "runs the worker after HandlerRegistry.reset! when job_type and worker_class are on the message" do
      KafkaBatch::HandlerRegistry.reset!
      consumer.send(:process_message, job_message(worker: SuccessfulWorker, batch_id: "b1", job_id: "reg1"))

      expect(KafkaBatchSpec::WorkerRuns.runs.last).to include(name: :success)
      expect(KafkaBatch::HandlerRegistry.registered?("successful")).to eq(true)
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

    it "does not persist failure metadata in Redis on exhaustion (DLT is the record)" do
      consumer.send(:process_message, job_message(worker: FailingWorker, batch_id: "b1", attempt: 2, job_id: "jx"))

      expect(KafkaBatch.store.list_failures("b1")).to be_empty

      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.first.payload["job_id"]).to eq("jx")
      expect(dlt.first.payload["dlt_error_class"]).to eq("RuntimeError")
    end

    it "invokes the worker retries_exhausted callback before forwarding to the DLT" do
      consumer.send(:process_message, job_message(worker: RetriesExhaustedWorker, batch_id: "b1", attempt: 2, job_id: "re1"))

      run = KafkaBatchSpec::WorkerRuns.runs.find { |r| r[:name] == :retries_exhausted }
      expect(run).not_to be_nil
      expect(run[:payload][:job]).to include(
        "job_id" => "re1",
        "batch_id" => "b1",
        "worker_class" => "RetriesExhaustedWorker",
        "attempt" => 2,
        "error_class" => "RuntimeError"
      )
      expect(run[:payload][:error_class]).to eq("RuntimeError")

      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.first.payload["job_id"]).to eq("re1")
    end

    it "still forwards to the DLT when retries_exhausted raises" do
      consumer.send(:process_message, job_message(worker: RetriesExhaustedRaisingWorker, batch_id: "b1", attempt: 2, job_id: "rer1"))

      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.first.payload["job_id"]).to eq("rer1")
    end
  end

  describe "malformed payload" do
    it "forwards unparseable JSON to the DLT and does not run a worker" do
      msg = FakeMessage.new(topic: "test.success", payload: "{not json")
      consumer.send(:process_message, msg)

      expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
      expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).size).to eq(1)
    end

    it "forwards a nil/tombstone payload to the DLT instead of stalling the partition" do
      msg = FakeMessage.new(topic: "test.success", payload: nil)
      expect { consumer.send(:process_message, msg) }.not_to raise_error

      expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
      expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).size).to eq(1)
      expect(consumer).to have_received(:mark_as_consumed!).with(msg)
    end

    it "forwards a non-object JSON literal to the DLT" do
      msg = FakeMessage.new(topic: "test.success", payload: "12345")
      expect { consumer.send(:process_message, msg) }.not_to raise_error
      expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic).size).to eq(1)
    end
  end

  describe "Exception backstop (poison pill that is not a StandardError)" do
    it "routes a non-StandardError raised by the worker to the DLT and commits the offset" do
      msg = job_message(worker: PoisonWorker, batch_id: "b1", job_id: "poison1", topic: "test.poison")
      expect { consumer.send(:process_message, msg) }.not_to raise_error

      dlt = FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)
      expect(dlt.size).to eq(1)
      expect(dlt.first.payload["job_id"]).to eq("poison1")
      expect(dlt.first.payload["dlt_error_class"]).to eq("PoisonError")
      expect(consumer).to have_received(:mark_as_consumed!).with(msg)
    end

    it "advances the batch (failed event) so a poison job never wedges completion" do
      msg = job_message(worker: PoisonWorker, batch_id: "b1", job_id: "poison2", topic: "test.poison")
      consumer.send(:process_message, msg)

      evt = events_for("poison2").first
      expect(evt).not_to be_nil
      expect(evt.payload["status"]).to eq("failed")
    end

    it "re-raises shutdown signals instead of committing (offset left for redelivery)" do
      msg = job_message(worker: SuccessfulWorker, batch_id: "b1", job_id: "sig1")
      allow(consumer).to receive(:process_message!).and_raise(Interrupt.new("SIGINT"))
      expect { consumer.send(:process_message, msg) }.to raise_error(Interrupt)
      expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)).to be_empty
    end

    it "re-raises ProducerError (transient Kafka) so the message redelivers, not DLT'd" do
      msg = job_message(worker: SuccessfulWorker, batch_id: "b1", job_id: "prod1")
      allow(consumer).to receive(:process_message!).and_raise(KafkaBatch::ProducerError.new("broker down"))
      expect { consumer.send(:process_message, msg) }.to raise_error(KafkaBatch::ProducerError)
      expect(FakeProducer.for_topic(KafkaBatch.config.dead_letter_topic)).to be_empty
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

  describe "#record_failure / #clear_failure are Redis no-ops" do
    it "never populate the store, and a subsequent successful retry still runs cleanly" do
      expect {
        KafkaBatch.store.record_failure(
          batch_id: "b1", job_id: "jx", worker_class: "SuccessfulWorker",
          error_class: "RuntimeError", error_message: "transient", attempt: 0, status: "retrying"
        )
      }.not_to raise_error
      expect(KafkaBatch.store.list_failures("b1")).to be_empty

      # re-run (attempt 1) succeeds regardless
      consumer.send(:process_message, job_message(worker: SuccessfulWorker, batch_id: "b1", attempt: 1, job_id: "jx"))

      expect(KafkaBatchSpec::WorkerRuns.runs.last).to include(name: :success)
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

    it "binds job_id, batch_id, retry_count on the worker instance" do
      batch = KafkaBatch::Batch.create
      batch.push(ContextProbeWorker, {})

      msg = FakeMessage.new(
        topic: ContextProbeWorker.kafka_topic, partition: 0, offset: 1,
        payload: {
          "job_id" => "probe-j", "batch_id" => batch.id,
          "worker_class" => "ContextProbeWorker",
          "payload" => {}, "attempt" => 1
        }
      )
      consumer.send(:process_message, msg)

      probe = KafkaBatchSpec::WorkerRuns.runs.last
      expect(probe[:name]).to eq(:context_probe)
      expect(probe[:payload][:job_id]).to eq("probe-j")
      expect(probe[:payload][:batch_id]).to eq(batch.id)
      expect(probe[:payload][:retry_count]).to eq(1)
      expect(probe[:payload][:batch_open]).to be(true)
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
      emit_retries = []
      allow(KafkaBatch::Instrumentation).to receive(:job_emit_retried) do |**kw|
        emit_retries << kw
      end
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
      expect(emit_retries.map { |e| e[:attempt] }).to eq([1, 2])
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
      allow(scheduler).to receive(:claim_slot_execution!).and_return(true)
      allow(scheduler).to receive(:lease_ttl).and_return(1800.0)
      allow(scheduler).to receive(:renew_lease)
    end

    def fair_message(worker:, tenant:, batch_id: nil, attempt: 0, job_id: "j1", slot_id: "lease-1")
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
          "_fair_slot"   => true,
          "_fair_slot_id"=> slot_id
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

    it "skips duplicate fair-slot deliveries without running perform" do
      allow(scheduler).to receive(:claim_slot_execution!).and_return(false)
      consumer.send(:process_message, fair_message(worker: SuccessfulWorker, tenant: "acme", slot_id: "dup-slot"))
      expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
      expect(scheduler).not_to have_received(:complete)
    end

    it "releases the fair slot when an expired ready message is dropped" do
      till = 1.minute.ago.iso8601
      msg = FakeMessage.new(
        topic:   KafkaBatch.config.fairness_ready_topic(:time),
        offset:  2,
        payload: {
          "job_id"       => "j-exp",
          "worker_class" => SuccessfulWorker.name,
          "payload"      => {},
          "tenant_id"    => "acme",
          "_fair_slot"   => true,
          "_fair_slot_id"=> "lease-1",
          "valid_till"   => till
        }
      )
      consumer.send(:process_message, msg)

      expect(scheduler).to have_received(:complete).with(
        "acme", hash_including(slot_id: "lease-1", duration: 0.0)
      ).once
      expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
    end
  end
end
