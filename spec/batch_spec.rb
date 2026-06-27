RSpec.describe KafkaBatch::Batch do
  describe ".create (block form, auto-locks)" do
    it "persists the batch, produces a message per push, and grows total_jobs" do
      id = described_class.create(on_complete: "RecordingCallback") do |b|
        b.push(SuccessfulWorker, { "user_id" => 1 })
        b.push(SuccessfulWorker, { "user_id" => 2 })
      end

      batch = KafkaBatch.store.find_batch(id)
      expect(batch[:total_jobs]).to eq(2)
      expect(batch[:on_complete]).to eq("RecordingCallback")
      expect(batch[:locked_at]).not_to be_nil

      produced = FakeProducer.for_topic("test.success")
      expect(produced.size).to eq(2)
      expect(produced.map { |m| m.payload["batch_id"] }.uniq).to eq([id])
      expect(produced.first.payload["attempt"]).to eq(0)
    end

    it "fires the callback on lock when the (empty) batch is already complete" do
      id = described_class.create(on_complete: "RecordingCallback") { |_b| }

      cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
      expect(cb.size).to eq(1)
      expect(cb.first.payload["batch_id"]).to eq(id)
      expect(cb.first.payload["outcome"]).to eq("success")
    end
  end

  describe "open / streaming + lock" do
    it "returns an open Batch instance when no block is given" do
      batch = described_class.create(on_complete: "RecordingCallback")
      expect(batch).to be_a(described_class)
      expect(KafkaBatch.store.find_batch(batch.id)[:locked_at]).to be_nil
    end

    it "grows total_jobs as jobs are pushed" do
      batch = described_class.create
      batch.push(SuccessfulWorker, {})
      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(1)
      batch.push(SuccessfulWorker, {})
      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(2)
    end

    it "can be reopened by id with Batch.open to push more jobs" do
      batch = described_class.create
      batch.push(SuccessfulWorker, {})

      described_class.open(batch.id).push(SuccessfulWorker, {})
      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(2)
    end

    it "raises BatchLockedError when pushing after lock" do
      batch = described_class.create
      batch.push(SuccessfulWorker, {})
      batch.lock
      expect { batch.push(SuccessfulWorker, {}) }.to raise_error(KafkaBatch::BatchLockedError)
    end

    describe "#push_many" do
      it "grows total by the batch size in one call and produces each job" do
        batch = described_class.create
        ids = batch.push_many(SuccessfulWorker, [{ "n" => 1 }, { "n" => 2 }, { "n" => 3 }])

        expect(ids.size).to eq(3)
        expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(3)
        produced = FakeProducer.for_topic("test.success")
        expect(produced.size).to eq(3)
        expect(produced.map { |m| m.payload["batch_id"] }.uniq).to eq([batch.id])
      end

      it "is a no-op for an empty array" do
        batch = described_class.create
        expect(batch.push_many(SuccessfulWorker, [])).to eq([])
        expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(0)
      end

      it "raises BatchLockedError after lock" do
        batch = described_class.create
        batch.lock
        expect { batch.push_many(SuccessfulWorker, [{}]) }.to raise_error(KafkaBatch::BatchLockedError)
      end

      it "rolls back the unproduced remainder on produce failure" do
        batch = described_class.create
        FakeProducer.raise_for { |topic| topic == "test.success" }
        expect { batch.push_many(SuccessfulWorker, [{}, {}]) }.to raise_error(KafkaBatch::ProducerError)
        # nothing was produced, so the full count is rolled back
        expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(0)
      end
    end

    it "Batch.open raises BatchNotFoundError for an unknown id" do
      expect { described_class.open("does-not-exist") }.to raise_error(KafkaBatch::BatchNotFoundError)
    end
  end

  describe "callbacks are gated until lock" do
    it "does not finalize/fire while open even when all jobs have finished" do
      batch = described_class.create(on_complete: "RecordingCallback")
      batch.push(SuccessfulWorker, {})

      result = KafkaBatch.store.record_completion_by_offset(
        batch_id: batch.id, source_topic: "test.success", source_partition: 0,
        source_offset: 1, status: "success"
      )
      expect(result[:status]).to eq(:continue)
      expect(KafkaBatch.store.find_batch(batch.id)[:status]).to eq("running")
      expect(FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)).to be_empty

      batch.lock
      cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
      expect(cb.size).to eq(1)
      expect(cb.first.payload["outcome"]).to eq("success")
    end
  end

  describe ".push validation" do
    it "rejects classes that don't include KafkaBatch::Worker" do
      batch = described_class.new
      expect { batch.push(NotAWorker) }.to raise_error(ArgumentError, /must include/)
    end
  end

  describe "produce failure during push" do
    it "rolls back the job count and re-raises" do
      batch = described_class.create
      FakeProducer.raise_for { |topic| topic == "test.success" }

      expect { batch.push(SuccessfulWorker, {}) }.to raise_error(KafkaBatch::ProducerError)
      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(0)
    end
  end

  describe ".enqueue" do
    it "produces a single standalone job with a nil batch_id" do
      job_id = described_class.enqueue(SuccessfulWorker, { "user_id" => 7 })

      produced = FakeProducer.for_topic("test.success")
      expect(produced.size).to eq(1)
      expect(produced.first.payload["batch_id"]).to be_nil
      expect(produced.first.payload["job_id"]).to eq(job_id)
    end
  end

  describe ".reenqueue" do
    it "re-produces the message with the next attempt number" do
      described_class.reenqueue(
        topic:        "test.success",
        message:      { "job_id" => "j1", "attempt" => 1, "payload" => {} },
        next_attempt: 2
      )

      msg = FakeProducer.for_topic("test.success").first
      expect(msg.payload["attempt"]).to eq(2)
      expect(msg.key).to eq("j1")
    end
  end

  describe ".cancel" do
    it "sets the batch status to cancelled" do
      batch = described_class.create
      batch.push(SuccessfulWorker, {})
      described_class.cancel(batch.id)
      expect(KafkaBatch.store.find_batch(batch.id)[:status]).to eq("cancelled")
    end

    it "prevents further pushes after cancel" do
      batch = described_class.create
      described_class.cancel(batch.id)
      expect { batch.push(SuccessfulWorker, {}) }.to raise_error(KafkaBatch::BatchLockedError)
    end
  end
end
