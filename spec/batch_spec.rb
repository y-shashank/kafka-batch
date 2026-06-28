RSpec.describe KafkaBatch::Batch do
  describe ".create (block form)" do
    it "persists the batch, produces a message per push, and grows total_jobs" do
      batch = described_class.create(on_complete: "RecordingCallback") do |b|
        b.push(SuccessfulWorker, { "user_id" => 1 })
        b.push(SuccessfulWorker, { "user_id" => 2 })
      end

      data = KafkaBatch.store.find_batch(batch.id)
      expect(data[:total_jobs]).to eq(2)
      expect(data[:on_complete]).to eq("RecordingCallback")
      expect(data[:locked_at]).not_to be_nil  # sealed when the block returned

      produced = FakeProducer.for_topic("test.success")
      expect(produced.size).to eq(2)
      expect(produced.map { |m| m.payload["batch_id"] }.uniq).to eq([batch.id])
      expect(produced.first.payload["attempt"]).to eq(0)
    end

    it "holds the completion gate shut DURING the block, then fires on seal" do
      observed = nil
      batch = described_class.create(on_complete: "RecordingCallback") do |b|
        b.push(SuccessfulWorker, {})
        # All jobs finish while we're still populating: must NOT finalize yet.
        observed = KafkaBatch.store.record_completion_by_offset(
          batch_id: b.id, source_topic: "test.success", source_partition: 0,
          source_offset: 1, status: "success"
        )
      end

      expect(observed[:status]).to eq(:continue)
      cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
      expect(cb.size).to eq(1)  # fired once, on seal (block return)
      expect(cb.first.payload["batch_id"]).to eq(batch.id)
      expect(cb.first.payload["outcome"]).to eq("success")
    end

    it "persists an optional description" do
      batch = described_class.create(description: "Nightly report run") do |b|
        b.push(SuccessfulWorker, {})
      end
      expect(KafkaBatch.store.find_batch(batch.id)[:description]).to eq("Nightly report run")
    end

    it "fires the callback on seal when the (empty) batch is already complete" do
      batch = described_class.create(on_complete: "RecordingCallback") { |_b| }

      cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
      expect(cb.size).to eq(1)
      expect(cb.first.payload["batch_id"]).to eq(batch.id)
      expect(cb.first.payload["outcome"]).to eq("success")
    end
  end

  describe "open / streaming (no lock step)" do
    it "returns a Batch sealed immediately when no block is given" do
      batch = described_class.create(on_complete: "RecordingCallback")
      expect(batch).to be_a(described_class)
      expect(KafkaBatch.store.find_batch(batch.id)[:locked_at]).not_to be_nil
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

    it "finalizes on drain and then rejects further pushes with BatchClosedError" do
      batch = described_class.create(on_complete: "RecordingCallback")
      batch.push(SuccessfulWorker, {})

      result = KafkaBatch.store.record_completion_by_offset(
        batch_id: batch.id, source_topic: "test.success", source_partition: 0,
        source_offset: 1, status: "success"
      )
      expect(result[:status]).to eq(:done)
      expect(KafkaBatch.store.find_batch(batch.id)[:status]).to eq("success")

      expect { batch.push(SuccessfulWorker, {}) }.to raise_error(KafkaBatch::BatchClosedError)
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

  describe "fairness routing" do
    it "routes pushes to the ingest topic keyed by tenant when fairness is enabled" do
      KafkaBatch.config.fairness_enabled = true
      batch = described_class.create(tenant_id: "acme")
      batch.push(SuccessfulWorker, { "x" => 1 })

      ingest = FakeProducer.for_topic(KafkaBatch.config.fairness_ingest_topic)
      expect(ingest.size).to eq(1)
      expect(ingest.first.key).to eq("acme")
      expect(ingest.first.payload["tenant_id"]).to eq("acme")
      expect(FakeProducer.for_topic("test.success")).to be_empty  # not the worker topic
    end

    it "produces to the worker topic (not ingest) when fairness is disabled" do
      batch = described_class.create
      batch.push(SuccessfulWorker, { "x" => 1 })

      expect(FakeProducer.for_topic("test.success").size).to eq(1)
      expect(FakeProducer.for_topic(KafkaBatch.config.fairness_ingest_topic)).to be_empty
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
      expect { batch.push(SuccessfulWorker, {}) }.to raise_error(KafkaBatch::BatchClosedError)
    end
  end
end
