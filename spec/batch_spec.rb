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
      expect(produced.map { |m| m.payload["batch_seq"] }).to eq([1, 2])
    end

    it "holds the completion gate shut DURING the block, then fires on seal" do
      observed = nil
      batch = described_class.create(on_complete: "RecordingCallback") do |b|
        b.push(SuccessfulWorker, {})
        # All jobs finish while we're still populating: must NOT finalize yet.
        observed = KafkaBatch.store.record_completion_by_offset(
          batch_id: b.id, source_topic: "test.success", source_partition: 0,
          job_id: "j1", batch_seq: 1, source_offset: 1, status: "success"
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

    it "seals the batch even when the block raises so pushed jobs can still finalize" do
      batch = nil
      expect {
        described_class.create(on_complete: "RecordingCallback") do |b|
          batch = b
          b.push(SuccessfulWorker, { "id" => 1 })
          raise "population failed"
        end
      }.to raise_error(RuntimeError, "population failed")

      expect(KafkaBatch.store.find_batch(batch.id)[:locked_at]).not_to be_nil
      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(1)
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
        job_id: "j1", batch_seq: 1, source_offset: 1, status: "success"
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
        expect(produced.map { |m| m.payload["batch_seq"] }).to eq([1, 2, 3])
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

  describe "fairness routing (per-worker)" do
    it "routes a fair worker's pushes to the ingest topic keyed by tenant" do
      batch = described_class.create(tenant_id: "acme")
      batch.push(FairWorker, { "x" => 1 })

      ingest = FakeProducer.for_topic(KafkaBatch.config.fairness_ingest_topic(:time))
      expect(ingest.size).to eq(1)
      expect(ingest.first.key).to eq("acme")
      expect(ingest.first.payload["tenant_id"]).to eq("acme")
      expect(FakeProducer.for_topic("test.fair")).to be_empty  # not the worker topic
    end

    it "produces a plain worker to its own topic (not ingest)" do
      batch = described_class.create
      batch.push(SuccessfulWorker, { "x" => 1 })

      expect(FakeProducer.for_topic("test.success").size).to eq(1)
      expect(FakeProducer.for_topic(KafkaBatch.config.fairness_ingest_topic(:time))).to be_empty
    end

    it "lets fair and plain workers coexist in one batch" do
      batch = described_class.create(tenant_id: "acme")
      batch.push(FairWorker, { "x" => 1 })
      batch.push(SuccessfulWorker, { "y" => 2 })

      expect(FakeProducer.for_topic(KafkaBatch.config.fairness_ingest_topic(:time)).size).to eq(1)
      expect(FakeProducer.for_topic("test.success").size).to eq(1)
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

  # ── Meta and callback payload propagation ─────────────────────────────────
  describe "meta hash and callback payload" do
    it "persists the meta hash and makes it findable" do
      meta  = { "report_type" => "nightly", "requester" => "ops@example.com" }
      batch = described_class.create(meta: meta)
      expect(KafkaBatch.store.find_batch(batch.id)[:meta]).to include(meta)
    end

    it "carries meta into the callback payload produced at seal time" do
      # Complete the job INSIDE the block so the batch is drained when seal!
      # runs on block return — that's when Batch#produce_callback fires.
      # Completing after seal! does NOT auto-produce a callback (the EventConsumer
      # pipeline handles that path; the Batch object handles the block-form path).
      meta  = { "run_id" => "42" }
      described_class.create(on_complete: "RecordingCallback", meta: meta) do |b|
        b.push(SuccessfulWorker, {})
        KafkaBatch.store.record_completion_by_offset(
          batch_id: b.id, source_topic: "test.success",
          source_partition: 0, job_id: "j1", batch_seq: 1, source_offset: 1, status: "success"
        )
      end
      # seal! fires now that the batch is drained; callback produced above.

      cb = FakeProducer.for_topic(KafkaBatch.config.callbacks_topic)
      expect(cb.last).not_to be_nil
      expect(cb.last.payload["meta"]).to include("run_id" => "42")
    end

    it "stores tenant_id on the batch record" do
      batch = described_class.create(tenant_id: "acme-corp")
      expect(KafkaBatch.store.find_batch(batch.id)[:tenant_id]).to eq("acme-corp")
    end
  end

  # ── push tenant_id override ───────────────────────────────────────────────
  describe "push with per-job tenant_id" do
    it "uses batch-level tenant_id for fair workers when no per-job override is given" do
      batch = described_class.create(tenant_id: "batch-tenant")
      batch.push(FairWorker, {})

      msg = FakeProducer.for_topic(KafkaBatch.config.fairness_ingest_topic(:time)).first
      expect(msg.key).to eq("batch-tenant")
      expect(msg.payload["tenant_id"]).to eq("batch-tenant")
    end

    it "embeds tenant_id in every job message on the fair lane" do
      batch = described_class.create(tenant_id: "acme")
      batch.push_many(FairWorker, [{}, {}])

      msgs = FakeProducer.for_topic(KafkaBatch.config.fairness_ingest_topic(:time))
      expect(msgs.size).to eq(2)
      expect(msgs.map { |m| m.payload["tenant_id"] }.uniq).to eq(["acme"])
    end
  end
end
