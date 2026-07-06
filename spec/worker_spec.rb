RSpec.describe KafkaBatch::Worker do
  describe "class configuration" do
    it "exposes the configured topic" do
      expect(SuccessfulWorker.kafka_topic).to eq("test.success")
    end

    it "supports a per-worker max_retries override" do
      expect(FailingWorker.max_retries).to eq(2)
    end

    it "falls back to the global max_retries default when not overridden" do
      KafkaBatch.config.max_retries = 9
      expect(SuccessfulWorker.max_retries).to eq(9)
    end

    it "defaults retry_tier to nil (uses the progression)" do
      expect(SuccessfulWorker.retry_tier).to be_nil
    end

    it "defaults fairness to false" do
      expect(SuccessfulWorker.fairness?).to eq(false)
    end

    it "supports a per-worker fairness opt-in" do
      expect(FairWorker.fairness?).to eq(true)
    end

    it "supports a per-worker retry_tier override" do
      expect(TierPinnedWorker.retry_tier).to eq(:large)
    end

    it "falls back to config.jobs_topic when no topic is set" do
      klass = Class.new { include KafkaBatch::Worker }
      expect(klass.kafka_topic).to eq(KafkaBatch.config.jobs_topic)
    end

    describe "complete_after_retries" do
      it "falls back to the global config default when not overridden" do
        KafkaBatch.config.complete_after_retries = 7
        klass = Class.new { include KafkaBatch::Worker }
        expect(klass.complete_after_retries).to eq(7)
      end

      it "can be pinned per worker, independently of max_retries" do
        klass = Class.new do
          include KafkaBatch::Worker
          complete_after_retries 1
        end
        expect(klass.complete_after_retries).to eq(1)
      end
    end
  end

  it "registers including classes in the global registry" do
    klass = Class.new { include KafkaBatch::Worker }
    expect(KafkaBatch.workers).to include(klass)
  end

  it "raises NotImplementedError when #perform is not overridden" do
    klass = Class.new { include KafkaBatch::Worker }
    expect { klass.new.perform({}) }.to raise_error(NotImplementedError)
  end

  # ── Instance helpers (job context / batch) ────────────────────────────────
  describe "job context" do
    it "exposes job_id, batch_id, retry_count, and uniq_hex after bind_job_context!" do
      worker = UniqWorker.new
      worker.bind_job_context!(
        {
          "job_id"  => "jid-1",
          "batch_id"=> "bid-1",
          "attempt" => 2,
          "payload" => { "k" => "v" }
        },
        worker_class: UniqWorker
      )

      expect(worker.job_id).to eq("jid-1")
      expect(worker.batch_id).to eq("bid-1")
      expect(worker.kafka_batch_id).to eq("bid-1")
      expect(worker.retry_count).to eq(2)
      expect(worker.uniq_hex).to eq(KafkaBatch::Uniqueness.digest_hex(UniqWorker, { "k" => "v" }))
    end

    it "sets uniq_hex to nil for workers without uniq true" do
      worker = SuccessfulWorker.new
      worker.bind_job_context!(
        { "job_id" => "j", "batch_id" => "b", "attempt" => 0, "payload" => {} },
        worker_class: SuccessfulWorker
      )
      expect(worker.uniq_hex).to be_nil
    end
  end

  describe "#batch instance helper" do
    it "returns nil when batch_id is nil" do
      worker = SuccessfulWorker.new
      worker.bind_job_context!({ "job_id" => "j", "batch_id" => nil, "attempt" => 0, "payload" => {} })
      expect(worker.batch).to be_nil
    end

    it "returns nil when batch_id is an empty string" do
      worker = SuccessfulWorker.new
      worker.bind_job_context!({ "job_id" => "j", "batch_id" => "", "attempt" => 0, "payload" => {} })
      expect(worker.batch).to be_nil
    end

    it "returns an opened Batch with the correct id when batch_id is set" do
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 1, tenant_id: "acme")

      worker = SuccessfulWorker.new
      worker.bind_job_context!({ "job_id" => "j", "batch_id" => id, "attempt" => 0, "payload" => {} })
      b = worker.batch
      expect(b).to be_a(KafkaBatch::Batch)
      expect(b.id).to eq(id)
      expect(b.instance_variable_get(:@tenant_id)).to eq("acme")
    end

    it "memoizes the Batch on repeated calls (no extra store round-trips)" do
      id = SecureRandom.uuid
      KafkaBatch.store.create_batch(id: id, total_jobs: 1)

      worker = SuccessfulWorker.new
      worker.bind_job_context!({ "job_id" => "j", "batch_id" => id, "attempt" => 0, "payload" => {} })
      expect(worker.batch).to equal(worker.batch)  # same object identity
    end
  end
end
