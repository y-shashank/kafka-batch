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

    it "opts in via fairness_type alone (no fairness true needed)" do
      klass = Class.new do
        include KafkaBatch::Worker
        kafka_topic "test.fair_type_only"
        fairness_type :throughput
      end
      expect(klass.fairness?).to eq(true)
      expect(klass.fairness_type).to eq(:throughput)
    end

    it "still supports legacy fairness true (defaults to :time lane)" do
      klass = Class.new do
        include KafkaBatch::Worker
        kafka_topic "test.legacy_fair"
        fairness true
      end
      expect(klass.fairness?).to eq(true)
      expect(klass.fairness_type).to eq(:time)
    end

    it "returns nil fairness_type for plain workers" do
      expect(SuccessfulWorker.fairness_type).to be_nil
    end

    it "supports a per-worker retry_tier override" do
      expect(TierPinnedWorker.retry_tier).to eq(:large)
    end

    it "falls back to config.jobs_topic when no topic is set" do
      klass = Class.new { include KafkaBatch::Worker }
      expect(klass.kafka_topic).to eq(KafkaBatch.config.jobs_topic)
    end

    it "applies config.topic_prefix to declared kafka_topic names" do
      KafkaBatch.config.topic_prefix = "myapp"
      klass = Class.new do
        include KafkaBatch::Worker
        kafka_topic "kafka_batch.jobs.p0"
      end
      expect(klass.kafka_topic).to eq("myapp.kafka_batch.jobs.p0")
    end

    it "does not double-prefix when the worker already includes the prefix" do
      KafkaBatch.config.topic_prefix = "myapp"
      klass = Class.new do
        include KafkaBatch::Worker
        kafka_topic "myapp.kafka_batch.jobs.p0"
      end
      expect(klass.kafka_topic).to eq("myapp.kafka_batch.jobs.p0")
    end

    it "supports apply_prefix: false for a literal topic override" do
      KafkaBatch.config.topic_prefix = "myapp"
      klass = Class.new do
        include KafkaBatch::Worker
        kafka_topic "legacy.queue", apply_prefix: false
      end
      expect(klass.kafka_topic).to eq("legacy.queue")
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

    describe "retries_exhausted" do
      it "stores a class-level callback block" do
        received = []
        klass = Class.new do
          include KafkaBatch::Worker
          retries_exhausted { |job, error| received << [job, error] }
        end
        expect(klass.retries_exhausted).to be_a(Proc)

        error = RuntimeError.new("boom")
        job = { "job_id" => "j1" }
        klass.retries_exhausted.call(job, error)
        expect(received).to eq([[job, error]])
      end

      it "aliases sidekiq_retries_exhausted for Sidekiq migration" do
        klass = Class.new do
          include KafkaBatch::Worker
          sidekiq_retries_exhausted { |_| }
        end
        expect(klass.retries_exhausted).to be_a(Proc)
      end
    end
  end

  describe ".run_retries_exhausted!" do
    it "builds a job summary hash and invokes the worker block" do
      calls = []
      klass = Class.new do
        include KafkaBatch::Worker
        max_retries 3
        retries_exhausted { |job, error| calls << [job, error] }
      end

      error = RuntimeError.new("boom")
      data = {
        "job_id" => "j1", "batch_id" => "b1", "payload" => { "x" => 1 },
        "attempt" => 3, "tenant_id" => "t1"
      }

      expect(
        KafkaBatch::Worker.run_retries_exhausted!(
          worker_class: klass, data: data, error: error, attempt: 3
        )
      ).to eq(true)

      job, err = calls.first
      expect(job).to include(
        "job_id" => "j1",
        "batch_id" => "b1",
        "payload" => { "x" => 1 },
        "attempt" => 3,
        "max_retries" => 3,
        "worker_class" => klass.to_s,
        "tenant_id" => "t1",
        "error_class" => "RuntimeError",
        "error_message" => "boom"
      )
      expect(err).to eq(error)
    end

    it "returns false when no callback is defined" do
      klass = Class.new { include KafkaBatch::Worker }
      data = { "job_id" => "j1", "payload" => {} }
      error = RuntimeError.new("boom")

      expect(
        KafkaBatch::Worker.run_retries_exhausted!(
          worker_class: klass, data: data, error: error
        )
      ).to eq(false)
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
