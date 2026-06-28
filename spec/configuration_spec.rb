RSpec.describe KafkaBatch::Configuration do
  subject(:config) { described_class.new }

  describe "defaults" do
    it "defaults to the mysql store" do
      expect(config.store).to eq(:mysql)
    end

    it "ships sane topic + retry defaults" do
      expect(config.jobs_topic).to eq("kafka_batch.jobs")
      expect(config.retry_topic).to eq("kafka_batch.jobs.retry")
      expect(config.max_retries).to eq(3)
      expect(config.retry_first_delay).to eq(10)
      expect(config.retry_delay).to eq(180)
      expect(config.retry_jitter).to eq(0.1)
      expect(config.complete_after_retries).to eq(3)
    end

    it "decouples the reconciler lock TTL from the staleness threshold" do
      expect(config.reconciliation_interval).to eq(300)
      expect(config.reconciler_lock_ttl).to eq(600)
    end

    it "exposes configurable event-emission retry knobs" do
      expect(config.event_emit_retries).to eq(3)
      expect(config.event_emit_backoff).to eq(2)
    end

    it "bounds failure-metadata retention separately from batch_ttl" do
      expect(config.batch_ttl).to eq(7 * 24 * 3600)
      expect(config.failures_ttl).to eq(24 * 3600)       # shorter than batch_ttl
      expect(config.max_failures_per_batch).to eq(1000)
    end

    it "ships fairness disabled with sane WFQ defaults" do
      expect(config.fairness_enabled).to eq(false)
      expect(config.fairness_global_concurrency).to eq(50)
      expect(config.fairness_max_inflight_per_tenant).to eq(0)
      expect(config.fairness_ready_window).to eq(500)
      expect(config.fairness_default_weight).to eq(1.0)
      expect(config.fairness_ingest_topic).to eq("kafka_batch.ingest")
      expect(config.fairness_ready_topic).to eq("kafka_batch.ready")
      expect(config.fairness_ready_lag_high).to eq(5000)
      expect(config.fairness_ready_lag_low).to eq(1000)
      expect(config.fairness_min_ingest_partitions).to eq(2)
    end
  end

  describe "#validate!" do
    it "passes with valid mysql config" do
      config.store   = :mysql
      config.brokers = ["localhost:9092"]
      expect { config.validate! }.not_to raise_error
    end

    it "rejects an unknown store" do
      config.store = :cassandra
      expect { config.validate! }.to raise_error(KafkaBatch::ConfigurationError, /mysql or :redis/)
    end

    it "rejects empty brokers" do
      config.brokers = []
      expect { config.validate! }.to raise_error(KafkaBatch::ConfigurationError, /brokers/)
    end

    it "requires a redis_url for the redis store" do
      config.store     = :redis
      config.redis_url = ""
      expect { config.validate! }.to raise_error(KafkaBatch::ConfigurationError, /redis_url/)
    end
  end
end
