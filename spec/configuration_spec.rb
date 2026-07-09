RSpec.describe KafkaBatch::Configuration do
  subject(:config) { described_class.new }

  describe "defaults" do
    it "defaults to the redis store" do
      expect(config.store).to eq(:redis)
    end

    it "ships sane topic + retry defaults" do
      expect(config.jobs_topic).to eq("kafka_batch.jobs")
      expect(config.retry_topic).to eq("kafka_batch.jobs.retry")
      expect(config.max_retries).to eq(3)
      expect(config.retry_jitter).to eq(0.1)
      expect(config.complete_after_retries).to eq(3)
    end

    it "ships tiered retry delays (short/medium/large)" do
      expect(config.retry_tiers).to eq(short: 30, medium: 420, large: 1200)
      expect(config.retry_tier_progression).to eq(%i[short medium large])
    end

    it "derives a retry topic per tier" do
      expect(config.retry_topic_for(:short)).to eq("kafka_batch.jobs.retry.short")
      expect(config.retry_topic_for(:medium)).to eq("kafka_batch.jobs.retry.medium")
      expect(config.retry_topic_for(:large)).to eq("kafka_batch.jobs.retry.large")
      expect(config.retry_topics).to eq(%w[
        kafka_batch.jobs.retry.short
        kafka_batch.jobs.retry.medium
        kafka_batch.jobs.retry.large
      ])
    end

    it "walks the progression by retry index, clamping to the last tier" do
      expect(config.retry_tier_for(1)).to eq(:short)
      expect(config.retry_tier_for(2)).to eq(:medium)
      expect(config.retry_tier_for(3)).to eq(:large)
      expect(config.retry_tier_for(4)).to eq(:large)
      expect(config.retry_tier_for(99)).to eq(:large)
    end

    it "honours a valid worker tier override regardless of attempt" do
      expect(config.retry_tier_for(1, :large)).to eq(:large)
      expect(config.retry_tier_for(5, "short")).to eq(:short)
    end

    it "ignores an unknown worker tier and falls back to the progression" do
      expect(config.retry_tier_for(2, :bogus)).to eq(:medium)
    end

    it "applies the tier delay (no jitter) when retry_jitter is 0" do
      config.retry_jitter = 0
      expect(config.retry_delay_for(:short)).to eq(30.0)
      expect(config.retry_delay_for(:medium)).to eq(420.0)
      expect(config.retry_delay_for(:large)).to eq(1200.0)
    end

    it "keeps the tier delay within the jitter band" do
      config.retry_jitter = 0.1
      100.times do
        d = config.retry_delay_for(:short)
        expect(d).to be_between(27.0, 33.0)
      end
    end

    it "decouples the reconciler lock TTL from the staleness threshold" do
      expect(config.reconciliation_interval).to eq(300)
      expect(config.reconciler_lock_ttl).to eq(600)
    end

    it "exposes configurable event-emission retry knobs" do
      expect(config.event_emit_retries).to eq(3)
      expect(config.event_emit_backoff).to eq(1)
    end

    it "disables the schedule poller by default" do
      expect(config.schedule_poller_enabled).to eq(false)
    end

    it "bounds failure-metadata retention separately from batch_ttl" do
      expect(config.batch_ttl).to eq(7 * 24 * 3600)
      expect(config.failures_ttl).to eq(24 * 3600)       # shorter than batch_ttl
      expect(config.max_failures_per_batch).to eq(1000)
    end

    it "ships sane fairness-lane defaults (fairness is a per-worker opt-in)" do
      expect(config).not_to respond_to(:fairness_enabled)
      expect(config.fairness_global_concurrency).to eq(50)
      expect(config.fairness_ready_window).to eq(500)
      expect(config.fairness_default_weight).to eq(1.0)
      expect(config.fairness_weighted_concurrency).to eq(true)
      expect(config.fairness_ingest_topic(:time)).to eq("kafka_batch.fair_time_ingest")
      expect(config.fairness_ready_topic(:time)).to eq("kafka_batch.fair_time_ready")
      expect(config.fairness_ready_topic(:time, :go)).to eq("kafka_batch.fair_time_ready.go")
      expect(config.fairness_ready_topic(:time, :ruby)).to eq("kafka_batch.fair_time_ready.ruby")
      expect(config.fairness_ingest_topic(:throughput)).to eq("kafka_batch.fair_throughput_ingest")
      expect(config.fairness_ready_topic(:throughput)).to eq("kafka_batch.fair_throughput_ready")
      expect(config.fairness_min_ingest_partitions).to eq(2)
    end

    it "defaults fairness_max_inflight_per_tenant to 0 (dynamic fair share only)" do
      expect(config.fairness_max_inflight_per_tenant).to eq(0)
    end

    it "derives all topic names and the consumer group from topic_prefix" do
      config.topic_prefix = "myapp"
      expect(config.jobs_topic).to eq("myapp.kafka_batch.jobs")
      expect(config.events_topic).to eq("myapp.kafka_batch.events")
      expect(config.callbacks_topic).to eq("myapp.kafka_batch.callbacks")
      expect(config.dead_letter_topic).to eq("myapp.kafka_batch.dead_letter")
      expect(config.retry_topic).to eq("myapp.kafka_batch.jobs.retry")
      expect(config.consumer_group).to eq("myapp.kafka-batch")
      expect(config.fairness_ingest_topic(:time)).to eq("myapp.kafka_batch.fair_time_ingest")
      expect(config.fairness_ready_topic(:throughput)).to eq("myapp.kafka_batch.fair_throughput_ready")
      expect(config.resolve_topic("kafka_batch.jobs.p0")).to eq("myapp.kafka_batch.jobs.p0")
    end

    it "lets an explicit topic name override the prefix" do
      config.topic_prefix = "myapp"
      config.jobs_topic   = "custom.jobs"
      expect(config.jobs_topic).to eq("custom.jobs")
      expect(config.events_topic).to eq("myapp.kafka_batch.events")  # others still derived
    end

    it "uses bare (unprefixed) names when topic_prefix is empty" do
      expect(config.topic_prefix).to eq("")
      expect(config.jobs_topic).to eq("kafka_batch.jobs")
      expect(config.consumer_group).to eq("kafka-batch")
    end

    it "no longer exposes the removed global fairness_mode reader" do
      expect(config).not_to respond_to(:fairness_mode)
      # the setter is a deprecation no-op (does not raise)
      expect { config.fairness_mode = :time_fairness }.not_to raise_error
    end

    it "defaults retry_max_pause_seconds to 30 (caps partition pause to ~1 poll cycle)" do
      expect(config.retry_max_pause_seconds).to eq(30)
    end

    it "defaults max_message_bytes to 1 MiB (Kafka's typical broker default)" do
      expect(config.max_message_bytes).to eq(1_048_576)
    end

    it "defaults liveness_backend to :redis" do
      expect(config.liveness_backend).to eq(:redis)
      expect(config.liveness_stats_interval).to eq(15)
    end

    it "defaults max_reconcile_per_run to 100" do
      expect(config.max_reconcile_per_run).to eq(100)
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

    it "requires Redis via redis_url or redis hash" do
      config.store     = :redis
      config.redis_url = ""
      expect { config.validate! }.to raise_error(KafkaBatch::ConfigurationError, /redis_url|config\.redis/)

      config.redis = { host: "localhost", port: 6379, db: 0 }
      expect { config.validate! }.not_to raise_error
    end

    it "accepts :off as a valid liveness_backend" do
      config.store            = :mysql
      config.brokers          = ["localhost:9092"]
      config.liveness_backend = :off
      expect { config.validate! }.not_to raise_error
    end

    it "rejects an unknown liveness_backend" do
      config.store            = :mysql
      config.brokers          = ["localhost:9092"]
      config.liveness_backend = :store   # :store was removed — Redis is mandatory
      expect { config.validate! }.to raise_error(KafkaBatch::ConfigurationError, /liveness_backend/)
    end
  end
end
