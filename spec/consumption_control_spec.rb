RSpec.describe KafkaBatch::ConsumptionControl do
  describe "redis backend" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.store     = :mysql
      KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      described_class.reset!
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "pauses and resumes a whole topic" do
      described_class.pause_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 3)).to eq(true)

      described_class.resume_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(false)
    end

    it "pauses and resumes a single partition" do
      described_class.pause_partition(group: "g", topic: "demo", partition: 2)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 2)).to eq(true)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 1)).to eq(false)

      described_class.resume_partition(group: "g", topic: "demo", partition: 2)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 2)).to eq(false)
    end

    it "prefers redis over mysql when both are available" do
      expect(described_class.backend).to eq(:redis)
    end
  end

  describe "mysql backend" do
    before do
      KafkaBatch.config.store     = :mysql
      KafkaBatch.config.redis_url = ""
      described_class.reset!
    end

    it "pauses and resumes a whole topic" do
      expect(described_class.backend).to eq(:mysql)

      described_class.pause_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      described_class.resume_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(false)
    end

    it "pauses and resumes a single partition" do
      described_class.pause_partition(group: "g", topic: "demo", partition: 2)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 2)).to eq(true)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 1)).to eq(false)

      described_class.resume_partition(group: "g", topic: "demo", partition: 2)
      expect(described_class.paused?(group: "g", topic: "demo", partition: 2)).to eq(false)
    end
  end

  describe "consumer snapshot cache" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.consumption_control_refresh_interval = 60
      described_class.reset!
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "reuses the cached snapshot until the refresh interval elapses" do
      described_class.pause_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      # Simulate another process resuming without invalidating this process's cache.
      described_class.send(:redis_resume_topic, "g", "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      snap = described_class.snapshot(refresh: true)
      expect(snap[:topics]).not_to include(described_class.topic_key("g", "demo"))
    end

    it "reloads after consumption_control_refresh_interval seconds" do
      t = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC) { t }

      described_class.pause_topic(group: "g", topic: "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      described_class.send(:redis_resume_topic, "g", "demo")
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(true)

      t = 61.0
      expect(described_class.paused?(group: "g", topic: "demo", partition: 0)).to eq(false)
    end
  end
end
