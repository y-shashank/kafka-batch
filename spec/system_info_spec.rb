require "spec_helper"

RSpec.describe KafkaBatch::SystemInfo do
  let(:config) { KafkaBatch::Configuration.new }

  describe ".mask_redis_url" do
    it "masks password in a redis URL" do
      url = described_class.mask_redis_url("redis://user:sekrit@cache.example.com:6379/2")
      expect(url).to include("***")
      expect(url).not_to include("sekrit")
      expect(url).to include("cache.example.com:6379/2")
    end

    it "leaves passwordless URLs unchanged" do
      expect(described_class.mask_redis_url("redis://localhost:6379/0")).to eq("redis://localhost:6379/0")
    end
  end

  describe ".mask_config_value" do
    it "masks sensitive keys" do
      expect(described_class.mask_config_value("sasl.password", "secret")).to eq("***")
      expect(described_class.mask_config_value("api_key", "abc")).to eq("***")
    end

    it "passes through benign values" do
      expect(described_class.mask_config_value("compression.type", "snappy")).to eq("snappy")
    end
  end

  describe ".sections" do
    it "includes overview, kafka, and redis cards" do
      titles = described_class.sections(config).map(&:title)
      expect(titles).to include("Overview", "Kafka", "Redis", "Fairness", "Liveness")
    end

    it "masks redis hash passwords" do
      config.redis_url = ""
      config.redis = { host: "localhost", port: 6379, db: 0, password: "hunter2" }

      redis = described_class.sections(config).find { |s| s.id == "redis" }
      password_row = redis.rows.find { |r| r.label == "password" }
      expect(password_row.value).to eq("***")
      expect(password_row.masked).to be(true)
    end

    it "includes a MySQL card when store is :mysql" do
      config.store = :mysql
      titles = described_class.sections(config).map(&:title)
      expect(titles).to include("MySQL")
    end

    it "omits empty rdkafka cards" do
      config.producer_config = {}
      config.consumer_config = {}
      titles = described_class.sections(config).map(&:title)
      expect(titles).not_to include("Producer (rdkafka)", "Consumer (rdkafka)")
    end

    it "includes rdkafka cards when overrides are set" do
      config.producer_config = { "compression.type" => "snappy" }
      titles = described_class.sections(config).map(&:title)
      expect(titles).to include("Producer (rdkafka)")
    end
  end
end
