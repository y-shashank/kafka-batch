require "spec_helper"

RSpec.describe KafkaBatch::RedisClient do
  let(:config) { KafkaBatch::Configuration.new }

  describe ".url_for" do
    it "builds a URL from host/port/db" do
      url = described_class.url_for(host: "localhost", port: 6379, db: 2)
      expect(url).to eq("redis://localhost:6379/2")
    end

    it "uses :id when it looks like a redis URL" do
      url = described_class.url_for(
        host: "localhost", port: 6379, db: 0,
        id: "redis://localhost:6379", location: "localhost:6379"
      )
      expect(url).to eq("redis://localhost:6379")
    end
  end

  describe ".connection_options" do
    it "returns a URL option when redis_url is set" do
      config.redis_url = "redis://cache:6379/1"
      expect(described_class.connection_options(config)).to eq(url: "redis://cache:6379/1")
    end

    it "returns host/port/db when config.redis hash is set" do
      config.redis = { host: "redis.internal", port: 6380, db: 3 }
      expect(described_class.connection_options(config)).to eq(
        host: "redis.internal", port: 6380, db: 3
      )
    end

    it "prefers explicit :url inside the hash" do
      config.redis = { url: "redis://override:6379/4", host: "ignored" }
      expect(described_class.connection_options(config)).to eq(url: "redis://override:6379/4")
    end
  end

  describe "Configuration integration" do
    it "clears redis_url when redis hash is assigned" do
      config.redis_url = "redis://old:6379/0"
      config.redis = { host: "localhost", port: 6379, db: 0 }
      expect(config.redis_url_raw).to be_nil
      expect(config.redis_url).to eq("redis://localhost:6379/0")
    end

    it "clears redis hash when redis_url is assigned" do
      config.redis = { host: "localhost", port: 6379, db: 0 }
      config.redis_url = "redis://explicit:6379/1"
      expect(config.redis).to be_nil
      expect(config.redis_url).to eq("redis://explicit:6379/1")
    end

    it "validates when only redis hash is set" do
      config.redis_url = ""
      config.redis = { host: "localhost", port: 6379, db: 0 }
      expect { config.validate! }.not_to raise_error
    end
  end
end
