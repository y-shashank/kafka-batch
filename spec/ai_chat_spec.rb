# frozen_string_literal: true

require "oj"

RSpec.describe KafkaBatch::Ai::Crypto do
  before { KafkaBatch.config.ai_encryption_salt = "test-salt-#{Process.pid}" }

  after { KafkaBatch.config.ai_encryption_salt = "" }

  it "round-trips plaintext" do
    blob = described_class.encrypt("sk-or-secret")
    expect(blob).not_to include("sk-or-secret")
    expect(described_class.decrypt(blob)).to eq("sk-or-secret")
  end

  it "requires a salt to encrypt" do
    KafkaBatch.config.ai_encryption_salt = ""
    expect { described_class.encrypt("x") }.to raise_error(KafkaBatch::ConfigurationError, /ai_encryption_salt/)
  end
end

RSpec.describe KafkaBatch::Ai::ChatHistory do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_chat_history_max_lines = 5
    described_class.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
  end

  after do
    described_class.reset_pool!
    KafkaBatch.config.ai_chat_history_max_lines = 500
  end

  it "appends, lists newest-first, and trims to max_lines" do
    6.times { |i| described_class.append!(role: "user", content: "m#{i}") }
    expect(described_class.size).to eq(5)
    listed = described_class.list
    expect(listed.map { |m| m["content"] }).to eq(%w[m5 m4 m3 m2 m1])
  end

  it "clears history" do
    described_class.append!(role: "assistant", content: "hi")
    described_class.clear!
    expect(described_class.size).to eq(0)
  end
end

RSpec.describe KafkaBatch::Ai::Settings do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_encryption_salt = "settings-salt"
    described_class.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
  end

  after do
    described_class.reset_pool!
    KafkaBatch.config.ai_encryption_salt = ""
  end

  it "stores an encrypted key and returns a masked preview" do
    shown = described_class.update!(api_key: "sk-or-abcdefgh", model: "openai/gpt-4o-mini")
    expect(shown["api_key_set"]).to eq(true)
    expect(shown["api_key_masked"]).to eq("••••efgh")
    expect(shown["model"]).to eq("openai/gpt-4o-mini")
    expect(described_class.api_key).to eq("sk-or-abcdefgh")
  end

  it "clears the api key" do
    described_class.update!(api_key: "sk-or-abcdefgh")
    shown = described_class.update!(clear_api_key: true)
    expect(shown["api_key_set"]).to eq(false)
    expect(described_class.api_key).to be_nil
  end
end

RSpec.describe KafkaBatch::Ai::Retriever do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_knowledge_enabled = true
    KafkaBatch::Ai::KnowledgeIndex.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
    KafkaBatch::Ai::KnowledgeIndex.sync!
  end

  after { KafkaBatch::Ai::KnowledgeIndex.reset_pool! }

  it "returns scored knowledge chunks for a docs query" do
    hits = described_class.search("super_fetch_concurrency fairness")
    expect(hits).not_to be_empty
    expect(hits.first).to include("id", "title", "text")
  end

  it "puts the live config chunk first so broker partitions beat DEFAULT_PARTITIONS docs" do
    hits = described_class.search("how many partitions fair time ready")
    expect(hits.first["id"]).to eq(KafkaBatch::Ai::KnowledgeIndex::LIVE_CONFIG_CHUNK_ID)
    expect(hits.first["text"]).to include("live_broker_partitions=")
  end
end

RSpec.describe KafkaBatch::Ai::Chat do
  before do
    skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.ai_knowledge_enabled = true
    KafkaBatch.config.ai_encryption_salt = "chat-salt"
    KafkaBatch::Ai::Settings.reset_pool!
    KafkaBatch::Ai::ChatHistory.reset_pool!
    KafkaBatch::Ai::KnowledgeIndex.reset_pool!
    KafkaBatchSpec::RedisHelper.flush!
    KafkaBatch::Ai::KnowledgeIndex.sync!
    KafkaBatch::Ai::Settings.update!(api_key: "sk-or-test", model: "openai/gpt-4o-mini")
  end

  after do
    KafkaBatch::Ai::Settings.reset_pool!
    KafkaBatch::Ai::ChatHistory.reset_pool!
    KafkaBatch::Ai::KnowledgeIndex.reset_pool!
    KafkaBatch.config.ai_encryption_salt = ""
  end

  it "retrieves context, calls OpenRouter, and appends global history" do
    fake = instance_double(KafkaBatch::Ai::OpenRouter)
    expect(KafkaBatch::Ai::OpenRouter).to receive(:new).and_return(fake)
    expect(fake).to receive(:chat) do |messages:|
      expect(messages.any? { |m| m["role"] == "system" && m["content"].include?("Knowledge context") }).to eq(true)
      expect(messages.last).to eq("role" => "user", "content" => "What is SuperFetch?")
      "SuperFetch leases work from Redis."
    end

    result = described_class.ask("What is SuperFetch?")
    expect(result["ok"]).to eq(true)
    expect(result["reply"]).to include("SuperFetch")
    expect(result["history_size"]).to eq(2)
    chron = KafkaBatch::Ai::ChatHistory.list.reverse
    expect(chron.map { |m| m["role"] }).to eq(%w[user assistant])
  end

  it "rejects blank messages and missing API keys" do
    expect { described_class.ask("  ") }.to raise_error(ArgumentError, /blank/)
    KafkaBatch::Ai::Settings.update!(clear_api_key: true)
    expect { described_class.ask("hi") }.to raise_error(ArgumentError, /API key/)
  end
end
