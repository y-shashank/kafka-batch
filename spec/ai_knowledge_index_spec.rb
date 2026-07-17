# frozen_string_literal: true

require "oj"

RSpec.describe KafkaBatch::Ai::Chunker do
  let(:readme) do
    <<~MD
      # Title

      Preamble text.

      ## Table of contents

      - skip me

      ## 1. Overview

      Overview body.

      ### 1.1 Detail

      Detail body with config.super_fetch_concurrency.

      ## 2. Alone

      Just a section.
    MD
  end

  let(:faq) do
    <<~MD
      # FAQ

      ## A. Basics

      ### What is it?

      A batch system.

      ### Why Redis?

      Coordination.
    MD
  end

  def write_temp(name, content)
    path = File.join(Dir.tmpdir, "kb-ai-#{name}-#{Process.pid}.md")
    File.write(path, content)
    path
  end

  it "chunks README by ##/### and FAQ by questions, skipping TOC" do
    r = write_temp("readme", readme)
    f = write_temp("faq", faq)
    out = File.join(Dir.tmpdir, "kb-chunks-#{Process.pid}.json")

    payload = described_class.write!(output_path: out, readme_path: r, faq_path: f)
    expect(payload["chunk_count"]).to be >= 5
    expect(payload["corpus_version"]).to match(/\A[a-f0-9]{32}\z/)
    titles = payload["chunks"].map { |c| c["title"] }
    expect(titles).to include("Introduction")
    expect(titles).to include("1.1 Detail")
    expect(titles).to include("What is it?")
    expect(titles.grep(/table of contents/i)).to be_empty
    expect(payload["chunks"].all? { |c| c["id"] && c["text"] && c["source"] }).to eq(true)
  ensure
    FileUtils.rm_f([r, f, out].compact)
  end
end

RSpec.describe KafkaBatch::Ai::KnowledgeIndex do
  describe "when disabled" do
    it "returns :disabled" do
      KafkaBatch.config.ai_knowledge_enabled = false
      expect(described_class.sync!).to eq(:disabled)
    end
  end

  describe "with Redis available" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.ai_knowledge_enabled = true
      KafkaBatch.config.super_fetch_concurrency = 3
      described_class.reset_pool!
      KafkaBatchSpec::RedisHelper.flush!
    end

    after { described_class.reset_pool! }

    it "syncs corpus once, then skips while version and config are fresh" do
      expect(File).to exist(described_class.packaged_path)

      expect(described_class.sync!).to eq(:synced_corpus)
      meta = described_class.meta
      expect(meta["corpus_version"]).to be_a(String)
      expect(meta["config_refreshed_at"]).to be_a(String)
      expect(meta["chunk_count"].to_i).to be > 10
      expect(described_class.chunk_ids).to include(described_class::LIVE_CONFIG_CHUNK_ID)

      snap = described_class.config_snapshot
      expect(snap["super_fetch_concurrency"]).to eq(3)
      expect(snap["topic_inventory"]).to be_a(Hash)
      expect(snap["topic_inventory"]["topics"]).to be_an(Array)
      expect(snap["routing"]).to be_a(Hash)
      expect(snap["routing"]).to include("handlers", "priority_groups")

      live = described_class.fetch_chunk(described_class::LIVE_CONFIG_CHUNK_ID)
      expect(live["text"]).to include("super_fetch_concurrency: 3")
      expect(live["text"]).to include("AUTHORITATIVE LIVE TOPIC PARTITIONS")
      expect(live["text"]).to include("AUTHORITATIVE LIVE ROUTING")
      expect(live["text"]).to include("live_broker_partitions=")
      expect(live["text"]).to include("create_default_partitions=")

      expect(described_class.sync!).to eq(:skipped_fresh)
    end

    it "refreshes config only after 24h without rewriting corpus version" do
      expect(described_class.sync!).to eq(:synced_corpus)
      version = described_class.meta["corpus_version"]

      Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL).hset(
        described_class::META_KEY,
        "config_refreshed_at" => (Time.now.utc - described_class::CONFIG_REFRESH_SECONDS - 60).iso8601
      )

      KafkaBatch.config.super_fetch_concurrency = 9
      expect(described_class.sync!).to eq(:synced_config)
      expect(described_class.meta["corpus_version"]).to eq(version)
      expect(described_class.config_snapshot["super_fetch_concurrency"]).to eq(9)
      expect(described_class.fetch_chunk(described_class::LIVE_CONFIG_CHUNK_ID)["text"])
        .to include("super_fetch_concurrency: 9")
      expect(described_class.meta["topics_refreshed_at"]).to be_a(String)
    end

    it "rewrites corpus when meta is cleared (simulates new corpus_version)" do
      expect(described_class.sync!).to eq(:synced_corpus)
      expect(described_class.sync!).to eq(:skipped_fresh)

      Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL).del(described_class::META_KEY)
      expect(described_class.sync!).to eq(:synced_corpus)
    end

    it "skips when another pod holds the lock" do
      redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      redis.set(described_class::LOCK_KEY, "other-pod", nx: true, ex: 30)
      expect(described_class.sync!).to eq(:skipped_locked)
    end
  end
end
