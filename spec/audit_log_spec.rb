# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::AuditLog do
  before do
    KafkaBatchSpec::ActiveRecordSupport.establish!
    KafkaBatchSpec::ActiveRecordSupport.truncate!
    described_class.reset!
    KafkaBatch.configure { |c| c.audit_enabled = true }
  end

  after do
    described_class.reset!
    KafkaBatch.config.audit_enabled = false
  end

  it "persists a web action row when enabled" do
    described_class.record_web_action(
      env:    { "HTTP_X_KAFKA_BATCH_ACTOR" => "ops@example.com", "REMOTE_ADDR" => "127.0.0.1" },
      path:   "/batches/bulk",
      params: { "bulk_action" => "cancel", "batch_ids" => "a,b" },
      status: "ok"
    )

    row = ActiveRecord::Base.connection.select_one(
      "SELECT action, actor, status FROM kafka_batch_audit_logs LIMIT 1"
    )
    expect(row["action"]).to eq("batches.bulk")
    expect(row["actor"]).to eq("ops@example.com")
    expect(row["status"]).to eq("ok")
  end

  it "is a no-op when audit_enabled is false" do
    KafkaBatch.config.audit_enabled = false
    described_class.record(action: "test", path: "/x", status: "ok")
    count = ActiveRecord::Base.connection.select_value("SELECT COUNT(*) FROM kafka_batch_audit_logs")
    expect(count).to eq(0)
  end

  it "scrubs sensitive params" do
    described_class.record_web_action(
      env:    {},
      path:   "/weights",
      params: { "tenant_id" => "acme", "_csrf" => "secret", "weight" => "2" },
      status: "ok"
    )
    row = ActiveRecord::Base.connection.select_one("SELECT metadata FROM kafka_batch_audit_logs LIMIT 1")
    meta = Oj.load(row["metadata"])
    expect(meta["params"]).to include("tenant_id" => "acme", "weight" => "2")
    expect(meta["params"]).not_to have_key("_csrf")
  end

  describe ".list" do
    it "returns rows newest-first with parsed metadata" do
      described_class.record(action: "batches.cancel", path: "/batches/a/cancel", status: "ok", metadata: { a: 1 })
      described_class.record(action: "batches.delete", path: "/batches/b/delete", status: "error", metadata: { b: 2 })

      rows = described_class.list(limit: 10)
      expect(rows.size).to eq(2)
      expect(rows.first[:action]).to eq("batches.delete")   # newest first
      expect(rows.first[:status]).to eq("error")
      expect(rows.first[:metadata]).to eq({ "b" => 2 })     # parsed to a Hash
    end

    it "filters by action and paginates" do
      3.times { described_class.record(action: "lag.pause", path: "/lag/pause", status: "ok") }
      described_class.record(action: "lag.resume", path: "/lag/resume", status: "ok")

      expect(described_class.list(action: "lag.pause").size).to eq(3)
      expect(described_class.list(limit: 2).size).to eq(2)
      expect(described_class.list(limit: 2, offset: 2).size).to eq(2)  # 4 total
    end

    it "returns [] when auditing is disabled" do
      KafkaBatch.config.audit_enabled = false
      expect(described_class.list).to eq([])
    end
  end

  describe ".actions" do
    it "returns the distinct action names present" do
      described_class.record(action: "lag.pause", path: "/lag/pause", status: "ok")
      described_class.record(action: "lag.pause", path: "/lag/pause", status: "ok")
      described_class.record(action: "batches.delete", path: "/batches/b/delete", status: "ok")

      expect(described_class.actions).to contain_exactly("batches.delete", "lag.pause")
    end
  end
end
