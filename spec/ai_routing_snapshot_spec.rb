# frozen_string_literal: true

RSpec.describe KafkaBatch::Ai::RoutingSnapshot do
  let(:handlers_yml) { File.expand_path("fixtures/handlers/go_ruby.yml", __dir__) }
  let(:priority_yml) { File.expand_path("fixtures/priority/fast.yml", __dir__) }

  before do
    KafkaBatch::HandlerManifest.reset!
    KafkaBatch.config.handler_manifest_path = handlers_yml
    KafkaBatch.config.priority_config_paths = [priority_yml]
    KafkaBatch.config.extra_job_topics = %w[orders.process]
    KafkaBatch.config.jobs_topics = %w[kafka_batch.jobs.go]
  end

  after { KafkaBatch::HandlerManifest.reset! }

  it "reads handlers from YAML without loading HandlerManifest" do
    expect(KafkaBatch::HandlerManifest.loaded?).to eq(false)
    snap = described_class.build
    expect(snap["handlers_source"]).to eq("yaml")
    expect(snap["handler_count"]).to be >= 2

    segment = snap["handlers"].find { |h| h["job_type"] == "segment.export" }
    expect(segment).to include(
      "runtime" => "go",
      "topic" => "segment.exports",
      "max_retries" => 25,
      "fairness" => false
    )
  end

  it "uses the in-process registry when the manifest is already loaded" do
    KafkaBatch::HandlerManifest.load!(handlers_yml)
    snap = described_class.build
    expect(snap["handlers_source"]).to eq("registry")
    expect(snap["handlers"].map { |h| h["job_type"] }).to include("priority.fast")
  end

  it "includes priority groups from YAML (highest topic first)" do
    snap = described_class.build
    expect(snap["priority_group_count"]).to eq(1)
    group = snap["priority_groups"].first
    expect(group["consumer_group_suffix"]).to eq("jobs-fast")
    expect(group["mode"]).to eq("weighted")
    expect(group["topics"]).to eq(%w[kafka_batch.jobs.p0 kafka_batch.jobs.p1])
    expect(group["consumer_group"]).to include("jobs-fast")
  end

  it "includes extra_job_topics and jobs_topics" do
    snap = described_class.build
    expect(snap["extra_job_topics"]).to eq(%w[orders.process])
    expect(snap["jobs_topics"]).to eq(%w[kafka_batch.jobs.go])
  end
end
