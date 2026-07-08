# frozen_string_literal: true

require_relative "../support/ruby_worker_integration_helper"
require_relative "../support/ruby_daemon_workers"

RSpec.describe "Go daemon Ruby fair lane (integration)", :integration do
  include KafkaBatchSpec::RubyWorkerIntegrationHelper

  before(:each) do
    integration_preflight!
    KafkaBatchSpec::WorkerRuns.reset!
    start_ruby_worker_stack!(tmpdir_prefix: "kbatch-ruby-fair")
  end

  after(:each) { stop_ruby_worker_stack! }

  def write_manifest!
    @manifest_path = File.join(@tmpdir, "handlers.yml")
    @worker_topic = "kb.ruby.fair.worker.#{suffix}"
    @fair_ingest_topic = "kb.ruby.fair.ingest.#{suffix}"
    @fair_ready_topic = "kb.ruby.fair.ready.#{suffix}"

    File.write(@manifest_path, {
      "handlers" => {
        "integration.ruby_fair" => {
          "runtime" => "ruby",
          "worker_class" => "IntegrationRubyFairWorker",
          "topic" => @worker_topic,
          "apply_topic_prefix" => false,
          "fairness_type" => "time",
          "max_retries" => 2
        }
      }
    }.to_yaml)
  end

  def extra_daemon_config
    {
      "jobs_topics" => [@worker_topic],
      "fairness_enabled" => true,
      "fairness_time_ingest" => @fair_ingest_topic,
      "fairness_time_ready" => @fair_ready_topic,
      "fairness_ready_window" => 100,
      "fairness_global_concurrency" => 4,
      "fairness_lease_ttl" => 300,
      "fairness_default_weight" => 1.0,
      "fairness_weighted_concurrency" => false
    }
  end

  def integration_topics
    super + [@worker_topic, @fair_ingest_topic, @fair_ready_topic]
  end

  def configure_kafka_batch!
    super
    KafkaBatch.configure do |c|
      c.fair_time_ingest_topic = @fair_ingest_topic
      c.fair_time_ready_topic = @fair_ready_topic
    end
  end

  it "routes runtime:ruby fair jobs through ingest → ready and completes the batch" do
    job_id = nil
    batch = KafkaBatch::Batch.create(description: "ruby fair #{suffix}") do |b|
      job_id = b.push_job("integration.ruby_fair", { "tenant" => "acme" }, tenant_id: "acme")
    end

    wait_for_batch!(batch.id)
    wait_for_marker!

    expect(File.read(@marker_path)).to eq("#{job_id}:acme")

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("success")
    expect(reloaded[:completed_count]).to eq(1)
  end
end
