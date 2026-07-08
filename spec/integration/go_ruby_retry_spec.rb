# frozen_string_literal: true

require_relative "../support/ruby_worker_integration_helper"
require_relative "../support/ruby_daemon_workers"

RSpec.describe "Go daemon Ruby worker retry (integration)", :integration do
  include KafkaBatchSpec::RubyWorkerIntegrationHelper

  before(:each) do
    integration_preflight!
    KafkaBatchSpec::WorkerRuns.reset!
    start_ruby_worker_stack!(tmpdir_prefix: "kbatch-ruby-retry")
  end

  after(:each) { stop_ruby_worker_stack! }

  def write_manifest!
    @manifest_path = File.join(@tmpdir, "handlers.yml")
    @worker_topic = "kb.ruby.retry.#{suffix}"
    File.write(@manifest_path, {
      "handlers" => {
        "integration.ruby_retry_once" => {
          "runtime" => "ruby",
          "worker_class" => "IntegrationRubyRetryWorker",
          "topic" => @worker_topic,
          "apply_topic_prefix" => false,
          "max_retries" => 2
        }
      }
    }.to_yaml)
  end

  def extra_daemon_config
    { "jobs_topics" => [@worker_topic] }
  end

  def integration_topics
    super + [@worker_topic]
  end

  it "retries a failing Ruby job then completes the batch" do
    job_id = nil
    batch = KafkaBatch::Batch.create(description: "ruby retry #{suffix}") do |b|
      job_id = b.push_job("integration.ruby_retry_once", { "ping" => 1 })
    end

    wait_for_batch!(batch.id)
    wait_for_marker!

    expect(File.read(@marker_path)).to eq(job_id)
    expect(KafkaBatchSpec::WorkerRuns.runs.map { |r| r[:name] }).to include(:integration_ruby_retry_once)

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("success")
    expect(reloaded[:completed_count]).to eq(1)
  end
end
