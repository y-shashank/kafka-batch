# frozen_string_literal: true

require_relative "../support/ruby_worker_integration_helper"
require_relative "../support/ruby_daemon_workers"

RSpec.describe "Go daemon Ruby priority queue (integration)", :integration do
  include KafkaBatchSpec::RubyWorkerIntegrationHelper

  before(:each) do
    integration_preflight!
    KafkaBatchSpec::WorkerRuns.reset!
    start_ruby_worker_stack!(tmpdir_prefix: "kbatch-ruby-prio")
  end

  after(:each) { stop_ruby_worker_stack! }

  def write_manifest!
    @manifest_path = File.join(@tmpdir, "handlers.yml")
    @p1_topic = "kb.ruby.prio.p1.#{suffix}"
    @priority_path = File.join(@tmpdir, "priority.yml")

    File.write(@priority_path, {
      "consumer_group_suffix" => "jobs-fast",
      "mode" => "weighted",
      "weighted_interleave" => 4,
      "topics" => [@p1_topic]
    }.to_yaml)

    File.write(@manifest_path, {
      "handlers" => {
        "integration.ruby_p1" => {
          "runtime" => "ruby",
          "worker_class" => "IntegrationRubyP1Worker",
          "topic" => @p1_topic,
          "apply_topic_prefix" => false,
          "max_retries" => 2
        }
      }
    }.to_yaml)
  end

  def extra_daemon_config
    {
      "priority_config_paths" => [@priority_path],
      "priority_lag_check_interval" => 1,
      "priority_weighted_interleave" => 4
    }
  end

  def integration_topics
    super + [@p1_topic]
  end

  it "runs runtime:ruby p1 jobs through the priority consumer group" do
    job_id = KafkaBatch::Batch.enqueue_job("integration.ruby_p1", { "n" => 1 })
    wait_for_marker!
    expect(File.read(@marker_path)).to eq(job_id)
    expect(KafkaBatchSpec::WorkerRuns.runs.last[:name]).to eq(:integration_ruby_p1)
  end
end
