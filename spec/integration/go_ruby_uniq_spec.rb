# frozen_string_literal: true

require_relative "../support/ruby_worker_integration_helper"
require_relative "../support/ruby_daemon_workers"

RSpec.describe "Go daemon Ruby uniq lock release (integration)", :integration do
  include KafkaBatchSpec::RubyWorkerIntegrationHelper

  before(:each) do
    integration_preflight!
    KafkaBatchSpec::WorkerRuns.reset!
    start_ruby_worker_stack!(tmpdir_prefix: "kbatch-ruby-uniq")
  end

  after(:each) { stop_ruby_worker_stack! }

  def write_manifest!
    @manifest_path = File.join(@tmpdir, "handlers.yml")
    @worker_topic = "kb.ruby.uniq.#{suffix}"
    File.write(@manifest_path, {
      "handlers" => {
        "integration.ruby_uniq" => {
          "runtime" => "ruby",
          "worker_class" => "IntegrationRubyUniqWorker",
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

  it "releases the uniq lock after success so the same payload can be enqueued again" do
    payload = { "dedupe" => suffix }

    batch1 = KafkaBatch::Batch.create(description: "uniq first #{suffix}") do |b|
      b.push_job("integration.ruby_uniq", payload)
    end
    wait_for_batch!(batch1.id)
    wait_for_marker!
    first_runs = KafkaBatchSpec::WorkerRuns.runs.size

    KafkaBatchSpec::WorkerRuns.reset!
    ENV["KBATCH_RUBY_WORKER_ITEST_MARKER"] = File.join(@tmpdir, "marker2")
    @marker_path = ENV["KBATCH_RUBY_WORKER_ITEST_MARKER"]

    job_id2 = nil
    batch2 = KafkaBatch::Batch.create(description: "uniq second #{suffix}") do |b|
      job_id2 = b.push_job("integration.ruby_uniq", payload)
    end
    expect(job_id2).not_to be_nil

    wait_for_batch!(batch2.id)
    wait_for_marker!

    expect(KafkaBatchSpec::WorkerRuns.runs.size).to eq(first_runs)
    expect(File.read(@marker_path)).to eq(job_id2)
  end
end
