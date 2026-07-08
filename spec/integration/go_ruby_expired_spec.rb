# frozen_string_literal: true

require_relative "../support/ruby_worker_integration_helper"
require_relative "../support/ruby_daemon_workers"

RSpec.describe "Go daemon Ruby job expiry (integration)", :integration do
  include KafkaBatchSpec::RubyWorkerIntegrationHelper

  before(:each) do
    integration_preflight!
    KafkaBatchSpec::WorkerRuns.reset!
    start_ruby_worker_stack!(tmpdir_prefix: "kbatch-ruby-expired")
  end

  after(:each) { stop_ruby_worker_stack! }

  def write_manifest!
    @manifest_path = File.join(@tmpdir, "handlers.yml")
    @worker_topic = "kb.ruby.expired.#{suffix}"
    File.write(@manifest_path, {
      "handlers" => {
        "integration.ruby_daemon" => {
          "runtime" => "ruby",
          "worker_class" => "IntegrationRubyDaemonWorker",
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

  it "routes expired jobs to DLT without invoking the Ruby worker" do
    batch = KafkaBatch::Batch.create(description: "ruby expired #{suffix}") do |b|
      b.push_job("integration.ruby_daemon", { "stale" => true },
                 valid_till: "2000-01-01T00:00:00Z")
    end

    wait_for_batch!(batch.id, status: %w[complete failed])

    expect(File.exist?(@marker_path)).to be(false)
    expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty

    dlt = poll_dlt!(batch_id: batch.id)
    expect(dlt["dlt_type"]).to eq("job")
    expect(dlt["dlt_error_class"]).to eq("ExpiredError")

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("complete")
    expect(reloaded[:failed_count]).to eq(1)
  end
end
