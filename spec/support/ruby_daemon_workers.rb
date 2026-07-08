# frozen_string_literal: true

# Workers used by Go daemon + Ruby worker-server integration specs.
class IntegrationRubyDaemonWorker
  include KafkaBatch::Worker
  job_type "integration.ruby_daemon"

  def perform(payload)
    if (path = ENV["KBATCH_RUBY_WORKER_ITEST_MARKER"])
      File.write(path, job_id.to_s)
    end
    KafkaBatchSpec::WorkerRuns.record(:integration_ruby_daemon, payload)
  end
end
