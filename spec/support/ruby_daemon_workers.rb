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

class IntegrationRubyFairWorker
  include KafkaBatch::Worker
  job_type "integration.ruby_fair"

  def perform(payload)
    if (path = ENV["KBATCH_RUBY_WORKER_ITEST_MARKER"])
      tenant = payload["tenant"].to_s
      File.write(path, "#{job_id}:#{tenant}")
    end
    KafkaBatchSpec::WorkerRuns.record(:integration_ruby_fair, payload)
  end
end

class IntegrationRubyP1Worker
  include KafkaBatch::Worker
  job_type "integration.ruby_p1"

  def perform(payload)
    if (path = ENV["KBATCH_RUBY_WORKER_ITEST_MARKER"])
      File.write(path, job_id.to_s)
    end
    KafkaBatchSpec::WorkerRuns.record(:integration_ruby_p1, payload)
  end
end

class IntegrationRubyHybridWorker
  include KafkaBatch::Worker
  job_type "integration.ruby_hybrid"

  def perform(payload)
    path = ENV["KBATCH_RUBY_HYBRID_MARKER"]
    File.write(path, job_id.to_s) if path && !path.empty?
    KafkaBatchSpec::WorkerRuns.record(:integration_ruby_hybrid, payload)
  end
end

class IntegrationRubyRetryWorker
  include KafkaBatch::Worker
  job_type "integration.ruby_retry_once"

  def perform(_payload)
    raise RuntimeError, "fail on first attempt" if retry_count < 1

    if (path = ENV["KBATCH_RUBY_WORKER_ITEST_MARKER"])
      File.write(path, job_id.to_s)
    end
    KafkaBatchSpec::WorkerRuns.record(:integration_ruby_retry_once, {})
  end
end

class IntegrationRubyUniqWorker
  include KafkaBatch::Worker
  job_type "integration.ruby_uniq"
  uniq true

  def perform(payload)
    if (path = ENV["KBATCH_RUBY_WORKER_ITEST_MARKER"])
      File.write(path, job_id.to_s)
    end
    KafkaBatchSpec::WorkerRuns.record(:integration_ruby_uniq, payload)
  end
end
