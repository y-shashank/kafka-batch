module KafkaBatchSpec
  module WorkerRuns
    class << self
      def reset!
        @runs = []
      end

      def record(name, payload)
        runs << { name: name, payload: payload }
      end

      def runs
        @runs ||= []
      end
    end
  end
end

class SuccessfulWorker
  include KafkaBatch::Worker
  kafka_topic "test.success"

  def perform(payload)
    KafkaBatchSpec::WorkerRuns.record(:success, payload)
  end
end

class FailingWorker
  include KafkaBatch::Worker
  kafka_topic "test.fail"
  max_retries 2

  def perform(_payload)
    raise "always fails"
  end
end

# Worker that opts into the multi-tenant fair lane (ingest -> ready).
class FairWorker
  include KafkaBatch::Worker
  kafka_topic "test.fair"
  fairness true

  def perform(payload)
    KafkaBatchSpec::WorkerRuns.record(:fair, payload)
  end
end

# Worker on the THROUGHPUT fairness lane (job-count fairness).
class ThroughputFairWorker
  include KafkaBatch::Worker
  kafka_topic "test.fair_throughput"
  fairness true
  fairness_type :throughput

  def perform(payload)
    KafkaBatchSpec::WorkerRuns.record(:fair_throughput, payload)
  end
end

# Always-failing worker that pins every retry to the :large tier.
class TierPinnedWorker
  include KafkaBatch::Worker
  kafka_topic "test.tier_pinned"
  max_retries 5
  retry_tier :large

  def perform(_payload)
    raise "always fails"
  end
end

# Pushes a child job into its own batch (exercises the in-job `batch` context).
class FanoutWorker
  include KafkaBatch::Worker
  kafka_topic "test.fanout"

  def perform(payload)
    KafkaBatchSpec::WorkerRuns.record(:fanout, payload)
    batch&.push(SuccessfulWorker, { "child_of" => payload["id"] })
  end
end

class ContextProbeWorker
  include KafkaBatch::Worker
  kafka_topic "test.context_probe"

  def perform(_payload)
    KafkaBatchSpec::WorkerRuns.record(
      :context_probe,
      batch_open: !batch.nil?,
      job_id:     job_id,
      batch_id:   batch_id,
      retry_count: retry_count
    )
  end
end

class UniqWorker
  include KafkaBatch::Worker
  kafka_topic "test.uniq"
  uniq true

  def perform(payload)
    KafkaBatchSpec::WorkerRuns.record(:uniq, payload)
  end
end

# A plain class that is NOT a KafkaBatch::Worker (for negative validation).
class NotAWorker
end
