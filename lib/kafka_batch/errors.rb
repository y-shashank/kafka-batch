module KafkaBatch
  class Error < StandardError; end

  # Raised when the gem is misconfigured
  class ConfigurationError < Error; end

  # Raised when a batch cannot be found in the store
  class BatchNotFoundError < Error; end

  # Raised when pushing jobs into a batch that is already closed: it has either
  # completed (its callback was dispatched) or been cancelled. Open batches –
  # including ones currently running jobs – always accept more jobs.
  class BatchClosedError < Error; end

  # Raised when Kafka message production fails
  class ProducerError < Error; end

  # Raised when a bulk produce call fails after delivering a prefix of messages.
  # +produced_count+ is the number of messages confirmed on Kafka (gap-free
  # prefix). +dispatched+ holds WaterDrop delivery handles for the failing chunk.
  class PartialProduceError < ProducerError
    attr_reader :dispatched, :produced_count

    def initialize(message, dispatched: [], produced_count: nil)
      super(message)
      @dispatched     = dispatched || []
      @produced_count = produced_count
    end
  end

  # Raised when enqueueing a job that opts into `uniq true` while an identical
  # job (same worker + payload) is already queued or in progress. Only raised
  # when config.uniq_on_duplicate is :raise.
  class DuplicateJobError < Error
    attr_reader :worker_class, :payload

    def initialize(worker_class:, payload:)
      @worker_class = worker_class
      @payload      = payload
      super("Duplicate job: #{worker_class.name} with same arguments already queued or in progress")
    end
  end

  # Raised on store read/write failures
  class StoreError < Error; end

  # Raised when a job exhausts all retry attempts.
  # Note: does not override Exception#cause – use Ruby's native exception
  # chaining (`raise ... ` inside a rescue) to preserve the original error.
  class JobExhaustedError < Error
    attr_reader :job_id, :batch_id, :worker_class, :payload

    def initialize(msg = nil, job_id:, batch_id:, worker_class:, payload:)
      @job_id       = job_id
      @batch_id     = batch_id
      @worker_class = worker_class
      @payload      = payload
      super(msg || "Job #{job_id} exhausted retries (#{worker_class})")
    end
  end
end
