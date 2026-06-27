module KafkaBatch
  class Error < StandardError; end

  # Raised when the gem is misconfigured
  class ConfigurationError < Error; end

  # Raised when a batch cannot be found in the store
  class BatchNotFoundError < Error; end

  # Raised when pushing jobs into a batch that has already been locked
  class BatchLockedError < Error; end

  # Raised when Kafka message production fails
  class ProducerError < Error; end

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
