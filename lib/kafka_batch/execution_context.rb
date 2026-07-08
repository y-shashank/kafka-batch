# frozen_string_literal: true

module KafkaBatch
  # Immutable view of a decoded job message passed to an Executor.
  # The execution host (JobConsumer) owns offset commit, fair slots, retries,
  # and events; executors run user code only.
  class ExecutionContext
    attr_reader :data, :message, :handler, :worker_class, :job_type,
                :job_id, :batch_id, :payload, :attempt

    def initialize(data:, message:, handler:)
      @data          = data
      @message       = message
      @handler       = handler
      @worker_class  = handler.worker_class
      @job_type      = handler.job_type
      @job_id        = data["job_id"]
      @batch_id      = data["batch_id"]
      @payload       = data["payload"] || {}
      @attempt       = data["attempt"].to_i
    end
  end
end
