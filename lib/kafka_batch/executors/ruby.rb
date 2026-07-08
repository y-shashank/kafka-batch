# frozen_string_literal: true

module KafkaBatch
  module Executors
    # Runs a registered Ruby Worker class (#perform).
    class Ruby
      def initialize(worker_class)
        @worker_class = worker_class
      end

      def call(context)
        worker = @worker_class.new
        worker.bind_job_context!(context.data, worker_class: @worker_class)
        worker.perform(context.payload)
      end
    end
  end
end
