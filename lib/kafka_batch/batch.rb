require "securerandom"
require "oj"
require "time"

module KafkaBatch
  # Entry point for creating and enqueueing a batch of jobs.
  #
  # Usage:
  #
  #   batch_id = KafkaBatch::Batch.create(
  #     on_success: "MySuccessCallback",
  #     on_complete: "MyCompleteCallback"
  #   ) do |b|
  #     User.find_each { |u| b.push(ProcessUserWorker, user_id: u.id) }
  #   end
  #
  # The block receives a Batch instance.  Call #push for each job.
  # Jobs are buffered until the block returns; the store record is written
  # with the exact total before any Kafka messages are produced, ensuring
  # the batch can never "complete" before all jobs are enqueued.
  #
  # Standalone jobs (no batch context):
  #
  #   KafkaBatch::Batch.enqueue(ProcessUserWorker, user_id: 42)
  #
  class Batch
    attr_reader :id

    # @param on_success  [String, nil]  Callback worker called when ALL jobs succeed
    # @param on_complete [String, nil]  Callback worker called when ALL jobs finish
    # @param meta        [Hash]         Arbitrary metadata stored with the batch
    def initialize(on_success: nil, on_complete: nil, meta: {})
      @id          = SecureRandom.uuid
      @on_success  = on_success
      @on_complete = on_complete
      @meta        = meta
      @pending     = []           # buffered [worker_class, payload, job_id]
    end

    # Buffer a job for inclusion in this batch.
    # @param worker_class [Class]   a class that includes KafkaBatch::Worker
    # @param payload      [Hash]    arguments passed to worker#perform
    # @param job_id       [String]  optional explicit job ID (auto-generated if omitted)
    def push(worker_class, payload = {}, job_id: SecureRandom.uuid)
      unless worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
        raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
      end
      @pending << { worker_class: worker_class, payload: payload, job_id: job_id }
    end

    # Create a batch, yield a builder, then flush all buffered jobs to Kafka.
    # @return [String] batch ID
    def self.create(on_success: nil, on_complete: nil, meta: {}, &block)
      batch = new(on_success: on_success, on_complete: on_complete, meta: meta)
      yield batch
      batch.send(:flush!)
      batch.id
    end

    # Look up an existing batch by id.
    # @param id [String] batch UUID
    # @return [Hash, nil]
    def self.find(id)
      KafkaBatch.store.find_batch(id)
    end

    # Cancel a running batch.
    # Sets status to "cancelled" in the store.  Any jobs already in-flight will
    # still complete but callback dispatch will be skipped because the batch is
    # no longer in a terminal-without-callback state.
    # @param id [String] batch UUID
    def self.cancel(id)
      KafkaBatch.store.update_batch_status(id, "cancelled")
    end

    # Enqueue a single job outside of any batch context.
    # @param worker_class [Class]
    # @param payload      [Hash]
    # @param job_id       [String]
    # @return [String] job_id
    def self.enqueue(worker_class, payload = {}, job_id: SecureRandom.uuid)
      unless worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
        raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
      end

      message = build_message(
        worker_class: worker_class,
        payload:      payload,
        job_id:       job_id,
        batch_id:     nil,
        attempt:      0
      )

      KafkaBatch::Producer.produce_sync(
        topic:   worker_class.kafka_topic,
        payload: message,
        key:     job_id
      )

      job_id
    end

    # Re-enqueue a job (called internally by JobConsumer on retry).
    # @param topic        [String]
    # @param message      [Hash]   the original decoded message
    # @param next_attempt [Integer]
    def self.reenqueue(topic:, message:, next_attempt:)
      KafkaBatch::Producer.produce_sync(
        topic:   topic,
        payload: message.merge("attempt" => next_attempt),
        key:     message["job_id"]
      )
    end

    private

    def flush!
      if @pending.empty?
        KafkaBatch.logger.warn("[KafkaBatch] Batch #{@id} created with 0 jobs – skipping")
        return
      end

      # Write batch record with the exact job count BEFORE producing any
      # messages.  This prevents a fast consumer from marking the batch
      # complete before all messages are enqueued.
      KafkaBatch.store.create_batch(
        id:          @id,
        total_jobs:  @pending.size,
        on_success:  @on_success,
        on_complete: @on_complete,
        meta:        @meta
      )

      # Produce all job messages.  If Kafka fails mid-way, roll back the
      # store record so the batch doesn't hang in "running" forever with
      # fewer jobs than total_jobs.
      produced = 0
      begin
        @pending.each do |entry|
          message = self.class.build_message(
            worker_class: entry[:worker_class],
            payload:      entry[:payload],
            job_id:       entry[:job_id],
            batch_id:     @id,
            attempt:      0
          )

          KafkaBatch::Producer.produce_sync(
            topic:   entry[:worker_class].kafka_topic,
            payload: message,
            key:     entry[:job_id]
          )
          produced += 1
        end
      rescue StandardError => e
        KafkaBatch.logger.error(
          "[KafkaBatch] Batch #{@id} produce failed after #{produced}/#{@pending.size} jobs – " \
          "rolling back store record (#{e.class}: #{e.message})"
        )
        begin
          KafkaBatch.store.delete_batch(@id)
        rescue => store_err
          KafkaBatch.logger.error(
            "[KafkaBatch] Batch #{@id} store rollback also failed: #{store_err.message}. " \
            "Batch is stuck – run rake kafka_batch:reconcile to clean up."
          )
        end
        raise  # re-raise so the caller knows the batch wasn't created
      end

      KafkaBatch.logger.info("[KafkaBatch] Batch #{@id} created with #{@pending.size} jobs")
    end

    def self.build_message(worker_class:, payload:, job_id:, batch_id:, attempt:)
      {
        "job_id"       => job_id,
        "batch_id"     => batch_id,
        "worker_class" => worker_class.name,
        "payload"      => payload,
        "attempt"      => attempt,
        "max_retries"  => worker_class.max_retries,
        "retry_backoff"=> worker_class.retry_backoff,
        "enqueued_at"  => Time.now.iso8601
      }
    end
  end
end
