require "securerandom"
require "oj"
require "time"

module KafkaBatch
  # Entry point for creating and enqueueing batches of jobs.
  #
  # Batches are OPEN by default: you may push jobs into them at any time and from
  # anywhere (even in a different process after Batch.find/open). Each push
  # atomically grows the batch's total_jobs and produces the job immediately.
  # Completion callbacks DO NOT fire until you call #lock, after which no further
  # jobs may be pushed (push raises BatchLockedError).
  #
  # Incremental usage (push 1000 at a time, across processes):
  #
  #   batch = KafkaBatch::Batch.create(on_complete: "MyCallback")
  #   batch.id  # => persist/pass this around
  #
  #   # ...later / elsewhere...
  #   batch = KafkaBatch::Batch.open(batch_id)
  #   users.each_slice(1000) do |slice|
  #     slice.each { |u| batch.push(ProcessUserWorker, "user_id" => u.id) }
  #   end
  #
  #   # when everything has been pushed:
  #   KafkaBatch::Batch.open(batch_id).lock
  #
  # Convenience block form (auto-locks when the block returns):
  #
  #   KafkaBatch::Batch.create(on_complete: "MyCallback") do |b|
  #     User.find_each { |u| b.push(ProcessUserWorker, "user_id" => u.id) }
  #   end
  #
  # Standalone jobs (no batch context):
  #
  #   KafkaBatch::Batch.enqueue(ProcessUserWorker, user_id: 42)
  #
  class Batch
    attr_reader :id

    def initialize(on_success: nil, on_complete: nil, meta: {}, id: nil)
      @id          = id || SecureRandom.uuid
      @on_success  = on_success
      @on_complete = on_complete
      @meta        = meta
    end

    # Create a new OPEN batch (persisted immediately with total_jobs = 0).
    # Without a block: returns the Batch instance for incremental pushing.
    # With a block: yields the batch, then locks it, and returns the batch id
    # (backwards-compatible convenience form).
    def self.create(on_success: nil, on_complete: nil, meta: {})
      batch = new(on_success: on_success, on_complete: on_complete, meta: meta)
      KafkaBatch.store.create_batch(
        id:          batch.id,
        total_jobs:  0,
        on_success:  on_success,
        on_complete: on_complete,
        meta:        meta,
        locked:      false
      )

      if block_given?
        yield batch
        batch.lock
        batch.id
      else
        batch
      end
    end

    # Re-attach to an existing batch (e.g. in another process) so you can push
    # more jobs or lock it. Raises BatchNotFoundError if it doesn't exist.
    # @return [Batch]
    def self.open(id)
      data = KafkaBatch.store.find_batch(id)
      raise BatchNotFoundError, "Batch #{id} not found" unless data

      new(
        id:          id,
        on_success:  data[:on_success],
        on_complete: data[:on_complete],
        meta:        data[:meta]
      )
    end

    # Push a job into this (open) batch: atomically grows total_jobs and produces
    # the job. Raises BatchLockedError if the batch is already locked/cancelled.
    # @return [String] job_id
    def push(worker_class, payload = {}, job_id: SecureRandom.uuid)
      validate_worker!(worker_class)
      reserve!(1)

      begin
        produce_job(worker_class, payload, job_id)
      rescue StandardError
        KafkaBatch.store.add_jobs(@id, -1) rescue nil  # roll back the reserved count
        raise
      end

      job_id
    end

    # Push many jobs (same worker class) into this open batch in one call.
    # total_jobs is grown by payloads.size with a single atomic store write, then
    # each job is produced. Raises BatchLockedError if locked/cancelled.
    #
    #   batch.push_many(ProcessUserWorker, users.map { |u| { "user_id" => u.id } })
    #
    # @param payloads [Array<Hash>] one payload per job
    # @return [Array<String>] the job ids, in order
    def push_many(worker_class, payloads)
      validate_worker!(worker_class)
      payloads = payloads.to_a
      return [] if payloads.empty?

      reserve!(payloads.size)

      job_ids  = []
      produced = 0
      begin
        payloads.each do |payload|
          job_id = SecureRandom.uuid
          produce_job(worker_class, payload, job_id)
          job_ids << job_id
          produced += 1
        end
      rescue StandardError
        # Roll back only the jobs we didn't manage to produce.
        remainder = payloads.size - produced
        (KafkaBatch.store.add_jobs(@id, -remainder) rescue nil) if remainder.positive?
        raise
      end

      job_ids
    end

    # Lock the batch: no further jobs may be pushed. If all pushed jobs have
    # already finished, the batch finalizes now and its callback is dispatched.
    # @return [self]
    def lock
      result = KafkaBatch.store.lock_batch(@id)
      case result[:status]
      when :not_found
        raise BatchNotFoundError, "Batch #{@id} not found"
      when :done
        produce_callback(result[:batch], result[:outcome])
      end
      self
    end

    # Look up an existing batch by id. @return [Hash, nil]
    def self.find(id)
      KafkaBatch.store.find_batch(id)
    end

    # Cancel a batch: remaining jobs are skipped and callbacks never fire.
    def self.cancel(id)
      KafkaBatch.store.update_batch_status(id, "cancelled")
    end

    # Enqueue a single job outside of any batch context. @return [String] job_id
    def self.enqueue(worker_class, payload = {}, job_id: SecureRandom.uuid)
      unless worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
        raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
      end

      message = build_message(
        worker_class: worker_class, payload: payload,
        job_id: job_id, batch_id: nil, attempt: 0
      )
      KafkaBatch::Producer.produce_sync(
        topic: worker_class.kafka_topic, payload: message, key: job_id
      )
      job_id
    end

    # Re-enqueue a job (called internally by JobConsumer on retry).
    def self.reenqueue(topic:, message:, next_attempt:)
      KafkaBatch::Producer.produce_sync(
        topic:   topic,
        payload: message.merge("attempt" => next_attempt),
        key:     message["job_id"]
      )
    end

    def self.build_message(worker_class:, payload:, job_id:, batch_id:, attempt:)
      {
        "job_id"        => job_id,
        "batch_id"      => batch_id,
        "worker_class"  => worker_class.name,
        "payload"       => payload,
        "attempt"       => attempt,
        "max_retries"   => worker_class.max_retries,
        "retry_backoff" => worker_class.retry_backoff,
        "enqueued_at"   => Time.now.iso8601
      }
    end

    private

    def validate_worker!(worker_class)
      return if worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
      raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
    end

    # Atomically grow total_jobs by +count+, raising if the batch can't accept jobs.
    def reserve!(count)
      case KafkaBatch.store.add_jobs(@id, count)
      when :locked
        raise BatchLockedError, "Batch #{@id} is locked – no new jobs may be pushed"
      when :cancelled
        raise BatchLockedError, "Batch #{@id} is cancelled – no new jobs may be pushed"
      when :not_found
        raise BatchNotFoundError, "Batch #{@id} not found"
      end
    end

    def produce_job(worker_class, payload, job_id)
      message = self.class.build_message(
        worker_class: worker_class, payload: payload,
        job_id: job_id, batch_id: @id, attempt: 0
      )
      KafkaBatch::Producer.produce_sync(
        topic: worker_class.kafka_topic, payload: message, key: job_id
      )
    end

    # Produce the callback message when locking finalizes the batch (mirrors
    # EventConsumer#trigger_callbacks). The CallbackConsumer dedupes via its claim.
    def produce_callback(batch, outcome)
      KafkaBatch::Producer.produce_sync(
        topic:   KafkaBatch.config.callbacks_topic,
        payload: {
          "batch_id"        => batch[:id],
          "outcome"         => outcome,
          "total_jobs"      => batch[:total_jobs],
          "completed_count" => batch[:completed_count],
          "failed_count"    => batch[:failed_count],
          "on_success"      => batch[:on_success],
          "on_complete"     => batch[:on_complete],
          "meta"            => batch[:meta],
          "finished_at"     => batch[:finished_at] || Time.now.iso8601
        },
        key: batch[:id]
      )
    end
  end
end
