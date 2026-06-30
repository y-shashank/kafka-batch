require "securerandom"
require "oj"
require "time"

module KafkaBatch
  # Entry point for creating and enqueueing batches of jobs.
  #
  # There is NO explicit lock step. A batch stays OPEN and accepts more jobs –
  # from anywhere, including from jobs that belong to the batch – until it
  # COMPLETES (all jobs done → on_complete fires) or is cancelled. Pushing into a
  # completed or cancelled batch raises BatchClosedError.
  #
  # The completion callback fires automatically the moment the batch drains
  # (completed + failed >= total_jobs). This is safe for the common patterns:
  #
  # 1) Block form (recommended) – the batch can't complete mid-population because
  #    it is held open until the block returns:
  #
  #      KafkaBatch::Batch.create(on_complete: "MyCallback") do |b|
  #        User.find_each { |u| b.push(ProcessUserWorker, "user_id" => u.id) }
  #      end
  #
  # 2) Jobs adding jobs – a running job is itself a pending unit, so the batch
  #    cannot drain while it runs. It may push children into its own batch and
  #    they are counted before the parent's completion is recorded:
  #
  #      def perform(payload)
  #        batch.push(ChildWorker, ...) if more_work?   # batch == this job's batch
  #      end
  #
  # Bare create without a block returns the Batch so you can push incrementally,
  # but note it can complete as soon as it drains – if every pushed job finishes
  # before you push more, the callback fires and further pushes raise
  # BatchClosedError. Prefer the block form for one-shot population.
  #
  # Standalone jobs (no batch context):
  #
  #   KafkaBatch::Batch.enqueue(ProcessUserWorker, user_id: 42)
  #
  class Batch
    attr_reader :id

    # @param tenant_id [String, nil] default tenant for jobs pushed into this
    #   batch (used by the multi-tenant fairness scheduler). Each push can
    #   override it.
    def initialize(on_success: nil, on_complete: nil, meta: {}, description: nil, tenant_id: nil, id: nil)
      @id          = id || SecureRandom.uuid
      @on_success  = on_success
      @on_complete = on_complete
      @meta        = meta
      @description = description
      @tenant_id   = tenant_id
    end

    # Create a new batch (persisted immediately with total_jobs = 0).
    #
    # With a block (recommended): the batch is held open for the duration of the
    # block so it cannot complete mid-population; when the block returns the
    # batch is sealed and finalizes if already drained. Returns the Batch.
    #
    # Without a block: the batch is sealed immediately and will complete as soon
    # as it drains. Returns the Batch for incremental pushing.
    def self.create(on_success: nil, on_complete: nil, meta: {}, description: nil, tenant_id: nil)
      batch = new(on_success: on_success, on_complete: on_complete, meta: meta, description: description, tenant_id: tenant_id)
      KafkaBatch.store.create_batch(
        id:          batch.id,
        total_jobs:  0,
        on_success:  on_success,
        on_complete: on_complete,
        meta:        meta,
        description: description,
        tenant_id:   tenant_id,
        # Block form: hold the completion gate shut until population finishes.
        sealed:      !block_given?
      )

      # #22: emit batch_created event after the record is durably persisted.
      KafkaBatch::Instrumentation.batch_created(
        batch_id:    batch.id,
        description: description,
        tenant_id:   tenant_id,
        on_success:  on_success,
        on_complete: on_complete
      )

      if block_given?
        yield batch
        batch.send(:seal!)  # population done – open the completion gate
      end

      batch
    end

    # Re-attach to an existing (open) batch, e.g. in another process or from a
    # running job, so you can push more jobs. Raises BatchNotFoundError if it
    # doesn't exist. @return [Batch]
    def self.open(id)
      data = KafkaBatch.store.find_batch(id)
      raise BatchNotFoundError, "Batch #{id} not found" unless data

      # Bug #13 fix: restore tenant_id from the store record so that jobs pushed
      # via Batch.open inherit the batch-level tenant without re-specifying it.
      new(
        id:          id,
        on_success:  data[:on_success],
        on_complete: data[:on_complete],
        meta:        data[:meta],
        description: data[:description],
        tenant_id:   data[:tenant_id]
      )
    end

    # Push a job into this (open) batch: atomically grows total_jobs and produces
    # the job. Raises BatchClosedError if the batch has completed or been
    # cancelled. @return [String] job_id
    def push(worker_class, payload = {}, job_id: SecureRandom.uuid, tenant_id: nil)
      validate_worker!(worker_class)
      reserve!(1)

      begin
        produce_job(worker_class, payload, job_id, tenant_id || @tenant_id)
      rescue StandardError
        KafkaBatch.store.add_jobs(@id, -1) rescue nil  # roll back the reserved count
        raise
      end

      job_id
    end

    # Push many jobs (same worker class) into this open batch in one call.
    # total_jobs is grown by payloads.size with a single atomic store write, then
    # ALL jobs are produced in one produce_many_sync call so librdkafka can
    # pipeline them into one (or a few) Kafka MessageSets. This is the equivalent
    # of Sidekiq's push_bulk: one broker round-trip instead of N.
    #
    #   batch.push_many(ProcessUserWorker, users.map { |u| { "user_id" => u.id } })
    #
    # @param payloads [Array<Hash>] one payload per job
    # @return [Array<String>] the job ids, in order
    def push_many(worker_class, payloads, tenant_id: nil)
      validate_worker!(worker_class)
      payloads = payloads.to_a
      return [] if payloads.empty?

      reserve!(payloads.size)

      tid      = tenant_id || @tenant_id
      fair     = worker_class.fairness?
      topic    = fair ? KafkaBatch.config.fairness_ingest_topic : worker_class.kafka_topic

      job_ids  = []
      messages = payloads.map do |payload|
        job_id  = SecureRandom.uuid
        job_ids << job_id
        message = self.class.build_message(
          worker_class: worker_class, payload: payload,
          job_id: job_id, batch_id: @id, attempt: 0, tenant_id: tid
        )
        # Fairness: key by tenant so one partition per tenant in the ingest topic.
        # Normal: key by job_id for even partition spread.
        key = fair ? (tid || @id).to_s : job_id
        { topic: topic, payload: message, key: key }
      end

      begin
        KafkaBatch::Producer.produce_many_sync(messages)
      rescue StandardError
        # On broker failure the idempotent producer will have sent zero or all
        # messages (partial produce is extremely rare with acks:all). Roll back
        # the full reservation; the reconciler will catch any edge-case mismatch.
        #
        # Bug #2 fix: if the rollback itself fails, total_jobs is overstated and
        # the batch can never drain. Force-cancel it so it doesn't hang forever.
        begin
          KafkaBatch.store.add_jobs(@id, -payloads.size)
        rescue StandardError => rollback_err
          KafkaBatch.logger.error(
            "[KafkaBatch][Batch] push_many rollback failed for batch_id=#{@id}: " \
            "#{rollback_err.message}. Cancelling batch to prevent it from hanging."
          )
          KafkaBatch.store.update_batch_status(@id, "cancelled") rescue nil
        end
        raise
      end

      job_ids
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
    def self.enqueue(worker_class, payload = {}, job_id: SecureRandom.uuid, tenant_id: nil)
      unless worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
        raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
      end

      message = build_message(
        worker_class: worker_class, payload: payload,
        job_id: job_id, batch_id: nil, attempt: 0, tenant_id: tenant_id
      )
      if worker_class.fairness?
        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.fairness_ingest_topic,
          payload: message,
          key:     (tenant_id || job_id).to_s
        )
      else
        KafkaBatch::Producer.produce_sync(
          topic: worker_class.kafka_topic, payload: message, key: job_id
        )
      end
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

    # #27 note: `enqueued_at` is stamped by the producer pod's wall clock (UTC).
    # Across pods this is subject to NTP jitter (typically ±10–100 ms). For ordering
    # purposes the Kafka message's broker-assigned timestamp (LogAppendTime) is a
    # more reliable monotonic reference within a partition. `enqueued_at` is useful
    # for human display and approximate age, but should not be used for strict
    # sequencing across concurrent producers.
    def self.build_message(worker_class:, payload:, job_id:, batch_id:, attempt:, tenant_id: nil)
      msg = {
        "job_id"                 => job_id,
        "batch_id"               => batch_id,
        "worker_class"           => worker_class.name,
        "payload"                => payload,
        "attempt"                => attempt,
        "max_retries"            => worker_class.max_retries,
        "complete_after_retries" => worker_class.complete_after_retries,
        "enqueued_at"            => Time.now.utc.iso8601
      }
      msg["tenant_id"]  = tenant_id            if tenant_id
      msg["retry_tier"] = worker_class.retry_tier.to_s if worker_class.retry_tier
      msg
    end

    private

    def validate_worker!(worker_class)
      return if worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
      raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
    end

    # Atomically grow total_jobs by +count+, raising if the batch can't accept jobs.
    def reserve!(count)
      case KafkaBatch.store.add_jobs(@id, count)
      when :closed
        raise BatchClosedError, "Batch #{@id} has already completed – no new jobs may be pushed"
      when :cancelled
        raise BatchClosedError, "Batch #{@id} is cancelled – no new jobs may be pushed"
      when :not_found
        raise BatchNotFoundError, "Batch #{@id} not found"
      end
    end

    # Open the completion gate after block-form population finishes. If the batch
    # already drained while the block ran, this finalizes it and fires the
    # callback now. Internal – there is no public lock step.
    def seal!
      result = KafkaBatch.store.seal_batch(@id)
      case result[:status]
      when :not_found
        raise BatchNotFoundError, "Batch #{@id} not found"
      when :done
        produce_callback(result[:batch], result[:outcome])
      end
      # #22: emit batch_sealed after population is durably committed.
      KafkaBatch::Instrumentation.batch_sealed(
        batch_id:   @id,
        total_jobs: KafkaBatch.store.find_batch(@id)&.dig(:total_jobs) || 0
      )
      self
    end

    def produce_job(worker_class, payload, job_id, tenant_id = nil)
      message = self.class.build_message(
        worker_class: worker_class, payload: payload,
        job_id: job_id, batch_id: @id, attempt: 0, tenant_id: tenant_id
      )

      if worker_class.fairness?
        # Land on the ingest topic keyed by tenant (per-tenant ordering); the
        # Dispatcher fairly schedules from there onto the ready topic.
        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.fairness_ingest_topic,
          payload: message,
          key:     (tenant_id || @id).to_s
        )
      else
        KafkaBatch::Producer.produce_sync(
          topic: worker_class.kafka_topic, payload: message, key: job_id
        )
      end
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
