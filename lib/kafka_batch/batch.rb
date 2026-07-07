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
        block_error = nil
        begin
          yield batch
        rescue StandardError => e
          block_error = e
        ensure
          # Always seal after block form — even when the block raises — so jobs
          # already pushed can still finalize and fire callbacks.
          batch.send(:seal!)
        end
        raise block_error if block_error
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
    # cancelled. Returns nil when the worker opts into uniq and a duplicate is
    # skipped (config.uniq_on_duplicate :skip). @return [String, nil] job_id
    def push(worker_class, payload = {}, job_id: SecureRandom.uuid, tenant_id: nil, valid_till: nil)
      validate_worker!(worker_class)
      return nil if self.class.uniq_duplicate?(worker_class, payload, job_id: job_id, batch_id: @id)

      batch_seq = reserve!(1)

      begin
        produce_job(worker_class, payload, job_id, tenant_id || @tenant_id,
                    batch_seq: batch_seq, valid_till: valid_till)
      rescue StandardError
        self.class.release_uniq!(worker_class, payload, job_id: job_id)
        KafkaBatch.store.add_jobs(@id, -1) rescue nil  # roll back the reserved count
        raise
      end

      job_id
    end

    # Push many jobs (same worker class) into this open batch in one call.
    # total_jobs is grown by payloads.size with a single atomic store write, then
    # jobs are produced in sequential produce_many_sync chunks so librdkafka can
    # pipeline each chunk into one (or a few) Kafka MessageSets. This is the
    # equivalent of Sidekiq's push_bulk: one broker round-trip per chunk instead
    # of N per-message round-trips.
    #
    #   batch.push_many(ProcessUserWorker, users.map { |u| { "user_id" => u.id } })
    #
    # @param payloads [Array<Hash>] one payload per job
    # @return [Array<String, nil>] the job ids, in order (nil = uniq duplicate skipped)
    def push_many(worker_class, payloads, tenant_id: nil, valid_till: nil)
      validate_worker!(worker_class)
      payloads = payloads.to_a
      return [] if payloads.empty?

      tid     = tenant_id || @tenant_id
      entries = []
      job_ids = []

      payloads.each do |payload|
        job_id = SecureRandom.uuid
        if self.class.uniq_duplicate?(worker_class, payload, job_id: job_id, batch_id: @id)
          job_ids << nil
          next
        end
        entries << [payload, job_id]
        job_ids << job_id
      end
      return job_ids if entries.empty?

      reserve!(entries.size)

      messages = entries.map do |payload, job_id|
        message = self.class.build_message(
          worker_class: worker_class, payload: payload,
          job_id: job_id, batch_id: @id, attempt: 0, tenant_id: tid,
          batch_seq: next_batch_seq!, valid_till: valid_till
        )
        route = self.class.route_for(worker_class, job_id: job_id, tenant_id: tid, batch_id: @id)
        msg   = { topic: route[:topic], payload: message }
        if route[:partition]
          msg[:partition] = route[:partition]
        else
          msg[:key] = route[:key]
        end
        msg
      end

      begin
        self.class.produce_in_chunks!(messages)
      rescue KafkaBatch::PartialProduceError => e
        rollback_unproduced_jobs!(entries, e.produced_count || 0, worker_class)
        raise
      end

      job_ids
    end

    # Push a DELAYED job into this (open) batch, to run at an absolute time. Like
    # #push, it grows total_jobs immediately (reserve!) so the batch's completion
    # gate stays shut until the delayed job actually runs and completes — matching
    # Sidekiq-Pro `batch.jobs { W.perform_at(...) }` semantics. @return [String, nil] job_id
    def push_at(time, worker_class, payload = {}, job_id: SecureRandom.uuid, tenant_id: nil, valid_till: nil)
      validate_worker!(worker_class)
      return nil if self.class.uniq_duplicate?(worker_class, payload, job_id: job_id, batch_id: @id)

      batch_seq = reserve!(1)

      begin
        tid     = tenant_id || @tenant_id
        message = self.class.build_message(
          worker_class: worker_class, payload: payload,
          job_id: job_id, batch_id: @id, attempt: 0, tenant_id: tid,
          batch_seq: batch_seq, valid_till: valid_till
        )
        self.class.schedule_message(
          message, run_at: self.class.clamp_run_at(self.class.to_time(time)), batch_id: @id
        )
      rescue StandardError
        self.class.release_uniq!(worker_class, payload, job_id: job_id)
        KafkaBatch.store.add_jobs(@id, -1) rescue nil  # roll back the reserved count
        raise
      end

      job_id
    end

    # Push a delayed job to run after +interval+ seconds. @return [String] job_id
    def push_in(interval, worker_class, payload = {}, **opts)
      push_at(Time.now + interval, worker_class, payload, **opts)
    end

    # Push MANY delayed jobs (same worker) into this open batch, all due at one
    # absolute time. Grows total_jobs by payloads.size with a single atomic store
    # write, produces all payloads to the scheduled topic in one round-trip, and
    # bulk-writes their pointers — the delayed equivalent of #push_many. The batch
    # won't complete until every scheduled job has run. @return [Array<String, nil>] job ids
    def push_many_at(time, worker_class, payloads, tenant_id: nil, valid_till: nil)
      validate_worker!(worker_class)
      payloads = payloads.to_a
      return [] if payloads.empty?

      tid      = tenant_id || @tenant_id
      run_at   = self.class.clamp_run_at(self.class.to_time(time))
      entries  = []
      job_ids  = []

      payloads.each do |payload|
        job_id = SecureRandom.uuid
        if self.class.uniq_duplicate?(worker_class, payload, job_id: job_id, batch_id: @id)
          job_ids << nil
          next
        end
        entries << self.class.build_message(
          worker_class: worker_class, payload: payload,
          job_id: job_id, batch_id: @id, attempt: 0, tenant_id: tid,
          batch_seq: nil, valid_till: valid_till
        )
        job_ids << job_id
      end
      return job_ids if entries.empty?

      reserve!(entries.size)

      begin
        messages = entries.each_with_index.map do |message, _i|
          message.merge("batch_seq" => next_batch_seq!)
        end
        self.class.schedule_messages(messages, run_at: run_at, batch_id: @id)
      rescue KafkaBatch::PartialProduceError => e
        rollback_unproduced_batch_messages!(entries, e.produced_count || 0, worker_class)
        raise
      rescue StandardError
        rollback_unproduced_batch_messages!(entries, 0, worker_class)
        raise
      end

      job_ids
    end

    # Push many delayed jobs to run after +interval+ seconds. @return [Array<String>]
    def push_many_in(interval, worker_class, payloads, **opts)
      push_many_at(Time.now + interval, worker_class, payloads, **opts)
    end

    # Look up an existing batch by id. @return [Hash, nil]
    def self.find(id)
      KafkaBatch.store.find_batch(id)
    end

    # Cancel a batch: remaining jobs are skipped and callbacks never fire.
    def self.cancel(id)
      KafkaBatch.store.update_batch_status(id, "cancelled")
    end

    # Enqueue a single job outside of any batch context. @return [String, nil] job_id
    def self.enqueue(worker_class, payload = {}, job_id: SecureRandom.uuid, tenant_id: nil, valid_till: nil)
      ensure_worker!(worker_class)
      return nil if uniq_duplicate?(worker_class, payload, job_id: job_id)

      message = build_message(
        worker_class: worker_class, payload: payload,
        job_id: job_id, batch_id: nil, attempt: 0, tenant_id: tenant_id,
        valid_till: valid_till
      )
      route = route_for(worker_class, job_id: job_id, tenant_id: tenant_id)
      begin
        KafkaBatch::Producer.produce_sync(
          topic: route[:topic], payload: message, key: route[:key], partition: route[:partition]
        )
      rescue StandardError
        release_uniq!(worker_class, payload, job_id: job_id)
        raise
      end
      job_id
    end

    # Enqueue a single job to run at an absolute time (Sidekiq perform_at).
    # The payload is produced to the durable scheduled_topic and a compact pointer
    # is stored in the delayed-job index; the SchedulePoller re-produces it onto
    # the worker's real topic when due. @return [String, nil] job_id
    def self.enqueue_at(time, worker_class, payload = {}, job_id: SecureRandom.uuid, tenant_id: nil, valid_till: nil)
      ensure_worker!(worker_class)
      return nil if uniq_duplicate?(worker_class, payload, job_id: job_id)

      message = build_message(
        worker_class: worker_class, payload: payload,
        job_id: job_id, batch_id: nil, attempt: 0, tenant_id: tenant_id,
        valid_till: valid_till
      )
      begin
        schedule_message(message, run_at: clamp_run_at(to_time(time)), batch_id: nil)
      rescue StandardError
        release_uniq!(worker_class, payload, job_id: job_id)
        raise
      end
      job_id
    end

    # Enqueue a single job to run after +interval+ seconds (Sidekiq perform_in).
    def self.enqueue_in(interval, worker_class, payload = {}, **opts)
      enqueue_at(Time.now + interval, worker_class, payload, **opts)
    end

    # Bulk-enqueue many standalone jobs (same worker) to run at one absolute time.
    # Delayed push_bulk: one produce_many_sync + one bulk index write.
    # @param payloads [Array<Hash>] one payload per job
    # @return [Array<String, nil>] the job ids, in order (nil = uniq duplicate skipped)
    def self.enqueue_many_at(time, worker_class, payloads, tenant_id: nil, valid_till: nil)
      ensure_worker!(worker_class)
      payloads = payloads.to_a
      return [] if payloads.empty?

      run_at   = clamp_run_at(to_time(time))
      messages = []
      job_ids  = []

      payloads.each do |payload|
        job_id = SecureRandom.uuid
        if uniq_duplicate?(worker_class, payload, job_id: job_id)
          job_ids << nil
          next
        end
        messages << build_message(
          worker_class: worker_class, payload: payload,
          job_id: job_id, batch_id: nil, attempt: 0, tenant_id: tenant_id,
          valid_till: valid_till
        )
        job_ids << job_id
      end
      return job_ids if messages.empty?

      begin
        schedule_messages(messages, run_at: run_at, batch_id: nil)
      rescue KafkaBatch::PartialProduceError => e
        release_unproduced_message_locks!(messages, e.produced_count || 0, worker_class)
        raise
      rescue StandardError
        release_unproduced_message_locks!(messages, 0, worker_class)
        raise
      end
      job_ids
    end

    # Bulk-enqueue many standalone jobs to run after +interval+ seconds.
    def self.enqueue_many_in(interval, worker_class, payloads, **opts)
      enqueue_many_at(Time.now + interval, worker_class, payloads, **opts)
    end

    # Resolve the destination topic / partition key for a job. Single source of
    # truth shared by immediate enqueue (#enqueue, #produce_job) and the delayed
    # SchedulePoller so scheduled jobs route identically to immediate ones.
    #   fair worker  → fairness ingest topic (explicit tenant partition, else
    #                  key-hash by tenant_id → batch_id → job_id)
    #   plain worker → worker.kafka_topic, keyed by job_id
    # @return [Hash] { topic:, key:, partition: }
    def self.route_for(worker_class, job_id:, tenant_id: nil, batch_id: nil)
      if worker_class.fairness?
        type   = worker_class.fairness_type
        ingest = KafkaBatch.config.fairness_ingest_topic(type)
        explicit = KafkaBatch.tenant_ingest_partition(tenant_id, type)
        if explicit
          { topic: ingest, key: nil, partition: explicit }
        else
          { topic: ingest, key: (tenant_id || batch_id || job_id).to_s, partition: nil }
        end
      else
        { topic: worker_class.kafka_topic, key: job_id, partition: nil }
      end
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
    # Produce a job payload to the durable scheduled_topic and record a compact
    # pointer (job_id:partition:offset) in the delayed-job index, scored by run_at.
    # The payload lives in Kafka; the index stays small (see Schedule::Base).
    def self.schedule_message(message, run_at:, batch_id:)
      store = KafkaBatch.schedule_store
      raise ConfigurationError, "schedule_store is not available" unless store

      report = KafkaBatch::Producer.produce_sync(
        topic:   KafkaBatch.config.scheduled_topic,
        payload: message,
        key:     message["job_id"]
      )
      partition, offset = delivery_coords(report)

      store.schedule(
        job_id:    message["job_id"],
        run_at:    run_at,
        partition: partition,
        offset:    offset,
        batch_id:  batch_id
      )

      KafkaBatch::Instrumentation.scheduled_enqueued(
        job_id:       message["job_id"],
        batch_id:     batch_id,
        worker_class: message["worker_class"],
        run_at:       run_at
      )
      message["job_id"]
    end

    # Bulk variant of #schedule_message: produce payloads to the scheduled topic
    # in sequential produce_many_sync chunks, then bulk-write pointers per chunk.
    # All jobs share +run_at+. On partial failure the delivered prefix is indexed
    # before raising so scheduled jobs are not orphaned on the topic.
    # @param messages [Array<Hash>] built job messages (each has "job_id")
    # @return [Array<String>] job ids, in order
    # @raise [KafkaBatch::PartialProduceError] when produce fails mid-batch;
    #   +produced_count+ reflects the gap-free prefix that was indexed
    def self.schedule_messages(messages, run_at:, batch_id:)
      store = KafkaBatch.schedule_store
      raise ConfigurationError, "schedule_store is not available" unless store
      return [] if messages.empty?

      produce_msgs = messages.map do |m|
        { topic: KafkaBatch.config.scheduled_topic, payload: m, key: m["job_id"] }
      end

      produced_total = 0
      chunk_size     = push_many_chunk_size

      produce_msgs.each_slice(chunk_size) do |chunk|
        reports = KafkaBatch::Producer.produce_many_sync(chunk)
        unless reports.is_a?(Array) && reports.size == chunk.size
          raise KafkaBatch::ProducerError,
            "scheduled bulk produce returned #{reports.inspect} (expected #{chunk.size} delivery results)"
        end

        entries = build_schedule_entries(chunk, reports, run_at: run_at, batch_id: batch_id)
        begin
          store.schedule_many(entries)
        rescue StandardError => e
          raise KafkaBatch::PartialProduceError.new(
            "schedule index write failed: #{e.message}",
            dispatched:     [],
            produced_count: produced_total
          )
        end
        produced_total += chunk.size
      rescue KafkaBatch::PartialProduceError => e
        delivered = KafkaBatch::Producer.prefix_delivered_count(e.dispatched)
        if delivered.positive?
          prefix_chunk   = chunk.first(delivered)
          prefix_reports = e.dispatched.first(delivered)
          store.schedule_many(
            build_schedule_entries(prefix_chunk, prefix_reports, run_at: run_at, batch_id: batch_id)
          )
          produced_total += delivered
        end
        raise KafkaBatch::PartialProduceError.new(
          e.message,
          dispatched:     e.dispatched,
          produced_count: produced_total
        )
      end

      KafkaBatch::Instrumentation.scheduled_enqueued_bulk(
        count: produced_total, batch_id: batch_id,
        worker_class: messages.first["worker_class"], run_at: run_at
      )
      messages.map { |m| m["job_id"] }
    end

    # Produce Kafka messages in sequential chunks via produce_many_sync.
    # Preserves gap-free prefix semantics: on failure, indices 0..(n-1) are on
    # Kafka and indices n.. are not.
    #
    # @return [Integer] number of messages successfully delivered
    # @raise [KafkaBatch::PartialProduceError] +produced_count+ is the delivered prefix
    def self.produce_in_chunks!(messages)
      return 0 if messages.nil? || messages.empty?

      produced_total = 0
      chunk_size     = push_many_chunk_size

      messages.each_slice(chunk_size) do |chunk|
        KafkaBatch::Producer.produce_many_sync(chunk)
        produced_total += chunk.size
      rescue KafkaBatch::PartialProduceError => e
        delivered = KafkaBatch::Producer.prefix_delivered_count(e.dispatched)
        produced_total += delivered
        raise KafkaBatch::PartialProduceError.new(
          e.message,
          dispatched:     e.dispatched,
          produced_count: produced_total
        )
      end

      produced_total
    end

    def self.push_many_chunk_size
      size = KafkaBatch.config.push_many_chunk_size.to_i
      size < 1 ? 500 : size
    end

    def self.build_schedule_entries(chunk, reports, run_at:, batch_id:)
      chunk.each_with_index.map do |msg_hash, i|
        payload = msg_hash[:payload]
        partition, offset = delivery_coords(reports[i])
        {
          job_id:    payload["job_id"],
          run_at:    run_at,
          partition: partition,
          offset:    offset,
          batch_id:  batch_id
        }
      end
    end

    def self.release_unproduced_message_locks!(messages, produced_count, worker_class)
      messages.drop(produced_count.to_i).each do |message|
        release_uniq!(worker_class, message["payload"] || {}, job_id: message["job_id"])
      end
    end

    # Extract (partition, offset) from a WaterDrop/rdkafka delivery result.
    #
    # produce_sync returns a Rdkafka::Producer::DeliveryReport (responds to
    # #partition/#offset directly), but produce_many_sync returns an array of
    # Rdkafka::Producer::DeliveryHandle — each already in its final state, whose
    # #create_result yields the DeliveryReport. Normalize a handle to its report.
    def self.delivery_coords(result)
      report = result.respond_to?(:create_result) ? result.create_result : result
      if report.respond_to?(:partition) && report.respond_to?(:offset)
        [report.partition, report.offset]
      else
        raise KafkaBatch::ProducerError,
          "scheduled produce did not return delivery coordinates (got #{result.class})"
      end
    end

    # Clamp a run-at time to [now, now + max_schedule_horizon]. A horizon beyond
    # the scheduled_topic's retention would let a job point at a log-cleaned offset.
    def self.clamp_run_at(time)
      now = Time.now
      max = now + KafkaBatch.config.max_schedule_horizon.to_i
      return max if time > max
      return now if time < now

      time
    end

    def self.to_time(time)
      return time if time.is_a?(Time)
      return Time.at(time) if time.is_a?(Numeric)
      Time.parse(time.to_s)
    end

    def self.ensure_worker!(worker_class)
      return if worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
      raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
    end

    # @return [Boolean] true when the enqueue should be skipped as a duplicate
    def self.uniq_duplicate?(worker_class, payload, job_id:, batch_id: nil)
      return false unless worker_class.uniq? && KafkaBatch.config.uniq_enabled

      if KafkaBatch::Uniqueness.claim(worker_class, payload, job_id: job_id)
        false
      else
        KafkaBatch::Instrumentation.job_uniq_skipped(
          worker_class: worker_class, payload: payload, job_id: job_id, batch_id: batch_id
        )
        case KafkaBatch.config.uniq_on_duplicate
        when :raise
          raise DuplicateJobError.new(worker_class: worker_class, payload: payload)
        else
          true
        end
      end
    end

    def self.release_uniq!(worker_class, payload, job_id:)
      KafkaBatch::Uniqueness.release(worker_class, payload, job_id: job_id)
    end

    def self.build_message(worker_class:, payload:, job_id:, batch_id:, attempt:, tenant_id: nil, batch_seq: nil, valid_till: nil)
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
      msg["tenant_id"]   = tenant_id            if tenant_id
      msg["batch_seq"]   = batch_seq            if batch_seq && batch_id
      msg["retry_tier"]  = worker_class.retry_tier.to_s if worker_class.retry_tier
      normalized_till    = JobExpiry.normalize_valid_till(valid_till)
      msg["valid_till"]  = normalized_till      if normalized_till
      if worker_class.uniq? && KafkaBatch.config.uniq_enabled
        msg["_uniq_fp"] = KafkaBatch::Uniqueness.digest_hex(worker_class, payload)
      end
      msg
    end

    private

    def validate_worker!(worker_class)
      self.class.ensure_worker!(worker_class)
    end

    # Atomically grow total_jobs by +count+, raising if the batch can't accept jobs.
    # Returns the reserved 1-based batch_seq for a single-job reservation, or
    # sets @seq_cursor/@seq_end for bulk (#push_many) assignment.
    # @return [Integer, nil] batch_seq when count == 1
    def reserve!(count)
      result = KafkaBatch.store.add_jobs(@id, count)
      status, seq_start, seq_end = parse_add_jobs_result(result)

      case status
      when :closed
        raise BatchClosedError, "Batch #{@id} has already completed – no new jobs may be pushed"
      when :cancelled
        raise BatchClosedError, "Batch #{@id} is cancelled – no new jobs may be pushed"
      when :not_found
        raise BatchNotFoundError, "Batch #{@id} not found"
      end

      if seq_start
        @seq_cursor = seq_start
        @seq_end    = seq_end
        return @seq_cursor if count == 1
      else
        @seq_cursor = @seq_end = nil
      end

      nil
    end

    def next_batch_seq!
      raise BatchClosedError, "Batch #{@id} has no reserved batch_seq slots" unless @seq_cursor && @seq_end
      raise BatchClosedError, "Batch #{@id} reserved too few batch_seq slots" if @seq_cursor > @seq_end

      seq = @seq_cursor
      @seq_cursor += 1
      seq
    end

    def rollback_unproduced_jobs!(entries, produced_count, worker_class)
      produced   = produced_count.to_i
      unproduced = entries.size - produced
      return if unproduced <= 0

      entries.drop(produced).each do |payload, job_id|
        self.class.release_uniq!(worker_class, payload, job_id: job_id)
      end

      begin
        KafkaBatch.store.add_jobs(@id, -unproduced)
      rescue StandardError => rollback_err
        KafkaBatch.logger.error(
          "[KafkaBatch][Batch] push_many rollback failed for batch_id=#{@id}: " \
          "#{rollback_err.message}. Leaving total_jobs unchanged — jobs already " \
          "on Kafka will still complete."
        )
      end
    end

    def rollback_unproduced_batch_messages!(entries, produced_count, worker_class)
      produced   = produced_count.to_i
      unproduced = entries.size - produced

      self.class.release_unproduced_message_locks!(entries, produced, worker_class)
      return if unproduced <= 0

      begin
        KafkaBatch.store.add_jobs(@id, -unproduced)
      rescue StandardError => rollback_err
        KafkaBatch.logger.error(
          "[KafkaBatch][Batch] push_many_at rollback failed for batch_id=#{@id}: " \
          "#{rollback_err.message}. Leaving total_jobs unchanged — scheduled jobs " \
          "already indexed will still complete."
        )
      end
    end

    private :rollback_unproduced_jobs!, :rollback_unproduced_batch_messages!

    def parse_add_jobs_result(result)
      case result
      when Hash
        [result[:status], result[:seq_start], result[:seq_end]]
      else
        [result, nil, nil]
      end
    end

    private :parse_add_jobs_result, :next_batch_seq!

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

    def produce_job(worker_class, payload, job_id, tenant_id = nil, batch_seq: nil, valid_till: nil)
      message = self.class.build_message(
        worker_class: worker_class, payload: payload,
        job_id: job_id, batch_id: @id, attempt: 0, tenant_id: tenant_id,
        batch_seq: batch_seq, valid_till: valid_till
      )
      route = self.class.route_for(worker_class, job_id: job_id, tenant_id: tenant_id, batch_id: @id)
      KafkaBatch::Producer.produce_sync(
        topic: route[:topic], payload: message, key: route[:key], partition: route[:partition]
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
