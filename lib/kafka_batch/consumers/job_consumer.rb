require "karafka"
require "oj"
require "time"

module KafkaBatch
  module Consumers
    # Karafka consumer that processes individual batch jobs.
    #
    # Retry strategy – tiered retry topics (non-blocking):
    #   On failure, the message is forwarded to the retry topic for the chosen
    #   delay tier (config.retry_topic_for(tier)) with a `retry_after` timestamp
    #   and `retry_to` (original topic) embedded. The tier is the worker's
    #   `retry_tier` override, else config.retry_tier_progression by retry index.
    #   A separate RetryConsumer handles the wait and re-enqueue, so this
    #   consumer's partition is never blocked during the delay.
    #
    # Rescue scope separation:
    #   worker#perform errors and event-emission errors are caught in separate
    #   rescue blocks.  A Kafka produce failure after a successful job does NOT
    #   trigger a job retry – instead the offset is left uncommitted so Karafka
    #   redelivers the message, allowing the worker to run again (workers must
    #   be idempotent) and try event emission once more.
    class JobConsumer < Karafka::BaseConsumer
      prepend ConsumptionGate
      include ExpiredJobHandler
      # Event-emission retry behaviour is configured via
      # KafkaBatch.config.event_emit_retries / .event_emit_backoff.

      # Cache of worker class name → Class to avoid repeated const_get lookups.
      WORKER_CACHE       = {}
      WORKER_CACHE_MUTEX = Mutex.new

      def consume
        process_messages
      end

      # Overridable batch-processing hook. Kept separate from #consume so the
      # prepended ConsumptionGate (heartbeat + /lag pause) always wraps the work:
      # subclasses (e.g. PriorityJobConsumer) override THIS, not #consume, so they
      # can never bypass the gate by forgetting to call `super`.
      def process_messages
        messages.each { |msg| process_message(msg) }
      end

      private

      def process_message(message)
        data = begin
          decode(message.raw_payload)
        rescue ArgumentError => e
          KafkaBatch.logger.error(
            "[KafkaBatch][JobConsumer] Malformed JSON payload – forwarding to DLT: #{e.message}"
          )
          publish_to_dlt(
            data:  { "raw_payload" => message.raw_payload.to_s },
            error: e,
            topic: message.topic
          )
          mark_as_consumed!(message)
          return
        end

        job_id        = data["job_id"]
        batch_id      = data["batch_id"]
        payload       = data["payload"] || {}

        # Fair-lane bookkeeping: parse before the expiry gate so an expired ready
        # message still releases its Scheduler in-flight slot.
        fair_slot     = data["_fair_slot"] ? true : false
        fair_tenant   = data["tenant_id"]
        fair_type     = (data["_fair_type"] || "time").to_sym  # which lane's slot to release
        fair_slot_id  = data["_fair_slot_id"]                  # lease id (nil for pre-upgrade messages)
        fair_started  = nil  # set right before perform so duration reflects run time

        if expired_job?(data)
          begin
            handle_expired_job(message: message, data: data)
          ensure
            if fair_slot
              release_fair_slot(fair_tenant, 0.0, fair_type, fair_slot_id)
            end
          end
          return
        end

        # A message forwarded by Fairness::Forwarder carries "_fair_slot" => true
        # and a tenant_id. It holds exactly one Scheduler in-flight slot that MUST
        # be released via Scheduler#complete exactly once when we finish with the
        # message — whether it succeeds, is scheduled for retry, is DLT'd, expired,
        # or is skipped (cancelled). In :time_fairness mode complete also advances
        # the tenant's virtual time by (duration / weight). Retried messages have
        # the marker stripped (see schedule_retry), so they never double-release.
        if fair_slot && fair_slot_id && !claim_fair_slot_execution!(fair_type, fair_slot_id)
          KafkaBatch.logger.info(
            "[KafkaBatch][JobConsumer] duplicate fair-slot delivery – skipping job_id=#{job_id}"
          )
          mark_as_consumed!(message)
          return
        end

        # Everything below runs inside begin/ensure so the fair-lane in-flight
        # slot is released exactly once on every exit path (success, retry, DLT,
        # cancel, unknown worker).
        begin
        # Resolve the worker class. An unknown/renamed class is a poison pill:
        # without handling it here the ArgumentError would bubble up, the offset
        # would never commit, and Karafka would redeliver forever (blocking the
        # partition). Route it to the DLT and advance the batch so it can finish.
        worker_class =
          begin
            resolve_worker(data["worker_class"])
          rescue ArgumentError => e
            KafkaBatch.logger.error(
              "[KafkaBatch][JobConsumer] #{e.message} – forwarding to DLT"
            )
            emit_event_with_retry(
              batch_id:     batch_id,
              job_id:       job_id,
              status:       "failed",
              worker_class: data["worker_class"].to_s,
              message:      message
            )
            record_failure(batch_id, job_id, data["worker_class"], e)
            publish_to_dlt(data: data, error: e, topic: message.topic)
            release_uniq_lock(data)
            mark_as_consumed!(message)
            return
          end

        attempt        = data["attempt"].to_i
        max_retries    = data.fetch("max_retries", KafkaBatch.config.max_retries).to_i
        complete_after = data.fetch("complete_after_retries", KafkaBatch.config.complete_after_retries).to_i
        # Whether this job has already been counted toward its batch early (rides
        # the retry message so a job is counted at most once).
        batch_counted  = data["batch_counted"] ? true : false

        KafkaBatch.logger.debug(
          "[KafkaBatch][JobConsumer] #{worker_class}#perform " \
          "job_id=#{job_id} batch_id=#{batch_id} attempt=#{attempt}"
        )

        # ── Cancellation gate ────────────────────────────────────────────
        # If the batch was cancelled, skip the job entirely – do not run the
        # worker and do not emit a completion event. Remaining in-flight jobs
        # simply drain without side effects.
        if batch_id && KafkaBatch.config.skip_cancelled_jobs && batch_cancelled?(batch_id)
          KafkaBatch.logger.info(
            "[KafkaBatch][JobConsumer] batch_id=#{batch_id} cancelled – skipping job_id=#{job_id}"
          )
          KafkaBatch::Instrumentation.job_cancelled(
            job_id:       job_id,
            batch_id:     batch_id,
            worker_class: worker_class
          )
          release_uniq_lock(data)
          mark_as_consumed!(message)
          return
        end

        # Record this job as running (best-effort, Redis-backed, TTL'd) so the
        # dashboard can show in-flight work. Cleared in the ensure below.
        KafkaBatch::Liveness.job_started(
          job_id: job_id, batch_id: batch_id, worker_class: worker_class,
          topic: message.topic, partition: message.partition
        )

        started_at = Time.now
        fair_started = started_at
        fair_renewer = start_fair_lease_renewal(fair_tenant, fair_type, fair_slot_id) if fair_slot && fair_slot_id

        begin
          # ── Step 1: execute the job ────────────────────────────────────
          # Only job-raised errors are caught here.  A successful perform
          # that subsequently fails at event emission is handled separately.
          begin
            worker = worker_class.new
            worker.bind_job_context!(data, worker_class: worker_class)
            worker.perform(payload)
          rescue StandardError => e
            handle_failure(
              message:                message,
              data:                   data,
              error:                  e,
              job_id:                 job_id,
              batch_id:               batch_id,
              worker_class:           worker_class,
              attempt:                attempt,
              max_retries:            max_retries,
              complete_after_retries: complete_after,
              batch_counted:          batch_counted
            )
            return  # offset committed inside handle_failure
          end

          # Succeeded (possibly on a retry): drop any prior "retrying" failure
          # record so it no longer shows as retrying in the dashboard.
          clear_failure(batch_id, job_id) if batch_id && attempt.positive?

          # ── Step 2: emit completion event ──────────────────────────────
          # Separate rescue: a Kafka error here must NOT be treated as a job
          # failure.  We retry emission a few times; if it keeps failing we
          # raise so the offset is NOT committed → Karafka redelivers the
          # message → worker runs again (idempotency required) → tries again.
          #
          # If the job was already counted toward the batch early (after
          # complete_after_retries), skip the success event so it isn't counted
          # twice — the batch already advanced. The work still ran.
          unless batch_counted
            emit_event_with_retry(
              batch_id:     batch_id,
              job_id:       job_id,
              status:       "success",
              worker_class: worker_class,
              message:      message
            )
          end

          duration = Time.now - started_at
          KafkaBatch::Instrumentation.job_processed(
            job_id:       job_id,
            batch_id:     batch_id,
            worker_class: worker_class,
            duration:     duration
          )

          release_uniq_lock(data)
          mark_as_consumed!(message)
        ensure
          stop_fair_lease_renewal(fair_renewer)
          KafkaBatch::Liveness.job_finished(job_id)
        end
        ensure
          # Release the WFQ in-flight slot for fair-lane messages. In
          # :time_fairness mode this also advances the tenant's vtime by
          # (duration / weight); in :job_count_fairness vtime already advanced at
          # checkout and duration is ignored.
          if fair_slot
            dur = fair_started ? (Time.now - fair_started) : 0.0
            release_fair_slot(fair_tenant, dur, fair_type, fair_slot_id)
          end
        end
      end

      # Release one Scheduler in-flight slot held by a fair-lane ready message.
      # Best-effort: a Redis hiccup here must never break job processing (the
      # slot self-heals — a stuck slot is only a soft concurrency undercount and
      # can be reset via Scheduler#reset!).
      def release_fair_slot(tenant_id, duration, type = :time, slot_id = nil)
        return unless tenant_id && !tenant_id.to_s.empty?
        sched = KafkaBatch.scheduler(type)
        return unless sched
        sched.complete(tenant_id, slot_id: slot_id, duration: duration)
      rescue StandardError => e
        KafkaBatch.logger.warn(
          "[KafkaBatch][JobConsumer] fair slot release failed for tenant=#{tenant_id} lane=#{type}: #{e.message}"
        )
      end

      # Background thread extends the fairness lease while perform runs so jobs
      # longer than fairness_lease_ttl do not admit extra work past the budget.
      # @return [Array(Thread, Array<Boolean>)] thread + stop flag container
      def start_fair_lease_renewal(tenant_id, type, slot_id)
        sched = KafkaBatch.scheduler(type)
        return nil unless sched && slot_id

        stop     = [false]
        interval = [sched.lease_ttl / 3.0, 10.0].max
        thread   = Thread.new do
          loop do
            sleep(interval)
            break if stop[0]
            sched.renew_lease(tenant_id, slot_id: slot_id)
          end
        end
        [thread, stop]
      end

      def stop_fair_lease_renewal(pair)
        return unless pair

        thread, stop = pair
        stop[0] = true
        thread.join(0.5) rescue nil
      end

      # Dedup ready-topic redelivery of the same _fair_slot_id (reclaim path).
      def claim_fair_slot_execution!(type, slot_id)
        sched = KafkaBatch.scheduler(type)
        return true unless sched

        sched.claim_slot_execution!(slot_id)
      end

      # ── Failure handling ─────────────────────────────────────────────────

      def handle_failure(message:, data:, error:, job_id:, batch_id:,
                         worker_class:, attempt:, max_retries:,
                         complete_after_retries:, batch_counted:)
        KafkaBatch.logger.error(
          "[KafkaBatch][JobConsumer] job_id=#{job_id} attempt=#{attempt} " \
          "#{error.class}: #{error.message}"
        )

        if attempt < max_retries
          next_attempt = attempt + 1
          # Pick the delay tier (worker override wins, else walk the progression)
          # and route the retry to that tier's own topic so a slow tier never
          # head-of-line-blocks a fast one.
          tier         = KafkaBatch.config.retry_tier_for(next_attempt, data["retry_tier"])
          delay        = KafkaBatch.config.retry_delay_for(tier)
          retry_after  = Time.now + delay

          # Record the failure on EVERY attempt (not just exhaustion) so problems
          # surface immediately, with when the next retry is due.
          record_failure(batch_id, job_id, worker_class, error,
                         attempt: attempt, status: "retrying", next_retry_at: retry_after)

          # Early batch completion: once a still-failing job has retried
          # complete_after_retries times, count it toward the batch (as failed)
          # so on_complete needn't wait for the full retry budget. It keeps
          # retrying; the batch_counted flag rides the retry message so it is
          # counted exactly once.
          if batch_id && !batch_counted && attempt >= complete_after_retries
            emit_event_with_retry(
              batch_id:     batch_id,
              job_id:       job_id,
              status:       "failed",
              worker_class: worker_class,
              message:      message
            )
            batch_counted = true
            KafkaBatch.logger.info(
              "[KafkaBatch][JobConsumer] job_id=#{job_id} counted toward batch after " \
              "#{attempt} retries (still retrying up to #{max_retries})"
            )
          end

          schedule_retry(
            message:       message,
            data:          data,
            job_id:        job_id,
            next_attempt:  next_attempt,
            retry_after:   retry_after,
            tier:          tier,
            worker_class:  worker_class,
            batch_id:      batch_id,
            batch_counted: batch_counted
          )
        else
          record_failure(batch_id, job_id, worker_class, error,
                         attempt: attempt, status: "failed", next_retry_at: nil)
          exhaust_job(
            message:       message,
            data:          data,
            job_id:        job_id,
            batch_id:      batch_id,
            worker_class:  worker_class,
            error:         error,
            attempt:       attempt,
            batch_counted: batch_counted
          )
        end
      end

      # Forward the message to the retry topic with a `retry_after` timestamp.
      # The RetryConsumer uses Karafka pause to wait until retry_after, then
      # re-enqueues back to the original topic – zero blocking here.
      def schedule_retry(message:, data:, job_id:, next_attempt:, retry_after:,
                         tier:, worker_class: nil, batch_id: nil, batch_counted: false)
        KafkaBatch.logger.info(
          "[KafkaBatch][JobConsumer] Scheduling retry for job_id=#{job_id} " \
          "attempt=#{next_attempt} tier=#{tier} at #{retry_after.iso8601}"
        )

        # Strip "_fair_slot": the current pass releases its Scheduler in-flight
        # slot in the outer ensure. The retried message re-enters the ready topic
        # WITHOUT holding a slot, so it must not trigger a second (spurious)
        # Scheduler#complete when it runs.
        retry_payload = data.merge(
          "attempt"       => next_attempt,
          "retry_after"   => retry_after.iso8601,
          "retry_to"      => message.topic,
          "batch_counted" => batch_counted
        )
        retry_payload.delete("_fair_slot")

        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.retry_topic_for(tier),
          payload: retry_payload,
          key: job_id
        )

        KafkaBatch::Instrumentation.job_retried(
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class || data["worker_class"],
          attempt:      next_attempt - 1,
          next_attempt: next_attempt,
          retry_after:  retry_after
        )

        mark_as_consumed!(message)
      end

      # Job has exhausted all retries.  Emit a failure event (so the batch
      # counter is updated) and forward the raw message to the DLT.
      def exhaust_job(message:, data:, job_id:, batch_id:, worker_class:, error:, attempt: nil, batch_counted: false)
        KafkaBatch.logger.error(
          "[KafkaBatch][JobConsumer] job_id=#{job_id} exhausted retries – failing"
        )

        # Skip the batch event if the job was already counted early
        # (complete_after_retries < max_retries) — it's already in the tally.
        unless batch_counted
          emit_event_with_retry(
            batch_id:     batch_id,
            job_id:       job_id,
            status:       "failed",
            worker_class: worker_class,
            message:      message
          )
        end

        KafkaBatch::Instrumentation.job_failed(
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class,
          attempt:      attempt || data["attempt"].to_i,
          error:        error
        )

        publish_to_dlt(data: data, error: error, topic: message.topic)
        release_uniq_lock(data)
        mark_as_consumed!(message)
      end

      # ── Event emission ───────────────────────────────────────────────────

      def emit_event_with_retry(batch_id:, job_id:, status:, worker_class:, message:)
        return unless batch_id  # standalone job – no batch tracking

        max_attempts        = KafkaBatch.config.event_emit_retries.to_i
        backoff             = KafkaBatch.config.event_emit_backoff.to_i
        payload, event_key  = build_event(
          batch_id: batch_id, job_id: job_id, status: status,
          worker_class: worker_class, message: message
        )

        attempts = 0
        begin
          KafkaBatch::Producer.produce_sync(
            topic:   KafkaBatch.config.events_topic,
            payload: payload,
            key:     event_key
          )
        rescue KafkaBatch::ProducerError => e
          attempts += 1
          if attempts <= max_attempts
            KafkaBatch::Instrumentation.job_emit_retried(
              job_id:   job_id,
              batch_id: batch_id,
              attempt:  attempts,
              error:    e
            )
            KafkaBatch.logger.warn(
              "[KafkaBatch][JobConsumer] Event emit failed (attempt #{attempts}) – retrying: #{e.message}"
            )
            sleep(attempts * backoff) if backoff.positive?
            retry
          end
          # All retries exhausted: re-raise so offset is NOT committed.
          # Karafka will redeliver the original job message.
          KafkaBatch.logger.error(
            "[KafkaBatch][JobConsumer] Event emit failed after #{max_attempts} attempts – " \
            "leaving offset uncommitted for redelivery (worker must be idempotent)"
          )
          raise
        end
      end

      # ── Helpers ──────────────────────────────────────────────────────────

      def release_uniq_lock(data)
        KafkaBatch::Uniqueness.release_by_name(
          data["worker_class"],
          data["payload"] || {},
          job_id: data["job_id"],
          fp:     data["_uniq_fp"]
        )
      rescue StandardError => e
        KafkaBatch.logger.warn(
          "[KafkaBatch][JobConsumer] uniq lock release failed job_id=#{data['job_id']}: #{e.message}"
        )
      end

      # Build the completion-event payload and its partition key.
      #
      # The event carries the job message's immutable source coordinates
      # (topic/partition/offset) that the EventConsumer dedups on, and is keyed
      # by the source partition so a batch's events spread across the event-topic
      # partitions instead of funnelling through one. job_id is kept for logging.
      def build_event(batch_id:, job_id:, status:, worker_class:, message:)
        data = decode(message.raw_payload)
        payload = {
          "batch_id"      => batch_id,
          "job_id"        => job_id,
          "status"        => status,
          "worker_class"  => worker_class.to_s,
          "occurred_at"   => Time.now.iso8601,
          "src_topic"     => message.topic,
          "src_partition" => message.partition,
          "src_offset"    => message.offset
        }
        payload["batch_seq"] = data["batch_seq"] if data["batch_seq"]

        [payload, "#{message.topic}/#{message.partition}"]
      end

      def publish_to_dlt(data:, error:, topic:)
        KafkaBatch::Dlt.publish(
          payload: data.merge(
            "dlt_type"          => "job",
            "dlt_source_topic"  => topic,
            "dlt_error_class"   => error.class.name,
            "dlt_error_message" => error.message,
            "dlt_at"            => Time.now.iso8601
          ),
          key:          data["job_id"],
          dlt_type:     "job",
          source_topic: topic,
          batch_id:     data["batch_id"],
          job_id:       data["job_id"]
        )
      rescue KafkaBatch::ProducerError => e
        KafkaBatch.logger.error("[KafkaBatch][JobConsumer] DLT publish failed: #{e.message}")
        raise  # re-raise so offset is NOT committed → redelivery
      end

      # Always-on failure tracking: record the failure for the batch so it can be
      # surfaced in the dashboard immediately (status "retrying") and on final
      # exhaustion (status "failed"). Best-effort – never breaks the consumer.
      def record_failure(batch_id, job_id, worker_class, error, attempt: 0, status: "failed", next_retry_at: nil)
        return unless batch_id

        KafkaBatch.store.record_failure(
          batch_id:      batch_id,
          job_id:        job_id,
          worker_class:  worker_class.to_s,
          error_class:   error.class.name,
          error_message: error.message,
          attempt:       attempt,
          status:        status,
          next_retry_at: next_retry_at
        )
      rescue StandardError => e
        KafkaBatch.logger.warn(
          "[KafkaBatch][JobConsumer] failed to record failure for job_id=#{job_id}: #{e.message}"
        )
      end

      # Drop a prior failure record after a successful (re)run. Best-effort.
      def clear_failure(batch_id, job_id)
        KafkaBatch.store.clear_failure(batch_id, job_id)
      rescue StandardError => e
        KafkaBatch.logger.warn(
          "[KafkaBatch][JobConsumer] failed to clear failure for job_id=#{job_id}: #{e.message}"
        )
      end

      def batch_cancelled?(batch_id)
        # Uses a process-local cache refreshed at most once per
        # cancellation_cache_ttl seconds – no per-job store read.
        KafkaBatch::CancellationCache.cancelled?(batch_id)
      end

      def resolve_worker(class_name)
        # Fast path: already cached
        cached = WORKER_CACHE_MUTEX.synchronize { WORKER_CACHE[class_name] }
        return cached if cached

        WORKER_CACHE_MUTEX.synchronize do
          # Double-check after acquiring the lock
          return WORKER_CACHE[class_name] if WORKER_CACHE[class_name]

          klass = Object.const_get(class_name)
          raise ArgumentError, "#{class_name} does not include KafkaBatch::Worker" \
            unless klass.include?(KafkaBatch::Worker)

          WORKER_CACHE[class_name] = klass
        end
      rescue NameError
        raise ArgumentError, "Unknown worker class: #{class_name}"
      end

      def decode(raw)
        Oj.load(raw)
      rescue Oj::ParseError => e
        raise ArgumentError, "Invalid JSON payload: #{e.message}"
      end
    end
  end
end
