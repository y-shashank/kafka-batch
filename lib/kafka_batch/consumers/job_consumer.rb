require "karafka"
require "oj"
require "time"

module KafkaBatch
  module Consumers
    # Karafka consumer that processes individual batch jobs.
    #
    # Retry strategy – dedicated retry topic (non-blocking):
    #   On failure, the message is forwarded to KafkaBatch.config.retry_topic
    #   with a `retry_after` timestamp and `retry_to` (original topic) embedded.
    #   A separate RetryConsumer handles the wait and re-enqueue, so this
    #   consumer's partition is never blocked during backoff.
    #
    # Rescue scope separation:
    #   worker#perform errors and event-emission errors are caught in separate
    #   rescue blocks.  A Kafka produce failure after a successful job does NOT
    #   trigger a job retry – instead the offset is left uncommitted so Karafka
    #   redelivers the message, allowing the worker to run again (workers must
    #   be idempotent) and try event emission once more.
    class JobConsumer < Karafka::BaseConsumer
      # Event-emission retry behaviour is configured via
      # KafkaBatch.config.event_emit_retries / .event_emit_backoff.

      # Cache of worker class name → Class to avoid repeated const_get lookups.
      WORKER_CACHE       = {}
      WORKER_CACHE_MUTEX = Mutex.new

      def consume
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
            publish_to_dlt(data: data, error: e, topic: message.topic)
            mark_as_consumed!(message)
            return
          end

        attempt       = data["attempt"].to_i
        max_retries   = data.fetch("max_retries",   KafkaBatch.config.max_retries).to_i
        backoff       = data.fetch("retry_backoff",  KafkaBatch.config.retry_backoff).to_i

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
          mark_as_consumed!(message)
          return
        end

        started_at = Time.now

        # ── Step 1: execute the job ──────────────────────────────────────
        # Only job-raised errors are caught here.  A successful perform
        # that subsequently fails at event emission is handled separately.
        begin
          worker = worker_class.new
          worker.kafka_batch_id = batch_id if worker.respond_to?(:kafka_batch_id=)
          worker.perform(payload)
        rescue StandardError => e
          handle_failure(
            message:      message,
            data:         data,
            error:        e,
            job_id:       job_id,
            batch_id:     batch_id,
            worker_class: worker_class,
            attempt:      attempt,
            max_retries:  max_retries,
            backoff:      backoff
          )
          return  # offset committed inside handle_failure
        end

        # ── Step 2: emit completion event ────────────────────────────────
        # Separate rescue: a Kafka error here must NOT be treated as a job
        # failure.  We retry emission a few times; if it keeps failing we
        # raise so the offset is NOT committed → Karafka redelivers the
        # message → worker runs again (idempotency required) → tries again.
        emit_event_with_retry(
          batch_id:     batch_id,
          job_id:       job_id,
          status:       "success",
          worker_class: worker_class,
          message:      message
        )

        duration = Time.now - started_at
        KafkaBatch::Instrumentation.job_processed(
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class,
          duration:     duration
        )

        mark_as_consumed!(message)
      end

      # ── Failure handling ─────────────────────────────────────────────────

      def handle_failure(message:, data:, error:, job_id:, batch_id:,
                         worker_class:, attempt:, max_retries:, backoff:)
        KafkaBatch.logger.error(
          "[KafkaBatch][JobConsumer] job_id=#{job_id} attempt=#{attempt} " \
          "#{error.class}: #{error.message}"
        )

        if attempt < max_retries
          schedule_retry(
            message:      message,
            data:         data,
            job_id:       job_id,
            next_attempt: attempt + 1,
            backoff:      backoff,
            worker_class: worker_class,
            batch_id:     batch_id
          )
        else
          exhaust_job(
            message:      message,
            data:         data,
            job_id:       job_id,
            batch_id:     batch_id,
            worker_class: worker_class,
            error:        error,
            attempt:      attempt
          )
        end
      end

      # Forward the message to the retry topic with a `retry_after` timestamp.
      # The RetryConsumer uses Karafka pause to wait until retry_after, then
      # re-enqueues back to the original topic – zero blocking here.
      def schedule_retry(message:, data:, job_id:, next_attempt:, backoff:,
                         worker_class: nil, batch_id: nil)
        retry_after = Time.now + (backoff * next_attempt)

        KafkaBatch.logger.info(
          "[KafkaBatch][JobConsumer] Scheduling retry for job_id=#{job_id} " \
          "attempt=#{next_attempt} at #{retry_after.iso8601}"
        )

        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.retry_topic,
          payload: data.merge(
            "attempt"      => next_attempt,
            "retry_after"  => retry_after.iso8601,
            "retry_to"     => message.topic
          ),
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
      def exhaust_job(message:, data:, job_id:, batch_id:, worker_class:, error:, attempt: nil)
        KafkaBatch.logger.error(
          "[KafkaBatch][JobConsumer] job_id=#{job_id} exhausted retries – failing"
        )

        emit_event_with_retry(
          batch_id:     batch_id,
          job_id:       job_id,
          status:       "failed",
          worker_class: worker_class,
          message:      message
        )

        KafkaBatch::Instrumentation.job_failed(
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class,
          attempt:      attempt || data["attempt"].to_i,
          error:        error
        )

        publish_to_dlt(data: data, error: error, topic: message.topic)
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

      # Build the completion-event payload and its partition key.
      #
      # The event carries the job message's immutable source coordinates
      # (topic/partition/offset) that the EventConsumer dedups on, and is keyed
      # by the source partition so a batch's events spread across the event-topic
      # partitions instead of funnelling through one. job_id is kept for logging.
      def build_event(batch_id:, job_id:, status:, worker_class:, message:)
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

        [payload, "#{message.topic}/#{message.partition}"]
      end

      def publish_to_dlt(data:, error:, topic:)
        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.dead_letter_topic,
          payload: data.merge(
            "dlt_type"          => "job",
            "dlt_source_topic"  => topic,
            "dlt_error_class"   => error.class.name,
            "dlt_error_message" => error.message,
            "dlt_at"            => Time.now.iso8601
          ),
          key: data["job_id"]
        )
      rescue KafkaBatch::ProducerError => e
        KafkaBatch.logger.error("[KafkaBatch][JobConsumer] DLT publish failed: #{e.message}")
        raise  # re-raise so offset is NOT committed → redelivery
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
