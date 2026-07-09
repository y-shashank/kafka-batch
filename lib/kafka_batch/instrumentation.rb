module KafkaBatch
  # ActiveSupport::Notifications integration.
  #
  # When ActiveSupport is available, each event is published via
  # ActiveSupport::Notifications.instrument so that users can subscribe with
  # the standard Rails/AS subscriber API:
  #
  #   ActiveSupport::Notifications.subscribe("job.processed.kafka_batch") do |*args|
  #     event = ActiveSupport::Notifications::Event.new(*args)
  #     StatsD.increment("kafka_batch.job.processed")
  #     StatsD.timing("kafka_batch.job.processed", event.duration)
  #   end
  #
  # When ActiveSupport is NOT available the methods are no-ops so the gem works
  # in non-Rails environments without requiring the dependency.
  #
  # Event name format: "<event>.<component>.kafka_batch"
  #
  # Events emitted:
  #   job.processed.kafka_batch           – job completed successfully
  #   job.retried.kafka_batch             – job scheduled for retry
  #   job.failed.kafka_batch              – job exhausted all retries
  #   job.cancelled.kafka_batch           – job skipped (batch was cancelled)
  #   job.uniq_skipped.kafka_batch        – duplicate rejected (worker has uniq true)
  #   job.expired.kafka_batch             – job dropped (valid_till passed)
  #   job.emit_retried.kafka_batch        – completion-event produce retried inline
  #   scheduled.enqueued.kafka_batch      – delayed job indexed (perform_in/at)
  #   scheduled.enqueued_bulk.kafka_batch – bulk delayed jobs indexed
  #   scheduled.dispatched.kafka_batch    – SchedulePoller re-produced a due job
  #   scheduled.index_failed.kafka_batch    – schedule index write failed after Kafka produce
  #   web.action.kafka_batch                – mutating Web UI action (when audit mirrors AS)
  #   batch.created.kafka_batch             – a new batch was persisted
  #   batch.sealed.kafka_batch              – block-form population finished
  #   batch.completed.kafka_batch           – batch reached terminal state
  #   callback.invoked.kafka_batch          – on_success / on_complete callback ran
  #   callback.failed.kafka_batch           – callback raised or was unresolvable
  #   dlt.published.kafka_batch             – message forwarded to dead-letter topic
  #   consumer.priority_yielded.kafka_batch   – priority consumer paused for higher lag
  #   reconciler.ran.kafka_batch              – reconciler sweep completed
  #
  module Instrumentation
    NAMESPACE = "kafka_batch"

    class << self
      # ── Job events ─────────────────────────────────────────────────────

      def job_processed(job_id:, batch_id:, worker_class:, duration: nil)
        instrument("job.processed", {
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class.to_s,
          duration:     duration
        })
      end

      def job_retried(job_id:, batch_id:, worker_class:, attempt:, next_attempt:, retry_after: nil)
        instrument("job.retried", {
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class.to_s,
          attempt:      attempt,
          next_attempt: next_attempt,
          retry_after:  retry_after
        })
      end

      def job_failed(job_id:, batch_id:, worker_class:, attempt:, error:)
        instrument("job.failed", {
          job_id:        job_id,
          batch_id:      batch_id,
          worker_class:  worker_class.to_s,
          attempt:       attempt,
          error_class:   error.class.name,
          error_message: error.message
        })
      end

      # Fired when a job is skipped because its batch was cancelled.
      # Useful for tracking how many jobs are being silently drained versus
      # actually executed (e.g. after an emergency cancel_batch! call).
      def job_cancelled(job_id:, batch_id:, worker_class:)
        instrument("job.cancelled", {
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class.to_s
        })
      end

      def job_uniq_skipped(worker_class:, payload:, job_id: nil, batch_id: nil)
        instrument("job.uniq_skipped", {
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class.to_s,
          payload:      payload
        })
      end

      def job_expired(job_id:, batch_id:, worker_class:, valid_till:)
        instrument("job.expired", {
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class.to_s,
          valid_till:   valid_till
        })
      end

      # Fired when the JobConsumer retries the completion-event produce inline.
      # Subscribe to this event to detect Kafka producer instability that is
      # causing event-emission retries (which block the consumer thread).
      def job_emit_retried(job_id:, batch_id:, attempt:, error:)
        instrument("job.emit_retried", {
          job_id:        job_id,
          batch_id:      batch_id,
          attempt:       attempt,
          error_class:   error.class.name,
          error_message: error.message
        })
      end

      # ── Scheduled (perform_in / perform_at) events ─────────────────────

      # Fired when a delayed job is persisted to the schedule index.
      def scheduled_enqueued(job_id:, batch_id:, worker_class:, run_at:)
        instrument("scheduled.enqueued", {
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class.to_s,
          run_at:       (run_at.respond_to?(:iso8601) ? run_at.iso8601 : run_at)
        })
      end

      # Fired when many delayed jobs are scheduled in one bulk call.
      def scheduled_enqueued_bulk(count:, batch_id:, worker_class:, run_at:)
        instrument("scheduled.enqueued_bulk", {
          count:        count,
          batch_id:     batch_id,
          worker_class: worker_class.to_s,
          run_at:       (run_at.respond_to?(:iso8601) ? run_at.iso8601 : run_at)
        })
      end

      # Fired when the SchedulePoller re-produces a due job onto its real topic.
      def scheduled_dispatched(job_id:, batch_id:, worker_class:, topic:)
        instrument("scheduled.dispatched", {
          job_id:       job_id,
          batch_id:     batch_id,
          worker_class: worker_class.to_s,
          topic:        topic
        })
      end

      # Fired when Kafka produce succeeded but persisting the schedule index failed
      # after all retries (transient store outage).
      def scheduled_index_failed(count:, batch_id: nil, job_id: nil, attempts:, error:)
        instrument("scheduled.index_failed", {
          count:         count,
          batch_id:      batch_id,
          job_id:        job_id,
          attempts:      attempts,
          error_class:   error.class.name,
          error_message: error.message
        })
      end

      # ── Batch events ───────────────────────────────────────────────────

      # Fired immediately after a new batch is persisted in the store. Useful
      # for tracking batch creation rates and correlating batch_id across systems.
      def batch_created(batch_id:, description: nil, tenant_id: nil, on_success: nil, on_complete: nil)
        instrument("batch.created", {
          batch_id:    batch_id,
          description: description,
          tenant_id:   tenant_id,
          on_success:  on_success,
          on_complete: on_complete
        })
      end

      # Fired when a block-form batch is sealed (population finished). For bare
      # (non-block) batches the batch is sealed at create time; no separate event
      # is emitted so as to avoid duplicate signals.
      def batch_sealed(batch_id:, total_jobs:)
        instrument("batch.sealed", {
          batch_id:   batch_id,
          total_jobs: total_jobs
        })
      end

      def batch_completed(batch_id:, outcome:, total_jobs:, completed_count:, failed_count:)
        instrument("batch.completed", {
          batch_id:        batch_id,
          outcome:         outcome,
          total_jobs:      total_jobs,
          completed_count: completed_count,
          failed_count:    failed_count
        })
      end

      # ── Callback events ────────────────────────────────────────────────

      def callback_invoked(batch_id:, callback_class:, callback_method:)
        instrument("callback.invoked", {
          batch_id:        batch_id,
          callback_class:  callback_class.to_s,
          callback_method: callback_method.to_s
        })
      end

      def callback_failed(batch_id:, callback_class:, callback_method:, error:)
        instrument("callback.failed", {
          batch_id:        batch_id,
          callback_class:  callback_class.to_s,
          callback_method: callback_method.to_s,
          error_class:     error.class.name,
          error_message:   error.message
        })
      end

      # ── DLT events ─────────────────────────────────────────────────────

      # Fired whenever a message is published to the dead-letter topic.
      # All DLT paths go through KafkaBatch::Dlt.publish so this always fires
      # on successful produce. dlt_type labels the publish path, e.g.:
      #   job              – JobConsumer (malformed, unknown worker, exhausted)
      #   expired          – valid_till passed
      #   retry_routing    – RetryConsumer could not route the retry
      #   callback         – unresolvable callback class/method
      #   callback_error   – callback raised during invocation
      #   malformed_callback / malformed_event – unparseable JSON
      def dlt_published(batch_id: nil, job_id: nil, dlt_type:, source_topic:)
        instrument("dlt.published", {
          batch_id:     batch_id,
          job_id:       job_id,
          dlt_type:     dlt_type,
          source_topic: source_topic
        })
      end

      # ── Consumer events ────────────────────────────────────────────────

      # Fired when a lower-ranked priority consumer pauses because higher-ranked
      # topics in the same group have lag.
      #
      # Payload keys:
      #   consumer_class  – e.g. "KafkaBatch::Consumers::PriorityJobConsumer"
      #   p0_topic        – first higher-ranked topic checked (legacy key name)
      #   consumer_group  – the consumer group owning those topics
      #   pause_ms        – how long the partition will be paused
      #   mode            – weighted | strict
      #   rank            – topic rank in the priority YAML (0 = highest)
      #   higher_topics   – all higher-ranked topics with lag at check time
      def consumer_priority_yielded(consumer_class:, p0_topic:, consumer_group:, pause_ms:,
                                    mode: nil, rank: nil, higher_topics: nil)
        instrument("consumer.priority_yielded", {
          consumer_class: consumer_class.to_s,
          p0_topic:       p0_topic,
          consumer_group: consumer_group,
          pause_ms:       pause_ms,
          mode:           mode,
          rank:           rank,
          higher_topics:  higher_topics
        }.compact)
      end

      # ── Reconciler events ──────────────────────────────────────────────

      # triggered_by: :consumer (auto-fired by EventConsumer) or :rake (manual run).
      def reconciler_ran(stale_count:, lost_count:, duration:, triggered_by: :rake)
        instrument("reconciler.ran", {
          stale_count:  stale_count,
          lost_count:   lost_count,
          duration:     duration,
          triggered_by: triggered_by
        })
      end

      # Fired when KafkaBatch::Web handles a mutating POST (mirrors audit log action).
      def web_action(action:, path:, status:, actor: nil, error: nil)
        instrument("web.action", {
          action: action.to_s,
          path:   path.to_s,
          status: status.to_s,
          actor:  actor,
          error:  error
        }.compact)
      end

      private

      def instrument(event, payload = {})
        name = "#{event}.#{NAMESPACE}"
        if defined?(ActiveSupport::Notifications)
          ActiveSupport::Notifications.instrument(name, payload)
        end
        # Always log at debug level so instrumentation is visible even without AS
        KafkaBatch.logger.debug("[KafkaBatch][Instrumentation] #{name} #{payload.inspect}")
      end
    end
  end
end
