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
  #   job.processed.kafka_batch        – job completed successfully
  #   job.retried.kafka_batch          – job scheduled for retry
  #   job.failed.kafka_batch           – job exhausted all retries
  #   job.cancelled.kafka_batch        – job skipped (batch was cancelled)
  #   job.emit_retried.kafka_batch     – completion-event produce retried inline
  #   batch.created.kafka_batch        – a new batch was persisted
  #   batch.sealed.kafka_batch         – block-form population finished; batch sealed
  #   batch.completed.kafka_batch      – batch reached terminal state
  #   callback.invoked.kafka_batch     – on_success / on_complete callback ran
  #   callback.failed.kafka_batch      – callback raised or was unresolvable
  #   dlt.published.kafka_batch        – a message was forwarded to the dead-letter topic
  #   consumer.priority_yielded.kafka_batch – p1 consumer paused for p0 lag
  #   reconciler.ran.kafka_batch       – reconciler sweep completed
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

      # Fired whenever any consumer publishes a message to the dead-letter topic.
      # dlt_type is a short label identifying the publish path, e.g.:
      #   "retry_routing"   – RetryConsumer could not route the retry
      #   "callback_failed" – CallbackConsumer failed to invoke the callback
      #   "job_failed"      – JobConsumer exhausted retries (forwarded by store)
      def dlt_published(batch_id: nil, job_id: nil, dlt_type:, source_topic:)
        instrument("dlt.published", {
          batch_id:     batch_id,
          job_id:       job_id,
          dlt_type:     dlt_type,
          source_topic: source_topic
        })
      end

      # ── Consumer events ────────────────────────────────────────────────

      # Fired by PriorityGate#yield_to_p0 when a p1 consumer pauses because the
      # corresponding p0 topic has lag.  Subscribe to this event to measure
      # priority queue contention and tune lag_check_interval or worker counts.
      #
      # Payload keys:
      #   consumer_class  – e.g. "KafkaBatch::Consumers::FastP1Consumer"
      #   p0_topic        – topic name the lag was detected on
      #   consumer_group  – the consumer group owning that topic
      #   pause_ms        – how long the partition will be paused
      def consumer_priority_yielded(consumer_class:, p0_topic:, consumer_group:, pause_ms:)
        instrument("consumer.priority_yielded", {
          consumer_class: consumer_class.to_s,
          p0_topic:       p0_topic,
          consumer_group: consumer_group,
          pause_ms:       pause_ms
        })
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
