module KafkaBatch
  # Periodic sweep that detects and recovers two categories of stuck batches:
  #
  #  1. Stuck-running: status="running" but all jobs are done
  #     Cause: EventConsumer never incremented the counter to completion
  #     (e.g. event messages were lost before the consumer started).
  #
  #  2. Lost-callback: status="success"|"complete" but callback was never
  #     dispatched (callback_dispatched_at IS NULL).
  #     Cause: EventConsumer crashed after updating the store record but before
  #     successfully producing the message to the callbacks topic.
  #
  # Run via Rake:
  #   bundle exec rake kafka_batch:reconcile
  #
  # Or schedule with cron / Whenever / a Karafka scheduled consumer.
  module Reconciler
    # @param older_than [Integer] seconds – only inspect batches older than this
    def self.run(older_than: KafkaBatch.config.reconciliation_interval)
      start_time = Time.now

      KafkaBatch.store.with_reconciler_lock(ttl: older_than) do
        threshold = Time.now - older_than

        # ── 1. Stuck-running batches ─────────────────────────────────────────
        stale = KafkaBatch.store.stale_batches(older_than: threshold)
        KafkaBatch.logger.info("[KafkaBatch][Reconciler] Found #{stale.size} stuck-running batch(es)")
        stale.each { |b| reconcile_running(b) }

        # ── 2. Done batches with lost callbacks ──────────────────────────────
        lost = KafkaBatch.store.done_batches_without_callback(older_than: threshold)
        KafkaBatch.logger.info("[KafkaBatch][Reconciler] Found #{lost.size} lost-callback batch(es)")
        lost.each { |b| refire_callback(b) }

        duration = Time.now - start_time
        KafkaBatch::Instrumentation.reconciler_ran(
          stale_count: stale.size,
          lost_count:  lost.size,
          duration:    duration
        )
        KafkaBatch.logger.info("[KafkaBatch][Reconciler] Done in #{duration.round(2)}s")
      end
    end

    # Re-evaluates a batch that's been stuck in "running" too long.
    # If counter arithmetic shows it's actually done, transitions it and fires
    # the callback.  Otherwise logs and moves on.
    def self.reconcile_running(batch)
      id    = batch[:id]
      total = batch[:total_jobs].to_i
      done  = batch[:completed_count].to_i + batch[:failed_count].to_i

      KafkaBatch.logger.info(
        "[KafkaBatch][Reconciler] stuck-running batch_id=#{id} " \
        "total=#{total} done=#{done}"
      )

      unless done >= total && total.positive?
        KafkaBatch.logger.warn(
          "[KafkaBatch][Reconciler] batch_id=#{id} genuinely still running – skipping"
        )
        return
      end

      outcome = batch[:failed_count].to_i.positive? ? "complete" : "success"
      KafkaBatch.store.update_batch_status(id, outcome)

      KafkaBatch.logger.warn(
        "[KafkaBatch][Reconciler] batch_id=#{id} transitioned to #{outcome} – producing callback"
      )

      produce_callback(batch.merge("outcome" => outcome))
    end

    # Re-produces the callback message for a done batch whose callback was never
    # dispatched.  The CallbackConsumer's atomic claim_callback guard ensures the
    # callback itself fires at most once even if this runs multiple times.
    def self.refire_callback(batch)
      KafkaBatch.logger.warn(
        "[KafkaBatch][Reconciler] lost-callback batch_id=#{batch[:id]} " \
        "status=#{batch[:status]} – re-producing callback message"
      )

      produce_callback(batch.merge(
        "outcome"    => batch[:status],
        "reconciled" => true
      ))
    end

    def self.produce_callback(batch)
      KafkaBatch::Producer.produce_sync(
        topic:   KafkaBatch.config.callbacks_topic,
        payload: {
          "batch_id"        => batch[:id],
          "outcome"         => batch["outcome"] || batch[:status],
          "total_jobs"      => batch[:total_jobs],
          "completed_count" => batch[:completed_count],
          "failed_count"    => batch[:failed_count],
          "on_success"      => batch[:on_success],
          "on_complete"     => batch[:on_complete],
          "meta"            => batch[:meta],
          "finished_at"     => batch[:finished_at],
          "reconciled"      => batch["reconciled"] || false
        },
        key: batch[:id]
      )
    rescue KafkaBatch::ProducerError => e
      KafkaBatch.logger.error(
        "[KafkaBatch][Reconciler] Failed to produce callback for " \
        "batch_id=#{batch[:id]}: #{e.message}"
      )
    end
  end
end
