require "karafka"
require "oj"
require "securerandom"
require "set"
require "time"

module KafkaBatch
  module Consumers
    # Karafka consumer that processes job completion events.
    #
    # Reads from KafkaBatch.config.events_topic.
    # For each event it atomically increments the batch counter in the store.
    # When a batch reaches 100% completion it produces a single callback message.
    #
    # Message payload schema:
    #   {
    #     "batch_id"    => "uuid",
    #     "job_id"      => "uuid",
    #     "status"      => "success" | "failed",
    #     "occurred_at" => "ISO8601"
    #   }
    class EventConsumer < Karafka::BaseConsumer
      prepend ConsumptionGate

      # Class-level reconciler scheduling: at most one run per
      # reconciliation_interval seconds per process.  The distributed lock
      # inside Reconciler.run handles multi-process safety.
      RECONCILER_MUTEX   = Mutex.new
      @last_reconcile_at  = nil
      # Bug #14 fix: track whether a reconciler thread is already running so we
      # never spawn a second one if the first takes longer than the interval.
      @reconciler_running = false
      # #11 fix: store the thread reference so maybe_reconcile can detect a
      # thread that died before its ensure block ran (e.g. Thread.new EAGAIN)
      # and self-heal the @reconciler_running flag.
      @reconciler_thread  = nil

      class << self
        attr_accessor :last_reconcile_at, :reconciler_running, :reconciler_thread
      end

      # Apply a whole poll's completion events in ONE store call (per-poll
      # batching). This collapses N per-event transactions/round-trips into one,
      # which dramatically reduces hot-batch-row lock contention on the MySQL
      # store (and round-trips on Redis). Dedup/finalization stay exactly-once:
      # the store deduplicates each event by its source offset, so re-delivering
      # this whole batch (if we crash before committing the offset) never
      # double-counts and never drops a job — protecting callback correctness.
      def consume
        maybe_reconcile
        events = []
        last   = nil

        messages.each do |message|
          last = message
          ev = extract_event(message)  # nil when malformed/skipped (handled inline)
          events << ev if ev
        end

        apply(events)

        # Commit the whole poll only after the counters are durably applied. If
        # anything above raised, the offset is NOT committed and Karafka redelivers
        # the batch – the store dedups already-applied events on the replay.
        mark_as_consumed!(last) if last
      end

      private

      # Trigger the reconciler in a background thread if the configured interval
      # has elapsed.  Uses a class-level timestamp so all EventConsumer instances
      # in this process (one per events-topic partition) share one timer — the
      # effective run frequency matches config.reconciliation_interval regardless
      # of partition count.  The reconciler's distributed lock (MySQL GET_LOCK /
      # Redis SET NX EX) deduplicates across multiple app processes.
      # Bug #14 fix: guard with @reconciler_running so we never spawn a second
      # thread if the previous run hasn't finished yet.
      def maybe_reconcile
        interval = KafkaBatch.config.reconciliation_interval.to_f
        now      = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        should_run = RECONCILER_MUTEX.synchronize do
          # #11 fix: if the stored thread died before its ensure block ran
          # (e.g. Thread.new itself raised EAGAIN / system thread limit), the
          # running flag would be permanently stuck.  Detect this and self-heal.
          if self.class.reconciler_running &&
             self.class.reconciler_thread &&
             !self.class.reconciler_thread.alive?
            KafkaBatch.logger.warn(
              "[KafkaBatch][EventConsumer] reconciler thread died unexpectedly – resetting flag"
            )
            self.class.reconciler_running = false
          end

          # Skip if a reconciler thread is already in flight.
          next false if self.class.reconciler_running

          last = self.class.last_reconcile_at
          if last.nil? || (now - last) >= interval
            self.class.last_reconcile_at  = now
            self.class.reconciler_running = true
            true
          else
            false
          end
        end

        return unless should_run

        # #11 fix: store the thread reference so the watchdog above can check
        # alive? on subsequent poll cycles.
        thread = Thread.new do
          KafkaBatch::Reconciler.run(triggered_by: :consumer)
        rescue StandardError => e
          KafkaBatch.logger.warn(
            "[KafkaBatch][EventConsumer] reconciler thread error: #{e.class}: #{e.message}"
          )
        ensure
          # Always clear the running flag so the next interval can fire.
          RECONCILER_MUTEX.synchronize { self.class.reconciler_running = false }
        end
        self.class.reconciler_thread = thread
      rescue ThreadError => e
        # Thread creation failed (system thread limit hit) – release the flag
        # immediately so the next poll cycle can try again.
        KafkaBatch.logger.error(
          "[KafkaBatch][EventConsumer] Failed to spawn reconciler thread: #{e.message}"
        )
        RECONCILER_MUTEX.synchronize { self.class.reconciler_running = false }
      end

      # Apply the deduped, aggregated counter updates and fire callbacks for any
      # batch that just finished. On offset redelivery (e.g. callback produce
      # failed after the batch was finalized), events dedup but undispatched
      # callbacks are retried here without waiting for the reconciler.
      def apply(events)
        return if events.empty?

        result       = KafkaBatch.store.record_completions_batch(events)
        finished_ids = Set.new

        Array(result[:finished]).each do |f|
          batch = f[:batch]
          next unless batch

          finished_ids << batch[:id]
          produce_callback_for_batch!(batch, f[:outcome])
        end

        # Only DEDUPED (replayed) events can belong to a batch that was already
        # finalized in a prior poll whose callback never dispatched. On first
        # delivery :replays is empty, so this loop does zero extra Redis reads.
        Array(result[:replays]).each do |batch_id|
          next if finished_ids.include?(batch_id)

          retry_undispatched_callback(batch_id)
        end
      end

      def produce_callback_for_batch!(batch, outcome)
        trigger_callbacks(batch: batch, outcome: outcome)
      rescue KafkaBatch::ProducerError => e
        KafkaBatch.logger.error(
          "[KafkaBatch][EventConsumer] Failed to produce callback for " \
          "batch_id=#{batch[:id]}: #{e.message} – offset uncommitted, " \
          "will retry on redelivery or reconciler."
        )
        raise
      end

      def retry_undispatched_callback(batch_id)
        batch = KafkaBatch.store.find_batch(batch_id)
        return unless batch

        status = batch[:status].to_s
        return unless %w[success complete].include?(status)
        return if KafkaBatch.store.callback_dispatched?(batch_id)

        KafkaBatch.logger.info(
          "[KafkaBatch][EventConsumer] Retrying undispatched callback for " \
          "batch_id=#{batch_id} (terminal batch, events deduped on replay)"
        )
        produce_callback_for_batch!(batch, status)
      end

      # Decode + validate one event message. Returns a normalized event Hash, or
      # nil after handling a malformed (→ DLT) or incomplete (→ skip) message.
      def extract_event(message)
        data = begin
          decode(message.raw_payload)
        rescue ArgumentError => e
          KafkaBatch.logger.error(
            "[KafkaBatch][EventConsumer] Malformed JSON – forwarding to DLT: #{e.message}"
          )
          publish_to_dlt(raw: message.raw_payload.to_s, error: e, topic: message.topic)
          return nil
        end

        batch_id = data["batch_id"]
        job_id   = data["job_id"]
        status   = data["status"]
        unless batch_id && job_id && status
          KafkaBatch.logger.warn("[KafkaBatch][EventConsumer] Malformed event – skipping: #{data.inspect}")
          return nil
        end

        src_topic     = data["src_topic"]
        src_partition = data["src_partition"]
        src_offset    = data["src_offset"]
        if src_topic.nil? || src_partition.nil? || src_offset.nil?
          KafkaBatch.logger.warn(
            "[KafkaBatch][EventConsumer] Event missing source coords – skipping: #{data.inspect}"
          )
          return nil
        end

        batch_seq = data["batch_seq"]
        if batch_seq.nil? || batch_seq.to_i <= 0
          KafkaBatch.logger.warn(
            "[KafkaBatch][EventConsumer] Event missing batch_seq – skipping: #{data.inspect}"
          )
          return nil
        end

        {
          batch_id:         batch_id,
          job_id:           data["job_id"],
          batch_seq:        batch_seq.to_i,
          status:           status,
          source_topic:     src_topic,
          source_partition: src_partition,
          source_offset:    src_offset
        }
      end

      # Apply a single event immediately (used in tests and any single-message
      # path). Mirrors the batched flow with a one-element batch.
      def process_event(message)
        ev = extract_event(message)
        apply([ev].compact)
        mark_as_consumed!(message)
      end

      def trigger_callbacks(batch:, outcome:)
        KafkaBatch.logger.info(
          "[KafkaBatch][EventConsumer] Batch #{batch[:id]} finished – " \
          "outcome=#{outcome} jobs=#{batch[:total_jobs]} " \
          "ok=#{batch[:completed_count]} failed=#{batch[:failed_count]}"
        )

        KafkaBatch::Instrumentation.batch_completed(
          batch_id:        batch[:id],
          outcome:         outcome,
          total_jobs:      batch[:total_jobs],
          completed_count: batch[:completed_count],
          failed_count:    batch[:failed_count]
        )

        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.callbacks_topic,
          payload: {
            "batch_id"        => batch[:id],
            "outcome"         => outcome,          # "success" | "complete"
            "total_jobs"      => batch[:total_jobs],
            "completed_count" => batch[:completed_count],
            "failed_count"    => batch[:failed_count],
            "on_success"      => batch[:on_success],
            "on_complete"     => batch[:on_complete],
            "meta"            => batch[:meta],
            "finished_at"     => Time.now.iso8601
          },
          key: batch[:id]
        )
      end

      def publish_to_dlt(raw:, error:, topic:)
        KafkaBatch::Dlt.publish(
          payload: {
            "dlt_type"          => "malformed_event",
            "dlt_source_topic"  => topic,
            "dlt_raw_payload"   => raw,
            "dlt_error_class"   => error.class.name,
            "dlt_error_message" => error.message,
            "dlt_at"            => Time.now.iso8601
          },
          dlt_type:     "malformed_event",
          source_topic: topic
        )
      rescue KafkaBatch::ProducerError => e
        KafkaBatch.logger.error("[KafkaBatch][EventConsumer] DLT publish failed: #{e.message}")
        raise  # leave offset uncommitted → redelivery
      end

      def decode(raw)
        Oj.load(raw)
      rescue Oj::ParseError => e
        raise ArgumentError, "Invalid JSON in event: #{e.message}"
      end
    end
  end
end
