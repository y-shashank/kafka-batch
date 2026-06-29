require "karafka"
require "oj"
require "securerandom"
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
      # Apply a whole poll's completion events in ONE store call (per-poll
      # batching). This collapses N per-event transactions/round-trips into one,
      # which dramatically reduces hot-batch-row lock contention on the MySQL
      # store (and round-trips on Redis). Dedup/finalization stay exactly-once:
      # the store deduplicates each event by its source offset, so re-delivering
      # this whole batch (if we crash before committing the offset) never
      # double-counts and never drops a job — protecting callback correctness.
      def consume
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

      # Apply the deduped, aggregated counter updates and fire callbacks for any
      # batch that just finished.
      def apply(events)
        return if events.empty?

        KafkaBatch.store.record_completions_batch(events).each do |f|
          trigger_callbacks(batch: f[:batch], outcome: f[:outcome])
        end
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
        status   = data["status"]
        unless batch_id && status
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

        {
          batch_id:         batch_id,
          job_id:           data["job_id"],
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
        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.dead_letter_topic,
          payload: {
            "dlt_type"          => "malformed_event",
            "dlt_source_topic"  => topic,
            "dlt_raw_payload"   => raw,
            "dlt_error_class"   => error.class.name,
            "dlt_error_message" => error.message,
            "dlt_at"            => Time.now.iso8601
          },
          key: SecureRandom.uuid
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
