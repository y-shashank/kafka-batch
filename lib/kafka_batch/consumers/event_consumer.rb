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
      def consume
        messages.each { |msg| process_event(msg) }
      end

      private

      def process_event(message)
        data = begin
          decode(message.raw_payload)
        rescue ArgumentError => e
          # Unparseable JSON: forward to DLT so nothing is silently dropped.
          KafkaBatch.logger.error(
            "[KafkaBatch][EventConsumer] Malformed JSON – forwarding to DLT: #{e.message}"
          )
          publish_to_dlt(
            raw:   message.raw_payload.to_s,
            error: e,
            topic: message.topic
          )
          mark_as_consumed!(message)
          return
        end

        batch_id = data["batch_id"]
        job_id   = data["job_id"]
        status   = data["status"]

        unless batch_id && status
          KafkaBatch.logger.warn("[KafkaBatch][EventConsumer] Malformed event – skipping: #{data.inspect}")
          mark_as_consumed!(message)
          return
        end

        KafkaBatch.logger.debug(
          "[KafkaBatch][EventConsumer] batch_id=#{batch_id} job_id=#{job_id} status=#{status}"
        )

        src_topic     = data["src_topic"]
        src_partition = data["src_partition"]
        src_offset    = data["src_offset"]

        if src_topic.nil? || src_partition.nil? || src_offset.nil?
          KafkaBatch.logger.warn(
            "[KafkaBatch][EventConsumer] Event missing source coords – skipping: #{data.inspect}"
          )
          mark_as_consumed!(message)
          return
        end

        result = KafkaBatch.store.record_completion_by_offset(
          batch_id:         batch_id,
          source_topic:     src_topic,
          source_partition: src_partition,
          source_offset:    src_offset,
          status:           status
        )

        case result[:status]
        when :done
          trigger_callbacks(batch: result[:batch], outcome: result[:outcome])
        when :duplicate
          KafkaBatch.logger.debug(
            "[KafkaBatch][EventConsumer] Duplicate event – job_id=#{job_id} already recorded"
          )
        when :not_found
          KafkaBatch.logger.warn(
            "[KafkaBatch][EventConsumer] Batch not found: #{batch_id} (job_id=#{job_id})"
          )
        when :continue
          # nothing – batch still running
        end

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
