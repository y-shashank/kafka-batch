require "karafka"
require "oj"
require "time"

module KafkaBatch
  module Consumers
    # Dedicated consumer for the retry topic.
    #
    # The retry topic is the mechanism that replaces sleep() inside JobConsumer.
    # When a job fails and has retries remaining, JobConsumer produces a message
    # to the retry topic with two extra fields:
    #
    #   retry_after – ISO8601 timestamp: the earliest time this job should
    #                 be re-enqueued to its original topic.
    #   retry_to    – the original Kafka topic to re-enqueue to.
    #
    # This consumer reads those messages and:
    #   - If retry_after is still in the future: uses Karafka's pause() to
    #     suspend polling this partition until the message is due.  No threads
    #     are blocked, no resources are wasted.
    #   - If retry_after is in the past (or now): produces the message back to
    #     retry_to (without the retry_after/retry_to fields) and commits.
    #
    # Partition ordering is preserved: messages in the same partition are always
    # processed in order, so earlier retries are re-enqueued before later ones.
    # Using batch_id as the message key routes all jobs for the same batch to
    # the same retry partition – that's fine because retries for different jobs
    # are independent.
    class RetryConsumer < Karafka::BaseConsumer
      prepend ConsumptionGate
      # Maximum time (seconds) to pause per wait cycle.
      # Capped so the consumer isn't suspended for extremely long backoffs in
      # one go; Karafka will re-deliver from the same offset after each pause.
      MAX_PAUSE_SECONDS = 30

      def consume
        # Process in offset order and STOP at the first not-yet-due message.
        # Once we pause() the partition we must not advance the offset past the
        # paused message: marking a later (already-due) message as consumed would
        # commit a higher offset and permanently skip the paused retry, dropping
        # the job (it would stay "retrying" forever and its batch never finishes).
        # process_retry returns false when it paused, true otherwise.
        messages.each do |msg|
          break unless process_retry(msg)
        end
      end

      private

      # @return [Boolean] true if the message was handled (offset may advance),
      #   false if the partition was paused and the loop must stop.
      def process_retry(message)
        data        = decode(message.raw_payload)
        retry_after = parse_time(data["retry_after"])
        retry_to    = data["retry_to"]

        # Unroutable: no original topic to retry to. Don't silently drop a batch
        # job – emit a 'failed' completion event so the batch can still finish,
        # then DLT for manual recovery (mirrors JobConsumer's poison-pill path).
        unless retry_to
          KafkaBatch.logger.error(
            "[KafkaBatch][RetryConsumer] Unroutable retry message (missing retry_to) " \
            "– failing job and forwarding to DLT: #{data.inspect}"
          )
          emit_failed_event(data, message)
          publish_to_dlt(data: data, topic: message.topic)
          mark_as_consumed!(message)
          return true
        end

        # A missing/invalid retry_after means "due now" – retry immediately
        # rather than discarding the job.
        wait_seconds = retry_after ? (retry_after - Time.now) : 0

        if wait_seconds > 0
          # Not ready yet – pause this partition.
          # Karafka will resume from message.offset after pause_ms milliseconds,
          # at which point we check again.  No thread is blocked.
          pause_ms = ([wait_seconds, MAX_PAUSE_SECONDS].min * 1000).ceil

          KafkaBatch.logger.debug(
            "[KafkaBatch][RetryConsumer] job_id=#{data['job_id']} not due for " \
            "#{wait_seconds.round(1)}s – pausing partition for #{pause_ms}ms"
          )

          # pause(seek_offset, timeout_in_ms):
          # Karafka will seek back to message.offset and resume after pause_ms.
          pause(message.offset, pause_ms)
          return false  # stop the batch loop – do NOT advance past this offset
        end

        # ── Due: re-enqueue to original topic ──────────────────────────────
        KafkaBatch.logger.info(
          "[KafkaBatch][RetryConsumer] Re-enqueuing job_id=#{data['job_id']} " \
          "attempt=#{data['attempt']} to #{retry_to}"
        )

        # Strip retry metadata before re-enqueuing so the JobConsumer sees a
        # clean message identical to the original job format.
        job_message = data.reject { |k, _| %w[retry_after retry_to].include?(k) }

        KafkaBatch::Producer.produce_sync(
          topic:   retry_to,
          payload: job_message,
          key:     data["job_id"]
        )

        mark_as_consumed!(message)
        true

      rescue KafkaBatch::ProducerError => e
        # Produce failed – do NOT commit. Karafka redelivers and we retry
        # the re-enqueue on the next poll cycle.
        KafkaBatch.logger.error(
          "[KafkaBatch][RetryConsumer] Failed to re-enqueue job_id=#{data['job_id']}: #{e.message}"
        )
        raise
      end

      # Emit a 'failed' completion event so a dropped/unroutable retry still
      # advances its batch toward completion. Dedup is keyed by this retry
      # message's own coordinates (unique per retry-topic partition/offset).
      # Best-effort: a standalone job (no batch_id) needs no event, and an emit
      # failure must not block DLT routing of the poison message.
      def emit_failed_event(data, message)
        batch_id = data["batch_id"]
        return unless batch_id

        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.events_topic,
          payload: {
            "batch_id"      => batch_id,
            "job_id"        => data["job_id"],
            "status"        => "failed",
            "worker_class"  => data["worker_class"].to_s,
            "occurred_at"   => Time.now.iso8601,
            "src_topic"     => message.topic,
            "src_partition" => message.partition,
            "src_offset"    => message.offset
          },
          key: "#{message.topic}/#{message.partition}"
        )
      rescue KafkaBatch::ProducerError => e
        KafkaBatch.logger.error(
          "[KafkaBatch][RetryConsumer] Failed to emit failed-event for " \
          "job_id=#{data['job_id']}: #{e.message}"
        )
      end

      def publish_to_dlt(data:, topic:)
        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.dead_letter_topic,
          payload: data.merge(
            "dlt_type"         => "retry_routing",
            "dlt_source_topic" => topic,
            "dlt_at"           => Time.now.iso8601
          ),
          key: data["job_id"]
        )
      rescue KafkaBatch::ProducerError => e
        KafkaBatch.logger.error("[KafkaBatch][RetryConsumer] DLT publish failed: #{e.message}")
        raise  # leave offset uncommitted → redelivery
      end

      def decode(raw)
        Oj.load(raw)
      rescue Oj::ParseError
        {}
      end

      def parse_time(str)
        return nil if str.nil? || str.empty?
        Time.parse(str)
      rescue ArgumentError, TypeError
        nil
      end
    end
  end
end
