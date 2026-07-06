# frozen_string_literal: true

require "time"

module KafkaBatch
  # Sidekiq-Enterprise-style job expiration: an immediately-produced job may carry
  # an optional +valid_till+ timestamp. Any consumer that would execute it after
  # that moment routes it to the dead-letter topic instead of running #perform.
  #
  # This is separate from delayed +enqueue_at+ (schedule poller): expired jobs are
  # produced to their normal worker topics immediately and only checked at
  # consumption time.
  module JobExpiry
    class ExpiredError < Error
      attr_reader :valid_till

      def initialize(valid_till)
        @valid_till = valid_till
        super("Job expired (valid_till=#{valid_till})")
      end
    end

    module_function

    # @param value [Time, Numeric, String, nil]
    # @return [String, nil] ISO8601 UTC timestamp for the message field
    def normalize_valid_till(value)
      return nil if value.nil?
      return nil if value.is_a?(String) && value.to_s.strip.empty?

      Batch.to_time(value).utc.iso8601
    end

    # @param data [Hash] decoded job message
    # @param now [Time]
    # @return [Boolean]
    def expired?(data, now: Time.now)
      till = data["valid_till"]
      return false if till.nil? || till.to_s.empty?

      Batch.to_time(till) <= now
    rescue ArgumentError, TypeError
      # Unparseable valid_till is a poison pill — treat as expired so consumers
      # route to DLT instead of redelivering forever.
      true
    end

    # Stamp immutable source coordinates on a fair-lane job the first time it is
    # consumed from ingest (used when expiry fires later in the forwarder).
    def stamp_source!(data, topic:, partition:, offset:)
      data["_src_topic"]     ||= topic
      data["_src_partition"] ||= partition
      data["_src_offset"]    ||= offset
      data
    end

    def source_coords(data, message: nil)
      if message
        [message.topic, message.partition, message.offset]
      else
        [
          data["_src_topic"] || data["src_topic"],
          data["_src_partition"] || data["src_partition"],
          data["_src_offset"] || data["src_offset"]
        ]
      end
    end

    # Core expired-job handling: DLT, batch event, uniq release, instrumentation.
    def drop!(data:, topic:, partition:, offset:, log_tag: "JobExpiry")
      job_id       = data["job_id"]
      batch_id     = data["batch_id"]
      worker_name  = data["worker_class"].to_s
      error        = ExpiredError.new(data["valid_till"])

      KafkaBatch.logger.info(
        "[KafkaBatch][#{log_tag}] job_id=#{job_id} expired " \
        "(valid_till=#{data['valid_till']}) – forwarding to DLT"
      )

      if batch_id && data["batch_seq"]
        emit_failed_event(
          data: data, batch_id: batch_id, job_id: job_id,
          worker_name: worker_name, topic: topic, partition: partition, offset: offset
        )
        record_failure(batch_id, job_id, worker_name, error)
      end

      KafkaBatch::Uniqueness.release_by_name(
        data["worker_class"], data["payload"] || {}, job_id: job_id
      )

      KafkaBatch::Instrumentation.job_expired(
        job_id:       job_id,
        batch_id:     batch_id,
        worker_class: worker_name,
        valid_till:   data["valid_till"]
      )

      publish_dlt(data: data, error: error, topic: topic)
    end

    def emit_failed_event(data:, batch_id:, job_id:, worker_name:, topic:, partition:, offset:)
      payload = {
        "batch_id"      => batch_id,
        "job_id"        => job_id,
        "status"        => "failed",
        "worker_class"  => worker_name,
        "occurred_at"   => Time.now.iso8601,
        "src_topic"     => topic,
        "src_partition" => partition,
        "src_offset"    => offset,
        "batch_seq"     => data["batch_seq"]
      }
      KafkaBatch::Producer.produce_sync(
        topic:   KafkaBatch.config.events_topic,
        payload: payload,
        key:     "#{topic}/#{partition}"
      )
    end

    def record_failure(batch_id, job_id, worker_name, error)
      KafkaBatch.store.record_failure(
        batch_id:      batch_id,
        job_id:        job_id,
        worker_class:  worker_name,
        error_class:   error.class.name,
        error_message: error.message,
        attempt:       0,
        status:        "expired",
        next_retry_at: nil
      )
    rescue StandardError => e
      KafkaBatch.logger.warn(
        "[KafkaBatch][JobExpiry] failed to record expiry for job_id=#{job_id}: #{e.message}"
      )
    end

    def publish_dlt(data:, error:, topic:)
      KafkaBatch::Dlt.publish(
        payload: data.merge(
          "dlt_type"          => "expired",
          "dlt_source_topic"  => topic,
          "dlt_error_class"   => error.class.name,
          "dlt_error_message" => error.message,
          "dlt_at"            => Time.now.iso8601
        ),
        key:          data["job_id"],
        dlt_type:     "expired",
        source_topic: topic,
        batch_id:     data["batch_id"],
        job_id:       data["job_id"]
      )
    end
  end
end
