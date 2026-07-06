# frozen_string_literal: true

require "securerandom"

module KafkaBatch
  # Central dead-letter publish helper — every DLT path should go through here
  # so +dlt.published+ instrumentation is never missed.
  module Dlt
    module_function

    # @param payload [Hash] full DLT message body (include +dlt_type+ field)
    # @param key [String, nil] Kafka message key
    # @param dlt_type [String] short label for metrics (may match payload["dlt_type"])
    # @param source_topic [String] topic the poison message came from
    # @param batch_id [String, nil] override when payload lacks batch_id
    # @param job_id [String, nil] override when payload lacks job_id
    # @raise [KafkaBatch::ProducerError] on produce failure (no instrumentation)
    def publish(payload:, dlt_type:, source_topic:, key: nil, batch_id: nil, job_id: nil)
      key ||= payload["job_id"] || payload["batch_id"] || SecureRandom.uuid

      KafkaBatch::Producer.produce_sync(
        topic:   KafkaBatch.config.dead_letter_topic,
        payload: payload,
        key:     key
      )

      KafkaBatch::Instrumentation.dlt_published(
        batch_id:     batch_id || payload["batch_id"],
        job_id:       job_id || payload["job_id"],
        dlt_type:     dlt_type,
        source_topic: source_topic
      )
    end
  end
end
