require "karafka"
require "oj"
require "securerandom"
require "time"

module KafkaBatch
  module Consumers
    # Karafka consumer that fires on_success / on_complete callbacks.
    #
    # Safety guarantees:
    #   1. At-most-once callback invocation – an atomic store claim
    #      (claim_callback) acts as a compare-and-swap before any callback
    #      fires.  If this consumer crashes after the callback runs but before
    #      committing the offset, the re-delivered message will fail the claim
    #      and be skipped.
    #   2. Unresolvable callback classes are forwarded to the dead-letter topic
    #      instead of being silently dropped.
    #   3. Callback exceptions are also forwarded to the DLT (dlt_type:
    #      "callback_error") so they are not silently lost.
    class CallbackConsumer < Karafka::BaseConsumer
      def consume
        messages.each { |msg| process_callback(msg) }
      end

      private

      def process_callback(message)
        data = begin
          decode(message.raw_payload)
        rescue ArgumentError => e
          # Unparseable JSON: forward to DLT so nothing is silently dropped.
          KafkaBatch.logger.error(
            "[KafkaBatch][CallbackConsumer] Malformed JSON – forwarding to DLT: #{e.message}"
          )
          publish_to_dlt(
            original_message: message,
            data:             { "dlt_raw_payload" => message.raw_payload.to_s },
            error:            e,
            callback_class:   nil,
            callback_method:  nil,
            dlt_type:         "malformed_callback"
          )
          mark_as_consumed!(message)
          return
        end

        batch_id = data["batch_id"]
        outcome  = data["outcome"]

        unless batch_id
          KafkaBatch.logger.warn("[KafkaBatch][CallbackConsumer] Missing batch_id – skipping")
          mark_as_consumed!(message)
          return
        end

        # ── Atomic claim: only one consumer process may invoke the callback ─
        # claim_callback does UPDATE WHERE callback_dispatched_at IS NULL.
        # If another process already won the race, rows_affected = 0 → skip.
        unless KafkaBatch.store.claim_callback(batch_id)
          KafkaBatch.logger.debug(
            "[KafkaBatch][CallbackConsumer] batch_id=#{batch_id} callback already claimed – skipping"
          )
          mark_as_consumed!(message)
          return
        end

        KafkaBatch.logger.info(
          "[KafkaBatch][CallbackConsumer] batch_id=#{batch_id} outcome=#{outcome} " \
          "jobs=#{data['total_jobs']} ok=#{data['completed_count']} failed=#{data['failed_count']}"
        )

        # on_success fires only when every job succeeded
        if outcome == "success" && data["on_success"].present_str?
          invoke_callback(data["on_success"], :on_success, data, message)
        end

        # on_complete fires for any terminal outcome
        if data["on_complete"].present_str?
          invoke_callback(data["on_complete"], :on_complete, data, message)
        end

        mark_as_consumed!(message)
      end

      def invoke_callback(class_name, method_name, batch_summary, original_message)
        klass = Object.const_get(class_name)

        unless klass.method_defined?(method_name)
          KafkaBatch.logger.error(
            "[KafkaBatch][CallbackConsumer] #{class_name} does not respond to ##{method_name}"
          )
          return
        end

        klass.new.public_send(method_name, batch_summary)

        KafkaBatch::Instrumentation.callback_invoked(
          batch_id:        batch_summary["batch_id"],
          callback_class:  class_name,
          callback_method: method_name
        )

      rescue NameError => e
        # Class doesn't exist – forward to DLT so it isn't silently lost.
        KafkaBatch.logger.error(
          "[KafkaBatch][CallbackConsumer] Cannot resolve '#{class_name}': #{e.message} – sending to DLT"
        )
        KafkaBatch::Instrumentation.callback_failed(
          batch_id:        batch_summary["batch_id"],
          callback_class:  class_name,
          callback_method: method_name,
          error:           e
        )
        publish_to_dlt(
          original_message: original_message,
          data:             batch_summary,
          error:            e,
          callback_class:   class_name,
          callback_method:  method_name,
          dlt_type:         "callback"
        )
      rescue StandardError => e
        # Callback itself raised – forward to DLT with dlt_type "callback_error".
        # The claim was already made so a retry would not re-invoke the callback,
        # but forwarding to DLT ensures the failure is visible and replayable.
        KafkaBatch.logger.error(
          "[KafkaBatch][CallbackConsumer] #{class_name}##{method_name} raised " \
          "#{e.class}: #{e.message}\n#{e.backtrace&.first(5)&.join("\n")}"
        )
        KafkaBatch::Instrumentation.callback_failed(
          batch_id:        batch_summary["batch_id"],
          callback_class:  class_name,
          callback_method: method_name,
          error:           e
        )
        publish_to_dlt(
          original_message: original_message,
          data:             batch_summary,
          error:            e,
          callback_class:   class_name,
          callback_method:  method_name,
          dlt_type:         "callback_error"
        )
      end

      def publish_to_dlt(original_message:, data:, error:, callback_class:, callback_method:,
                         dlt_type: "callback")
        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.dead_letter_topic,
          payload: data.merge(
            "dlt_type"              => dlt_type,
            "dlt_callback_class"    => callback_class.to_s,
            "dlt_callback_method"   => callback_method.to_s,
            "dlt_error_class"       => error.class.name,
            "dlt_error_message"     => error.message,
            "dlt_source_topic"      => original_message.topic,
            "dlt_at"                => Time.now.iso8601
          ),
          key: data["batch_id"] || SecureRandom.uuid
        )
      rescue KafkaBatch::ProducerError => e
        KafkaBatch.logger.error(
          "[KafkaBatch][CallbackConsumer] DLT publish failed: #{e.message}"
        )
        raise  # leave offset uncommitted → redelivery
      end

      def decode(raw)
        Oj.load(raw)
      rescue Oj::ParseError => e
        raise ArgumentError, "Invalid JSON in callback message: #{e.message}"
      end
    end
  end
end

# Minimal helper to avoid pulling in ActiveSupport for blank? checks
class String
  def present_str?
    !nil? && !empty?
  end
end
