require "karafka"
require "oj"
require "securerandom"
require "time"

module KafkaBatch
  module Consumers
    # Karafka consumer that fires on_success / on_complete callbacks.
    #
    # Delivery semantics – at-least-once (callbacks must be idempotent):
    #   The callback is invoked FIRST, then the dispatch is claimed
    #   (claim_callback marks callback_dispatched_at).  This ordering means a
    #   crash between invocation and claim results in re-invocation on
    #   redelivery – never a lost callback.  This matches Sidekiq-Pro's
    #   "callbacks may run more than once" guarantee.
    #
    #   Because callback messages are keyed by batch_id, all callbacks for a
    #   given batch land on the same partition and are processed sequentially
    #   by a single consumer, so a pre-dispatch check (callback_dispatched?)
    #   cheaply suppresses duplicates in the normal (non-crash) path.
    #
    # Safety guarantees:
    #   1. Unresolvable callback classes are forwarded to the dead-letter topic
    #      instead of being silently dropped.
    #   2. Callback exceptions are also forwarded to the DLT (dlt_type:
    #      "callback_error") so they are not silently lost.
    class CallbackConsumer < Karafka::BaseConsumer
      prepend ConsumptionGate
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

        # ── Duplicate suppression (normal path) ────────────────────────────
        # If the dispatch was already claimed, the callback has already run.
        # Callback messages are keyed by batch_id → same partition → processed
        # sequentially, so this read reliably skips duplicates without a race.
        if KafkaBatch.store.callback_dispatched?(batch_id)
          KafkaBatch.logger.debug(
            "[KafkaBatch][CallbackConsumer] batch_id=#{batch_id} callback already dispatched – skipping"
          )
          mark_as_consumed!(message)
          return
        end

        KafkaBatch.logger.info(
          "[KafkaBatch][CallbackConsumer] batch_id=#{batch_id} outcome=#{outcome} " \
          "jobs=#{data['total_jobs']} ok=#{data['completed_count']} failed=#{data['failed_count']}"
        )

        # on_success fires only when every job succeeded
        if outcome == "success" && present?(data["on_success"])
          invoke_callback(data["on_success"], :on_success, data, message)
        end

        # on_complete fires for any terminal outcome
        if present?(data["on_complete"])
          invoke_callback(data["on_complete"], :on_complete, data, message)
        end

        # ── Claim AFTER invocation ─────────────────────────────────────────
        # Marking dispatch only after the callbacks have run guarantees that a
        # crash mid-invocation leads to re-invocation (at-least-once), never a
        # silently lost callback. Record which pod/process ran it for tracking.
        KafkaBatch.store.claim_callback(batch_id, KafkaBatch.node_id)

        mark_as_consumed!(message)
      end

      def invoke_callback(class_name, method_name, batch_summary, original_message)
        klass = Object.const_get(class_name)

        unless klass.method_defined?(method_name)
          # Class resolves but the callback method is missing (e.g. method
          # renamed after deploy). Forward to the DLT so it isn't silently
          # dropped – consistent with the unresolvable-class path below.
          error = NoMethodError.new("#{class_name} does not define ##{method_name}")
          KafkaBatch.logger.error(
            "[KafkaBatch][CallbackConsumer] #{error.message} – sending to DLT"
          )
          KafkaBatch::Instrumentation.callback_failed(
            batch_id:        batch_summary["batch_id"],
            callback_class:  class_name,
            callback_method: method_name,
            error:           error
          )
          publish_to_dlt(
            original_message: original_message,
            data:             batch_summary,
            error:            error,
            callback_class:   class_name,
            callback_method:  method_name,
            dlt_type:         "callback"
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

      # True only for a non-empty String. Safe on nil (no monkey-patch, no
      # ActiveSupport dependency) – callback fields are nil when not configured.
      def present?(value)
        value.is_a?(String) && !value.empty?
      end
    end
  end
end
