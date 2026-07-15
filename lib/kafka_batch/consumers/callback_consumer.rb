require "karafka"
require "oj"
require "securerandom"
require "time"

module KafkaBatch
  module Consumers
    # Karafka consumer that fires on_success / on_complete callbacks.
    #
    # Delivery semantics – single-winner via Redis:
    #   claim_callback (HSETNX callback_dispatched_at) runs BEFORE invoke so two
    #   consumers cannot both fire side effects. A crash between claim and invoke
    #   can lose the callback (batch stays marked dispatched); prefer that over
    #   double emails/webhooks. Matches kafka-batch-go Callback Processor.
    #
    #   Because callback messages are keyed by batch_id, all callbacks for a
    #   given batch land on the same partition and are processed sequentially
    #   by a single consumer in the common case; the Redis claim is the fence
    #   under rebalance / multi-consumer races.
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

        batch_id   = data["batch_id"]
        outcome    = data["outcome"]
        preclaimed = data["preclaimed"] ? true : false

        unless batch_id
          KafkaBatch.logger.warn("[KafkaBatch][CallbackConsumer] Missing batch_id – skipping")
          mark_as_consumed!(message)
          return
        end

        unless preclaimed
          # Fast path: skip Redis claim when another consumer already won.
          if KafkaBatch.store.callback_dispatched?(batch_id) &&
             outcome.to_s != "success_only"
            KafkaBatch.logger.debug(
              "[KafkaBatch][CallbackConsumer] batch_id=#{batch_id} callback already dispatched – skipping"
            )
            mark_as_consumed!(message)
            return
          end

          # Claim BEFORE invoke so two consumers cannot both fire side effects.
          kind = KafkaBatch::Callbacks::Dispatcher.claim_kind(outcome)
          unless KafkaBatch.store.claim_callback(batch_id, KafkaBatch.node_id, kind)
            # EXISTS-guarded claim returns false when the batch hash is gone
            # (TTL) as well as when another consumer won. Only skip on a real
            # lost race — still invoke when the batch is missing so poison
            # callback classes land in the DLT instead of being dropped.
            if KafkaBatch.store.find_batch(batch_id)
              KafkaBatch.logger.debug(
                "[KafkaBatch][CallbackConsumer] batch_id=#{batch_id} lost claim – skipping"
              )
              mark_as_consumed!(message)
              return
            end
            KafkaBatch.logger.warn(
              "[KafkaBatch][CallbackConsumer] batch_id=#{batch_id} missing – " \
              "invoking without claim (DLT poison path)"
            )
          end
        end

        KafkaBatch.logger.info(
          "[KafkaBatch][CallbackConsumer] batch_id=#{batch_id} outcome=#{outcome} " \
          "jobs=#{data['total_jobs']} ok=#{data['completed_count']} failed=#{data['failed_count']}"
        )

        fire_success  = %w[success success_only].include?(outcome.to_s)
        fire_complete = %w[success complete].include?(outcome.to_s)

        # on_success fires for success / success_only (legacy Ruby class only).
        if fire_success && present?(data["on_success"])
          spec = KafkaBatch::Callback.parse(data["on_success"])
          if spec.is_a?(KafkaBatch::Callback::Legacy)
            invoke_callback(data["on_success"], :on_success, data, message)
          end
        end

        # on_complete fires for success / complete (legacy Ruby class only).
        if fire_complete && present?(data["on_complete"])
          spec = KafkaBatch::Callback.parse(data["on_complete"])
          if spec.is_a?(KafkaBatch::Callback::Legacy)
            invoke_callback(data["on_complete"], :on_complete, data, message)
          end
        end

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
        begin
          publish_to_dlt(
            original_message: original_message,
            data:             batch_summary,
            error:            e,
            callback_class:   class_name,
            callback_method:  method_name,
            dlt_type:         "callback"
          )
        rescue KafkaBatch::ProducerError => pub_err
          KafkaBatch.logger.error(
            "[KafkaBatch][CallbackConsumer] DLT publish also failed for NameError path: #{pub_err.message}"
          )
        end
      rescue StandardError => e
        # Callback itself raised – forward to DLT with dlt_type "callback_error".
        # Claim already won; do not re-raise or every redelivery would re-run.
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
        begin
          publish_to_dlt(
            original_message: original_message,
            data:             batch_summary,
            error:            e,
            callback_class:   class_name,
            callback_method:  method_name,
            dlt_type:         "callback_error"
          )
        rescue KafkaBatch::ProducerError => pub_err
          KafkaBatch.logger.error(
            "[KafkaBatch][CallbackConsumer] DLT publish also failed for StandardError path: #{pub_err.message}"
          )
        end
      end

      def publish_to_dlt(original_message:, data:, error:, callback_class:, callback_method:,
                         dlt_type: "callback")
        KafkaBatch::Dlt.publish(
          payload: data.merge(
            "dlt_type"            => dlt_type,
            "dlt_callback_class"  => callback_class.to_s,
            "dlt_callback_method" => callback_method.to_s,
            "dlt_error_class"     => error.class.name,
            "dlt_error_message"   => error.message,
            "dlt_source_topic"    => original_message.topic,
            "dlt_at"              => Time.now.utc.iso8601
          ),
          key:          data["batch_id"],
          dlt_type:     dlt_type,
          source_topic: original_message.topic,
          batch_id:     data["batch_id"],
          job_id:       data["job_id"]
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
