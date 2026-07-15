# frozen_string_literal: true

require "oj"

module KafkaBatch
  module Callbacks
    # Dispatches batch callbacks when a batch finalizes or early-completes.
    #
    # Job callbacks (Sidekiq-style): enqueue a normal job to a user topic — Go or
    # Ruby runtime depending on handler manifest / worker registration.
    #
    # Legacy callbacks: produce to callbacks_topic for CallbackConsumer (Ruby class).
    #
    # Outcome-aware firing (matches kafka-batch-go):
    #   complete / early complete → on_complete only
    #   success                   → on_success + on_complete
    #   success_only              → on_success only
    #
    # When Lua already won HSETNX claims (EventConsumer / seal), pass
    # preclaimed: true to skip a second claim.
    module Dispatcher
      class << self
        # @return [Symbol] :none, :job_only, :legacy_only, or :mixed
        def dispatch!(batch:, outcome:, finished_at: nil, preclaimed: false)
          summary = batch_summary(batch, outcome, finished_at)
          job_actions = []
          legacy_needed = false

          fire_success  = %w[success success_only].include?(outcome.to_s)
          fire_complete = %w[success complete].include?(outcome.to_s)

          if fire_success && present?(batch[:on_success])
            case Callback.parse(batch[:on_success])
            when Callback::Job
              job_actions << { raw: batch[:on_success], kind: :on_success }
            when Callback::Legacy
              legacy_needed = true
            end
          end

          if fire_complete && present?(batch[:on_complete])
            case Callback.parse(batch[:on_complete])
            when Callback::Job
              job_actions << { raw: batch[:on_complete], kind: :on_complete }
            when Callback::Legacy
              legacy_needed = true
            end
          end

          return :none if job_actions.empty? && !legacy_needed

          if job_actions.any?
            # Claim BEFORE produce so two finalizers cannot both enqueue jobs —
            # unless Lua already claimed (preclaimed).
            unless preclaimed
              unless KafkaBatch.store.claim_callback(batch[:id], KafkaBatch.node_id, claim_kind(outcome))
                return :none
              end
            end

            job_actions.each do |action|
              produce_job_callback!(action[:raw], summary, kind: action[:kind])
            end

            if legacy_needed
              # Already claimed — CallbackConsumer would skip. Fire legacy inline.
              fire_legacy!(batch, summary, outcome)
              return :mixed
            end

            return :job_only
          end

          produce_legacy!(batch, outcome, summary, preclaimed: preclaimed)
          :legacy_only
        end

        def any_legacy?(batch)
          [batch[:on_success], batch[:on_complete]].compact.any? do |raw|
            spec = Callback.parse(raw)
            spec.is_a?(Callback::Legacy)
          end
        end

        def claim_kind(outcome)
          case outcome.to_s
          when "success", "success_only" then "success"
          else "complete"
          end
        end

        private

        def batch_summary(batch, outcome, finished_at)
          summary = {
            "batch_id"        => batch[:id],
            "outcome"         => outcome,
            "total_jobs"      => batch[:total_jobs],
            "completed_count" => batch[:completed_count],
            "failed_count"    => batch[:failed_count],
            "touched_count"   => batch[:touched_count],
            "callback_args"   => batch[:callback_args] || {},
            "finished_at"     => finished_at || batch[:finished_at] || Time.now.utc.iso8601,
            "description"     => batch[:description],
            "tenant_id"       => batch[:tenant_id]
          }
          summary["reconciled"] = batch[:reconciled] if batch.key?(:reconciled)
          summary.compact
        end

        def produce_job_callback!(raw_spec, summary, kind:)
          spec = Callback.parse(raw_spec)
          raise ArgumentError, "expected job callback" unless spec.is_a?(Callback::Job)

          definition = KafkaBatch::Batch.resolve_definition!(spec.job_type)
          batch_id     = summary["batch_id"]
          job_id       = "#{batch_id}:#{kind}"

          payload = summary.merge("callback_kind" => kind.to_s)
          message = KafkaBatch::Batch.build_message_for(
            definition: definition,
            payload:    payload,
            job_id:     job_id,
            batch_id:   nil,
            attempt:    0
          )

          route = route_for(spec, definition, job_id: job_id, batch_id: batch_id)
          KafkaBatch::Producer.produce_sync(
            topic:     route[:topic],
            payload:   message,
            key:       route[:key],
            partition: route[:partition]
          )

          KafkaBatch::Instrumentation.callback_invoked(
            batch_id:        batch_id,
            callback_class:  spec.job_type,
            callback_method: kind.to_s
          )
        end

        def fire_legacy!(batch, summary, outcome)
          payload = summary.merge(
            "on_success"  => batch[:on_success],
            "on_complete" => batch[:on_complete]
          )
          fire_success  = %w[success success_only].include?(outcome.to_s)
          fire_complete = %w[success complete].include?(outcome.to_s)

          if fire_success && legacy_class?(batch[:on_success])
            invoke_legacy(batch[:on_success], :on_success, payload)
          end
          if fire_complete && legacy_class?(batch[:on_complete])
            invoke_legacy(batch[:on_complete], :on_complete, payload)
          end
        end

        def legacy_class?(raw)
          present?(raw) && Callback.parse(raw).is_a?(Callback::Legacy)
        end

        def invoke_legacy(class_name, method_name, batch_summary)
          klass = Object.const_get(class_name)
          unless klass.method_defined?(method_name)
            error = NoMethodError.new("#{class_name} does not define ##{method_name}")
            KafkaBatch.logger.error(
              "[KafkaBatch][Callbacks::Dispatcher] #{error.message} – sending to DLT"
            )
            publish_legacy_dlt(batch_summary, error, class_name, method_name, "callback")
            return
          end

          klass.new.public_send(method_name, batch_summary)
          KafkaBatch::Instrumentation.callback_invoked(
            batch_id:        batch_summary["batch_id"],
            callback_class:  class_name,
            callback_method: method_name.to_s
          )
        rescue NameError, StandardError => e
          dlt_type = e.is_a?(NameError) ? "callback" : "callback_error"
          KafkaBatch.logger.error(
            "[KafkaBatch][Callbacks::Dispatcher] #{class_name}##{method_name} " \
            "#{e.class}: #{e.message}"
          )
          KafkaBatch::Instrumentation.callback_failed(
            batch_id:        batch_summary["batch_id"],
            callback_class:  class_name,
            callback_method: method_name.to_s,
            error:           e
          )
          publish_legacy_dlt(batch_summary, e, class_name, method_name, dlt_type)
        end

        def publish_legacy_dlt(batch_summary, error, class_name, method_name, dlt_type)
          KafkaBatch::Dlt.publish(
            payload: batch_summary.merge(
              "dlt_type"            => dlt_type,
              "dlt_callback_class"  => class_name.to_s,
              "dlt_callback_method" => method_name.to_s,
              "dlt_error_class"     => error.class.name,
              "dlt_error_message"   => error.message,
              "dlt_source_topic"    => KafkaBatch.config.callbacks_topic,
              "dlt_at"              => Time.now.utc.iso8601
            ),
            key:          batch_summary["batch_id"],
            dlt_type:     dlt_type,
            source_topic: KafkaBatch.config.callbacks_topic,
            batch_id:     batch_summary["batch_id"]
          )
        rescue KafkaBatch::ProducerError => e
          KafkaBatch.logger.error(
            "[KafkaBatch][Callbacks::Dispatcher] DLT publish failed: #{e.message}"
          )
        end

        def route_for(spec, definition, job_id:, batch_id:)
          if spec.topic && !spec.topic.empty?
            topic = topic_includes_prefix?(spec.topic) ? spec.topic : KafkaBatch.config.resolve_topic(spec.topic)
            { topic: topic, key: job_id, partition: nil }
          elsif definition.fairness?
            raise ConfigurationError,
                  "callback job_type=#{spec.job_type.inspect} uses fairness — set an explicit topic"
          else
            KafkaBatch::Batch.route_for_definition(definition, job_id: job_id, batch_id: batch_id)
          end
        end

        def topic_includes_prefix?(topic)
          prefix = KafkaBatch.config.topic_prefix.to_s.strip
          return true if prefix.empty?

          topic.start_with?("#{prefix}.")
        end

        def produce_legacy!(batch, outcome, summary, preclaimed: false)
          payload = summary.merge(
            "on_success"  => batch[:on_success],
            "on_complete" => batch[:on_complete],
            "preclaimed"  => preclaimed
          )
          KafkaBatch::Producer.produce_sync(
            topic:   KafkaBatch.config.callbacks_topic,
            payload: payload,
            key:     batch[:id]
          )
        end

        def present?(value)
          !value.nil? && !value.to_s.strip.empty?
        end
      end
    end
  end
end
