# frozen_string_literal: true

require_relative "tenant_guard/recorder"

module KafkaBatch
  # Per-tenant error-rate guard (fairness lanes only). Watches a sliding
  # error-rate window per tenant and — when enabled — can auto-mitigate a hot
  # tenant by throttling its fairness weight and/or pausing its dedicated ingest
  # partition, always recording the action and firing a host callback.
  #
  # This module is the entry point; the pieces live under tenant_guard/:
  #   Recorder — the sliding window counters (this phase)
  #   (later)  — evaluator rule, control service, reconciler, settings store
  #
  # Nothing here ever raises into the job hot path.
  module TenantGuard
    class << self
      # Whether the guard is active. In this phase this reads the static config
      # flag; a later phase layers the runtime settings page over it (effective
      # = Redis settings ← config). Kept as a single method so callers never
      # branch on config vs settings directly.
      def enabled?
        KafkaBatch.config.tenant_guard_enabled
      rescue StandardError
        false
      end

      # Subscribe the recorder to job lifecycle events on this process. Safe to
      # call more than once (idempotent) and safe to call on every process —
      # recording only happens for fairness jobs (tenant_id present) while the
      # guard is enabled. Records are fire-and-forget.
      def install_recorder!
        return if @recorder_installed
        return unless defined?(ActiveSupport::Notifications)

        @recorder_installed = true

        subscribe_job_event("job.processed") { |tid| Recorder.record_ok(tid) }
        subscribe_job_event("job.failed")    { |tid| Recorder.record_fail(tid) }
        subscribe_job_event("job.retried")   { |tid| Recorder.record_retry(tid) }
        nil
      end

      # Testing / reset hook.
      def reset!
        @recorder_installed = false
        Recorder.reset!
      end

      private

      def subscribe_job_event(event)
        ActiveSupport::Notifications.subscribe(/#{Regexp.escape(event)}\.kafka_batch\z/) do |*args|
          next unless enabled?

          payload = ActiveSupport::Notifications::Event.new(*args).payload || {}
          tid = payload[:tenant_id] || payload["tenant_id"]
          next if tid.nil? || tid.to_s.empty?

          begin
            yield tid.to_s
          rescue StandardError
            nil
          end
        end
      end
    end
  end
end
