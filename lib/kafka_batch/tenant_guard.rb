# frozen_string_literal: true

require_relative "tenant_guard/recorder"
require_relative "tenant_guard/state"
require_relative "tenant_guard/control"

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

      # Per-tenant error-rate rows for active tenants that meet the minimum
      # sample count, using the effective guard thresholds. Consumed by the
      # alerts Sampler → tenant_error_rate_high rule. Returns [] when disabled.
      def error_rate_samples(at: Time.now)
        return [] unless enabled?

        win  = effective_window_seconds
        min  = effective_min_samples
        incl = effective_include_retries

        Recorder.active_tenants(within_seconds: win, at: at).filter_map do |tid|
          r = Recorder.error_rate(
            tid, window_seconds: win, min_samples: min, include_retries: incl, at: at
          )
          next unless r

          {
            "tenant_id" => tid,
            "rate"      => r[:rate],
            "samples"   => r[:samples],
            "ok"        => r[:ok],
            "fail"      => r[:fail],
            "retry"     => r[:retry]
          }
        end
      rescue StandardError
        []
      end

      # Effective thresholds. In this phase these read the static config; a later
      # phase layers the runtime settings page over them. Kept as one method each
      # so callers never branch on config vs settings.
      def effective_error_rate_pct
        KafkaBatch.config.tenant_guard_error_rate_pct.to_f
      end

      def effective_window_seconds
        KafkaBatch.config.tenant_guard_window_seconds.to_i
      end

      def effective_min_samples
        KafkaBatch.config.tenant_guard_min_samples.to_i
      end

      def effective_include_retries
        !!KafkaBatch.config.tenant_guard_include_retries
      end

      # ── Control surface (delegates to Control; used by API/UI + mitigation) ──
      def pause!(tenant_id, **kw)
        Control.pause!(tenant_id, **kw)
      end

      def throttle!(tenant_id, **kw)
        Control.throttle!(tenant_id, **kw)
      end

      # Release a single tenant's active control (resume/restore + audit close).
      def release!(tenant_id, **kw)
        Control.reset!(tenant_id, **kw)
      end

      def release_all!(**kw)
        Control.reset_all!(**kw)
      end

      def status(tenant_id)
        Control.status(tenant_id)
      end

      def list(**kw)
        Control.list(**kw)
      end

      # Testing hook: clears process-local caches (does NOT touch Redis state).
      def reset!
        @recorder_installed = false
        Recorder.reset!
        State.reset!
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
