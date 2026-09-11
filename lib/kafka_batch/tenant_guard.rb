# frozen_string_literal: true

require_relative "tenant_guard/recorder"
require_relative "tenant_guard/settings"
require_relative "tenant_guard/state"
require_relative "tenant_guard/control"
require_relative "tenant_guard/mitigation"
require_relative "tenant_guard/reconciler"

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
      # Whether the guard is active. Effective value = runtime settings page
      # (Redis) layered over the static config default; cached per-process so the
      # hot job-event path stays cheap.
      def enabled?
        !!Settings.effective["enabled"]
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

      # Effective thresholds = runtime settings layered over config defaults.
      def effective_error_rate_pct
        Settings.effective["error_rate_pct"].to_f
      end

      def effective_window_seconds
        Settings.effective["window_seconds"].to_i
      end

      def effective_min_samples
        Settings.effective["min_samples"].to_i
      end

      def effective_include_retries
        !!Settings.effective["include_retries"]
      end

      # Full effective settings hash (dashboard + mitigation policy).
      def settings(refresh: false)
        Settings.effective(refresh: refresh)
      end

      def update_settings(partial)
        Settings.update(partial)
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

      # ── Reconciler (auto-disengage + drift repair; control plane only) ──────
      # Start the background loop. Idempotent, self-gating, and NX-locked so
      # multiple replicas are safe — call it unconditionally on boot.
      #
      # Deliberately NOT gated on `enabled?`: the loop re-reads the Redis-backed
      # settings every tick, which is what lets the /tenant_guard page turn the
      # guard on and off at runtime. Gating thread start on the boot value is
      # what used to strand the UI toggle on a control plane that booted with
      # the guard off. While disabled, mitigation no-ops each tick and only
      # reconciliation runs, so a control left behind by a disable still
      # auto-releases.
      def start_reconciler!(**kw)
        return unless should_run_reconciler?

        Reconciler.start!(**kw)
      end

      # Redis + control plane. Mirrors Alerts.should_run_evaluator?.
      def should_run_reconciler?
        return false unless KafkaBatch.config.redis_configured?

        control_plane_process?
      end

      def reconcile_once!(**kw)
        Reconciler.reconcile_once!(**kw)
      end

      # One auto-mitigation pass (breach → action). Normally driven by the
      # reconciler loop; exposed for tests and manual triggering.
      def mitigate_once!(**kw)
        Mitigation.run_once!(**kw)
      end

      # Whether this process should host the guard control loop. Reuses the
      # alerts control-plane detection so the guard runs where alerts run.
      def control_plane_process?
        if defined?(KafkaBatch::Alerts) && KafkaBatch::Alerts.respond_to?(:control_plane_process?)
          KafkaBatch::Alerts.control_plane_process?
        else
          true
        end
      end

      # Testing hook: clears process-local caches (does NOT touch Redis state).
      # Settings is included because its 5s `effective` cache is process-local
      # too — leaving it behind leaks `enabled` (and every threshold) into the
      # next test, which then reads a guard that Redis says is off as on.
      def reset!
        @recorder_installed = false
        Reconciler.stop!
        Recorder.reset!
        State.reset!
        Settings.reset!
      end

      private

      def subscribe_job_event(event)
        ActiveSupport::Notifications.subscribe(/#{Regexp.escape(event)}\.kafka_batch\z/) do |*args|
          # Whole body guarded: AS::Notifications does not rescue subscriber
          # exceptions, and this runs on the job thread — recording must never
          # raise into the hot path (payload parse included).
          begin
            next unless enabled?

            payload = ActiveSupport::Notifications::Event.new(*args).payload || {}
            tid = payload[:tenant_id] || payload["tenant_id"]
            next if tid.nil? || tid.to_s.empty?

            yield tid.to_s
          rescue StandardError
            nil
          end
        end
      end
    end
  end
end
