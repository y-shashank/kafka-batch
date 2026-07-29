# frozen_string_literal: true

require "time"
require_relative "state"
require_relative "control"
require_relative "recorder"

module KafkaBatch
  module TenantGuard
    # Turns error-rate breaches into automatic actions per the configured policy
    # (:none | :throttle | :pause | :throttle_then_pause). Runs on the control
    # plane (NX-locked), fires the host callback + instrument event on every
    # action, and honors dry-run (evaluate + notify, take no action).
    #
    # Idempotent per tick: a tenant already under a guard control is not
    # re-actioned; a tenant under a MANUAL control is never overridden.
    module Mitigation
      GUARD_SOURCE = "error_rate_guard"

      class << self
        def run_once!(at: Time.now)
          return empty_summary unless TenantGuard.enabled? && State.available?

          s = TenantGuard.settings
          mode = s["mitigation"].to_s
          return empty_summary if mode == "none"
          return empty_summary unless State.try_lock!(ttl: lock_ttl)

          begin
            evaluate_and_act(s, mode, at: at)
          ensure
            State.unlock!
          end
        end

        private

        def evaluate_and_act(s, mode, at:)
          win       = s["window_seconds"].to_i
          min       = s["min_samples"].to_i
          incl      = !!s["include_retries"]
          threshold = s["error_rate_pct"].to_f
          grace     = s["grace_ticks"].to_i
          dry       = !!s["dry_run"]
          throttle_w = s["throttle_weight"].to_f
          auto      = s["auto_release_seconds"]
          until_ts  = auto.nil? ? nil : (at.to_i + auto.to_i)

          summary = empty_summary
          Recorder.active_tenants(within_seconds: win, at: at).each do |tid|
            r = Recorder.error_rate(tid, window_seconds: win, min_samples: min, include_retries: incl, at: at)
            next if r.nil?

            summary[:evaluated] += 1

            if r[:rate] < threshold
              State.reset_breach!(tid)
              next
            end

            # Breach — apply grace: warn for `grace` ticks, act on the next.
            ticks = State.incr_breach!(tid, ttl: win + 120)
            if ticks <= grace
              summary[:warned] += 1
              next
            end

            act_on_tenant(tid, mode: mode, rate: r, threshold: threshold,
                          throttle_w: throttle_w, until_ts: until_ts, dry: dry,
                          at: at, summary: summary)
          rescue StandardError => e
            KafkaBatch.logger.warn("[KafkaBatch][TenantGuard::Mitigation] tenant=#{tid}: #{e.message}")
          end
          summary
        end

        def act_on_tenant(tid, mode:, rate:, threshold:, throttle_w:, until_ts:, dry:, at:, summary:)
          existing = State.get_record(tid)
          action = next_action(mode, existing)

          # Nothing to do (already at terminal guard state, or operator-owned).
          if action.nil?
            summary[:skipped] += 1
            return
          end
          # Never override a manual control.
          if existing && existing["source"].to_s != GUARD_SOURCE
            summary[:skipped] += 1
            return
          end

          payload = build_payload(tid, action, mode, rate, threshold, until_ts, dry, at)

          if dry
            summary[:dry_run] += 1
            fire(payload)
            return
          end

          apply_action(tid, action, throttle_w: throttle_w, until_ts: until_ts,
                       rate: rate, threshold: threshold)
          summary[:acted] += 1
          fire(payload)
        end

        # Decide the action for this tenant given the mode and current control.
        # For :throttle_then_pause a tenant that is already guard-throttled and
        # still breaching escalates to pause; a fresh breach starts at throttle.
        # Returns "throttle", "pause", or nil (nothing to do).
        def next_action(mode, existing)
          current = existing && existing["source"].to_s == GUARD_SOURCE ? existing["action"].to_s : nil
          case mode
          when "throttle"
            current == "throttle" ? nil : "throttle"
          when "pause"
            current == "pause" ? nil : "pause"
          when "throttle_then_pause"
            case current
            when nil        then "throttle"
            when "throttle" then "pause"   # escalate
            else nil                       # already paused
            end
          end
        end

        def apply_action(tid, action, throttle_w:, until_ts:, rate:, threshold:)
          reason = "error_rate_guard: rate #{rate[:rate].round(1)}% >= #{threshold}% over #{rate[:samples]} samples"
          case action
          when "throttle"
            Control.throttle!(tid, weight: throttle_w, reason: reason,
                              source: GUARD_SOURCE, until_ts: until_ts)
          when "pause"
            Control.pause!(tid, reason: reason, source: GUARD_SOURCE, until_ts: until_ts)
          end
        end

        def build_payload(tid, action, mode, rate, threshold, until_ts, dry, at)
          {
            event:     dry ? "dry_run" : "action",
            tenant_id: tid,
            action:    action,
            mode:      mode,
            source:    GUARD_SOURCE,
            rate:      rate[:rate],
            threshold: threshold,
            samples:   rate[:samples],
            ok:        rate[:ok],
            fail:      rate[:fail],
            until:     until_ts,
            dry_run:   dry,
            at:        at.utc.iso8601
          }
        end

        # Fire the host callback + instrument event. Both are best-effort.
        def fire(payload)
          cb = KafkaBatch.config.tenant_guard_callback
          if cb.respond_to?(:call)
            begin
              cb.call(payload)
            rescue StandardError => e
              KafkaBatch.logger.warn("[KafkaBatch][TenantGuard] callback raised: #{e.message}")
            end
          end
          if defined?(KafkaBatch::Instrumentation)
            KafkaBatch::Instrumentation.tenant_guard_action(payload) rescue nil
          end
        end

        def lock_ttl
          [(KafkaBatch.config.tenant_guard_reconcile_interval.to_i * 3), 30].max
        end

        def empty_summary
          { evaluated: 0, acted: 0, dry_run: 0, warned: 0, skipped: 0 }
        end
      end
    end
  end
end
