# frozen_string_literal: true

require "time"
require_relative "state"
require_relative "control"

module KafkaBatch
  module TenantGuard
    # Keeps the guard's intent records in sync with the authoritative enforcement
    # state, on a background tick (control plane only, NX-locked so Ruby and Go
    # never both act). Each tick, for every tenant with an active control:
    #
    #   1. Auto-release  — `until` has passed  → revert + close action "expired".
    #   2. External drift — operator resumed the partition / changed the weight
    #      outside the guard → close the record WITHOUT re-applying, honoring the
    #      operator's action ("released_externally").
    #
    # This is what implements the configurable "auto-disengage after X minutes".
    module Reconciler
      DEFAULT_INTERVAL = 15

      class << self
        # One reconciliation pass. Returns a summary hash. Safe to call directly
        # (tests) or from the loop. NX-locked so only one control plane acts.
        def reconcile_once!(at: Time.now)
          return empty_summary unless State.available?
          return empty_summary unless State.try_lock!(ttl: lock_ttl)

          summary = empty_summary
          begin
            State.index_members.each do |tid|
              record = State.get_record(tid)
              next if record.nil?

              record["tenant_id"] = tid
              summary[:checked] += 1
              outcome = reconcile_record(record, at: at)
              summary[outcome] += 1 if outcome
            rescue StandardError => e
              KafkaBatch.logger.warn("[KafkaBatch][TenantGuard::Reconciler] tenant=#{tid}: #{e.message}")
            end
          ensure
            State.unlock!
          end
          summary
        end

        def start!(interval: nil)
          return @thread if @thread&.alive?

          secs = (interval || KafkaBatch.config.tenant_guard_reconcile_interval || DEFAULT_INTERVAL).to_i
          secs = DEFAULT_INTERVAL if secs <= 0

          @thread = Thread.new do
            Thread.current.name = "kb-tenant-guard-reconciler" if Thread.current.respond_to?(:name=)
            loop do
              begin
                reconcile_once!
              rescue StandardError => e
                KafkaBatch.logger.warn("[KafkaBatch][TenantGuard::Reconciler] tick failed: #{e.message}")
              end
              sleep(secs)
            end
          end
        end

        def stop!
          @thread&.kill
          @thread = nil
        end

        private

        # @return [Symbol, nil] the summary bucket the action fell into
        def reconcile_record(record, at:)
          # 1. Auto-release on expiry.
          if expired?(record, at)
            Control.revert_enforcement(record)
            close_and_delete(record, outcome: "expired", at: at)
            return :expired
          end

          # 2. Operator-initiated drift → release the record, do not re-apply.
          if drifted?(record)
            # Enforcement already reflects the operator's intent; only close the
            # record + action so the dashboard stops showing a stale control.
            close_and_delete(record, outcome: "released_externally", at: at)
            return :drift_released
          end

          nil
        end

        def expired?(record, at)
          u = record["until"].to_s
          return false if u.empty?

          u.to_i <= at.to_i
        end

        # True when the enforcement no longer matches what the guard applied,
        # i.e. an operator resumed the partition or changed the weight directly.
        def drifted?(record)
          case record["state"]
          when Control::STATE_PAUSED
            !partition_still_paused?(record)
          when Control::STATE_THROTTLED
            !weight_still_applied?(record)
          else
            false
          end
        end

        def partition_still_paused?(record)
          return true unless consumption_available?

          snap = KafkaBatch::ConsumptionControl.snapshot(refresh: true)
          KafkaBatch::ConsumptionControl.partition_paused?(
            snap, record["group"].to_s, record["topic"].to_s, record["partition"].to_i
          )
        rescue StandardError
          # If we cannot read the pause state, assume unchanged (do not release).
          true
        end

        def weight_still_applied?(record)
          sched = KafkaBatch.scheduler(lane_sym(record["lane"]))
          return true unless sched

          current = sched.weight_override(record["tenant_id"].to_s)
          effective = record["effective_weight"].to_s
          return true if effective.empty?

          # Still ours iff the current override equals the weight we applied.
          !current.nil? && (current - effective.to_f).abs < 1e-9
        rescue StandardError
          true
        end

        def close_and_delete(record, outcome:, at:)
          action_id = record["action_id"]
          if action_id && !action_id.to_s.empty?
            State.update_action(action_id, {
              "outcome"     => outcome,
              "released_at" => at.to_i,
              "released_by" => "reconciler"
            })
          end
          State.delete_record(record["tenant_id"])
        end

        def consumption_available?
          defined?(KafkaBatch::ConsumptionControl) && KafkaBatch::ConsumptionControl.available?
        end

        def lane_sym(lane)
          l = lane.to_s
          %w[time throughput].include?(l) ? l.to_sym : :time
        end

        def lock_ttl
          [(KafkaBatch.config.tenant_guard_reconcile_interval.to_i * 3), 30].max
        end

        def empty_summary
          { checked: 0, expired: 0, drift_released: 0 }
        end
      end
    end
  end
end
