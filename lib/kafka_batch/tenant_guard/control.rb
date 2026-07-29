# frozen_string_literal: true

require "time"
require_relative "state"

module KafkaBatch
  module TenantGuard
    # The control service: pause / throttle / reset a tenant on a fairness lane,
    # reusing the EXISTING levers (ConsumptionControl partition pause + the
    # fairness weight hash). It never touches the job/batch execution path, so
    # batch counting and completion callbacks are unaffected.
    #
    #   pause    → pause the tenant's dedicated ingest partition (stops new
    #              admission; in-flight + already-admitted work drains normally)
    #   throttle → lower the tenant's fairness weight (dispatch share), saving
    #              the prior value so reset can restore it exactly
    #   reset    → revert enforcement and close the action in the audit log
    #
    # Every operation writes a record + an audit action so the dashboard and the
    # reconciler can see intent, provenance (manual vs guard), and expiry.
    module Control
      class Error < StandardError; end
      class Unavailable < Error; end
      class Unresolvable < Error; end

      STATE_ACTIVE    = "active"
      STATE_PAUSED    = "paused"
      STATE_THROTTLED = "throttled"

      # API ops take the shared lock so they serialize against a control-plane
      # pass (mitigation/reconciler) on the same tenant — preventing lost updates
      # and orphaned enforcement. Callers already holding the lock (mitigation)
      # pass lock: false.
      API_LOCK_TTL  = 15
      API_LOCK_WAIT = 3.0

      class << self
        # Pause the tenant's ingest partition. Returns the new record hash.
        def pause!(tenant_id, lane: :time, reason: nil, source: "api", until_ts: nil, created_by: nil, at: Time.now, lock: true)
          tid = normalize_tenant(tenant_id)
          lane = normalize_lane(lane)
          raise Unavailable, "ConsumptionControl unavailable" unless consumption_available?

          with_optional_lock(lock) do
            group, topic, partition = resolve_partition!(tid, lane)

            supersede_existing!(tid, at: at)
            KafkaBatch::ConsumptionControl.pause_partition(group: group, topic: topic, partition: partition)

            persist!(tid, {
              "state"      => STATE_PAUSED,
              "lane"       => lane.to_s,
              "action"     => "pause",
              "source"     => source.to_s,
              "reason"     => reason.to_s,
              "group"      => group,
              "topic"      => topic,
              "partition"  => partition,
              "created_at" => at.to_i,
              "until"      => until_ts.nil? ? "" : until_ts.to_i,
              "created_by" => created_by.to_s
            }, at: at)
          end
        end

        # Throttle the tenant's fairness weight. Returns the new record hash.
        def throttle!(tenant_id, lane: :time, weight: nil, reason: nil, source: "api", until_ts: nil, created_by: nil, at: Time.now, lock: true)
          tid = normalize_tenant(tenant_id)
          lane = normalize_lane(lane)
          sched = scheduler_for(lane)
          raise Unavailable, "fairness scheduler unavailable for lane #{lane}" unless sched

          w = (weight || KafkaBatch.config.tenant_guard_throttle_weight).to_f
          raise Error, "throttle weight must be positive (got #{w})" unless w.positive?

          with_optional_lock(lock) do
            # Revert any existing control FIRST so weight_override now reflects the
            # true pre-guard weight (not a prior throttle's applied value) — this
            # is what reset restores.
            supersede_existing!(tid, at: at)
            original = sched.weight_override(tid) # nil = no prior override (default)
            sched.set_weight(tid, w)

            persist!(tid, {
              "state"            => STATE_THROTTLED,
              "lane"             => lane.to_s,
              "action"           => "throttle",
              "source"           => source.to_s,
              "reason"           => reason.to_s,
              "original_weight"  => original.nil? ? "" : original,
              "effective_weight" => w,
              "created_at"       => at.to_i,
              "until"            => until_ts.nil? ? "" : until_ts.to_i,
              "created_by"       => created_by.to_s
            }, at: at)
          end
        end

        # Revert a tenant's active control (resume partition / restore weight) and
        # close its audit action. No-op when the tenant has no active control.
        # `outcome` records WHY it was released for the audit log.
        def reset!(tenant_id, source: "api", released_by: nil, outcome: "manual_reset", at: Time.now, lock: true)
          tid = normalize_tenant(tenant_id)
          with_optional_lock(lock) do
            record = State.get_record(tid)
            if record.nil?
              nil
            else
              record["tenant_id"] = tid
              revert_enforcement(record)
              close_action(record["action_id"], outcome: outcome, released_by: released_by, at: at)
              State.delete_record(tid)
              record
            end
          end
        end

        # Reset every tenant with an active control. Returns the count reset.
        # Acquires the lock ONCE for the sweep; per-tenant resets skip re-locking.
        def reset_all!(source: "api", released_by: nil, at: Time.now)
          res = State.with_lock(ttl: API_LOCK_TTL, wait: API_LOCK_WAIT) do
            State.index_members.count do |tid|
              reset!(tid, source: source, released_by: released_by, outcome: "manual_reset", at: at, lock: false) ? true : false
            end
          end
          raise Error, "tenant guard busy (control-plane pass in progress), retry" if res == :busy

          res
        end

        def status(tenant_id)
          State.get_record(normalize_tenant(tenant_id))
        end

        # { active: [record, ...], actions: [action, ...] } for the dashboard.
        def list(action_limit: 100)
          active = State.index_members.filter_map { |tid| State.get_record(tid)&.merge("tenant_id" => tid) }
          { active: active, actions: State.recent_actions(limit: action_limit) }
        end

        # ── internals ───────────────────────────────────────────────────────

        # Revert whatever enforcement a record represents. Safe to call even if
        # the operator already reverted it externally (resume/HDEL are no-ops).
        def revert_enforcement(record)
          case record["state"]
          when STATE_PAUSED
            resume_partition(record)
          when STATE_THROTTLED
            restore_weight(record)
          end
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch][TenantGuard] revert failed for #{record['tenant_id']}: #{e.message}")
        end

        private

        # Serialize an API control op against the control-plane pass on the same
        # shared lock. When the caller already holds it (mitigation), run inline.
        def with_optional_lock(lock)
          return yield unless lock

          res = State.with_lock(ttl: API_LOCK_TTL, wait: API_LOCK_WAIT) { yield }
          raise Error, "tenant guard busy (control-plane pass in progress), retry" if res == :busy

          res
        end

        def persist!(tid, fields, at:)
          action_id = State.append_action(fields.merge("tenant_id" => tid, "outcome" => "active"), at: at)
          State.put_record(tid, fields.merge("action_id" => action_id))
          State.get_record(tid)&.merge("tenant_id" => tid)
        end

        # If the tenant already has an active control, revert it and close its
        # action as superseded before applying the new one.
        def supersede_existing!(tid, at:)
          existing = State.get_record(tid)
          return if existing.nil?

          existing["tenant_id"] = tid
          revert_enforcement(existing)
          close_action(existing["action_id"], outcome: "superseded", at: at)
        end

        def close_action(action_id, outcome:, released_by: nil, at: Time.now)
          return if action_id.nil? || action_id.to_s.empty?

          State.update_action(action_id, {
            "outcome"     => outcome,
            "released_at" => at.to_i,
            "released_by" => released_by.to_s
          })
        end

        def resume_partition(record)
          group = record["group"].to_s
          topic = record["topic"].to_s
          partition = record["partition"]
          return if group.empty? || topic.empty? || partition.nil? || partition.to_s.empty?

          KafkaBatch::ConsumptionControl.resume_partition(
            group: group, topic: topic, partition: partition.to_i
          )
        end

        def restore_weight(record)
          tid = record["tenant_id"].to_s
          return if tid.empty?

          lane = normalize_lane(record["lane"])
          sched = scheduler_for(lane)
          return unless sched

          orig = record["original_weight"].to_s
          if orig.empty?
            # No prior override — remove ours so the tenant returns to default.
            sched.delete_weight(tid)
          else
            sched.set_weight(tid, orig.to_f)
          end
        end

        def resolve_partition!(tid, lane)
          group = KafkaBatch.dispatch_consumer_group(lane)
          topic = KafkaBatch.config.fairness_ingest_topic(lane)
          partition = KafkaBatch.tenant_ingest_partition(tid, lane)
          if partition.nil? || partition.to_i.negative?
            raise Unresolvable,
                  "no ingest partition assigned for tenant=#{tid} lane=#{lane} " \
                  "(has it produced any fair jobs, or is the partition pool exhausted?)"
          end

          [group, topic, partition.to_i]
        end

        def scheduler_for(lane)
          KafkaBatch.scheduler(lane)
        rescue StandardError
          nil
        end

        def consumption_available?
          defined?(KafkaBatch::ConsumptionControl) && KafkaBatch::ConsumptionControl.available?
        end

        def normalize_tenant(tenant_id)
          tid = tenant_id.to_s
          raise Error, "tenant_id required" if tid.empty?

          tid
        end

        def normalize_lane(lane)
          l = lane.to_s.strip
          l = "time" if l.empty?
          raise Error, "unknown fairness lane: #{lane}" unless %w[time throughput].include?(l)

          l.to_sym
        end
      end
    end
  end
end
