# frozen_string_literal: true

require_relative "reader"

module KafkaBatch
  module Recurring
    # Pure misfire logic — mirrors the Go pkg/cron PlanFires so both runtimes
    # compute identical fire instants and next_run_at for a given schedule.
    #
    # Returns { fires: [Time(UTC)...], new_next: Time(UTC) }.
    #   - grace:        an instant within this of now counts as "on time" and
    #                   always fires regardless of policy; older ones are "missed".
    #   - max_backfill: caps fires emitted per tick (backfill); the remainder
    #                   drains on later ticks (deduped by the fire ledger).
    module Planner
      module_function

      SAFETY_ITERS = 1_000_000

      def plan(schedule, now:, grace:, max_backfill:)
        max_backfill = 1 if max_backfill.to_i < 1
        now = now.getutc
        next_run = schedule.next_run_at.getutc

        nexter = Reader.cron_parser(schedule.cron_expr, schedule.timezone)
        raise ArgumentError, "invalid cron #{schedule.cron_expr.inspect}" unless nexter

        case schedule.misfire_policy.to_s
        when "backfill"
          fires = []
          cur = next_run
          while cur <= now
            fires << cur
            nxt = nexter.call(cur)
            return { fires: fires, new_next: (nxt || (cur + 60)).getutc } if nxt.nil?

            cur = nxt
            break if fires.size >= max_backfill
          end
          { fires: fires, new_next: cur.getutc }

        when "skip"
          fires = (now - next_run) <= grace ? [next_run] : []
          { fires: fires, new_next: advance_past(nexter, next_run, now) }

        else # fire_once (and any unknown value → safe default)
          { fires: [next_run], new_next: advance_past(nexter, next_run, now) }
        end
      end

      # advance_past returns the first instant strictly after now, walking forward
      # from `from`. Guards against a non-advancing expression.
      def advance_past(nexter, from, now)
        cur = from
        SAFETY_ITERS.times do
          nxt = nexter.call(cur)
          return (cur + 60).getutc if nxt.nil?

          cur = nxt
          return cur.getutc if cur > now
        end
        (now + 60).getutc
      end
    end
  end
end
