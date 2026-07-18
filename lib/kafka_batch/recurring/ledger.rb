# frozen_string_literal: true

require_relative "reader"

module KafkaBatch
  module Recurring
    # The fire-idempotency ledger and claim/advance transaction — the Ruby twin
    # of the Go pkg/cron Store. Correctness rests on the (schedule_id, fire_at)
    # primary key of kafka_batch_recurring_fires: a second attempt to emit the
    # same instant is an INSERT IGNORE no-op, whether the duplicate comes from a
    # leader flap, a retried tick, or the Go daemon running concurrently.
    module Ledger
      module_function

      ClaimedFire = Struct.new(:schedule_id, :name, :job_type, :tenant_id, :args, :fire_at, keyword_init: true)

      # claim_and_advance selects due schedules FOR UPDATE SKIP LOCKED, inserts a
      # ledger row per planned fire (INSERT IGNORE), advances next_run_at, and
      # returns only the NEWLY-inserted fires this call must enqueue. `planner`
      # is a callable schedule -> { fires:, new_next: }.
      def claim_and_advance(now:, limit:, planner:)
        limit = 100 if limit.to_i <= 0
        conn = model.connection
        claimed = []

        model.transaction do
          scope = model.where(enabled: 1).where("next_run_at <= ?", now.getutc)
                       .order(:next_run_at).limit(limit)
          scope = scope.lock(lock_clause) if lock_clause
          due = scope.to_a
          created = Time.now.utc

          due.each do |sc|
            begin
              plan = planner.call(sc)
            rescue StandardError => e
              # Poison schedule (bad cron/tz): disable so it can't wedge the loop
              # or starve siblings; the heartbeat/logs surface it for ops.
              KafkaBatch.logger.error("[KafkaBatch][Recurring] disabling poison schedule=#{sc.name}: #{e.message}")
              model.where(id: sc.id).update_all(enabled: 0, updated_at: created)
              next
            end

            last_fire = nil
            Array(plan[:fires]).each do |fire_at|
              fire_at = fire_at.getutc
              conn.execute(model.sanitize_sql_array([
                "INSERT IGNORE INTO kafka_batch_recurring_fires (schedule_id, fire_at, status, created_at) " \
                "VALUES (?, ?, 'pending', ?)", sc.id, fire_at, created
              ]))
              last_fire = fire_at
              if conn.select_value("SELECT ROW_COUNT()").to_i == 1
                claimed << ClaimedFire.new(
                  schedule_id: sc.id, name: sc.name, job_type: sc.job_type,
                  tenant_id: sc.tenant_id, args: Reader.parse_args(sc.args_json), fire_at: fire_at
                )
              end
            end

            attrs = { next_run_at: plan[:new_next].getutc, updated_at: created }
            attrs[:last_fire_at] = last_fire if last_fire
            model.where(id: sc.id).update_all(attrs)
          end
        end

        claimed
      end

      def mark_dispatched(schedule_id, fire_at, job_id)
        model.connection.execute(model.sanitize_sql_array([
          "UPDATE kafka_batch_recurring_fires SET status='dispatched', job_id=?, dispatched_at=? " \
          "WHERE schedule_id=? AND fire_at=?", job_id, Time.now.utc, schedule_id, fire_at.getutc
        ]))
      end

      # recover_pending returns fires committed to the ledger but never dispatched
      # and older than `older_than` — the tick crashed between commit and enqueue.
      # Re-enqueueing with the same deterministic job id is idempotent.
      def recover_pending(older_than:, limit:)
        limit = limit.to_i
        limit = 100 if limit <= 0
        # LIMIT takes a validated integer inlined directly — a bind placeholder
        # there is quoted as a string by sanitize_sql_array and rejected by MySQL.
        rows = model.connection.exec_query(model.sanitize_sql_array([
          "SELECT f.schedule_id, f.fire_at, s.name, s.job_type, s.tenant_id, s.args_json " \
          "FROM kafka_batch_recurring_fires f " \
          "JOIN kafka_batch_recurring_schedules s ON s.id = f.schedule_id " \
          "WHERE f.status='pending' AND f.created_at < ? ORDER BY f.created_at LIMIT #{limit}",
          older_than.getutc
        ]))
        rows.map do |r|
          ClaimedFire.new(
            schedule_id: r["schedule_id"], name: r["name"], job_type: r["job_type"],
            tenant_id: r["tenant_id"], args: Reader.parse_args(r["args_json"]),
            fire_at: to_utc(r["fire_at"])
          )
        end
      end

      def prune(older_than:)
        model.connection.exec_delete(model.sanitize_sql_array([
          "DELETE FROM kafka_batch_recurring_fires WHERE status='dispatched' AND dispatched_at < ?",
          older_than.getutc
        ]), "Recurring Prune", [])
      end

      def lock_clause
        return @lock_clause if defined?(@lock_clause)

        adapter = model.connection.adapter_name.to_s.downcase
        @lock_clause = %w[mysql mysql2 trilogy postgresql postgis].any? { |a| adapter.include?(a) } ? "FOR UPDATE SKIP LOCKED" : nil
      end

      def to_utc(v)
        return v.getutc if v.respond_to?(:getutc)

        Time.parse(v.to_s).utc
      end

      def model
        Reader.model
      end
    end
  end
end
