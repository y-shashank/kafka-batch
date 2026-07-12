require "active_record"
require "time"
require_relative "base"
require_relative "../database_connection"

module KafkaBatch
  module Schedule
    # Named AR model — AR 8.1+ rejects anonymous classes in establish_connection.
    class ScheduledJobRecord < ActiveRecord::Base
      self.table_name         = "kafka_batch_scheduled_jobs"
      self.primary_key        = "job_id"
      self.inheritance_column = nil
    end

    # MySQL backend for the delayed-job index (config.schedule_store = :mysql),
    # detached from the batch ledger. Table kafka_batch_scheduled_jobs:
    #
    #   job_id (PK) | run_at (idx) | partition | offset | batch_id | lease_until (idx) | created_at
    #
    # Disk-resident (cheap at scale) with native, indexed per-job cancel/lookup.
    # Concurrency-safe claiming uses SELECT ... FOR UPDATE SKIP LOCKED (MySQL 8.0+),
    # so many pollers claim disjoint rows without blocking each other.
    #
    # Columns are named partition_id / kafka_offset (not the SQL reserved word
    # `partition`, nor `offset` which collides with the AR pagination method).
    class MysqlStore < Base
      def schedule(job_id:, run_at:, partition:, offset:, batch_id: nil)
        now = Time.now
        attrs = {
          run_at:       to_time(run_at),
          partition_id: partition.to_i,
          kafka_offset: offset.to_i,
          batch_id:     batch_id,
          lease_until:  nil,
          created_at:   now
        }
        rec = model.find_by(job_id: job_id)
        rec ? rec.update!(attrs) : model.create!(attrs.merge(job_id: job_id))
        Member.build(job_id, partition, offset)
      end

      # Bulk schedule: one multi-row INSERT (insert_all) instead of N inserts.
      def schedule_many(entries)
        return [] if entries.empty?

        now  = Time.now
        rows = entries.map do |e|
          {
            job_id:       e[:job_id],
            run_at:       to_time(e[:run_at]),
            partition_id: e[:partition].to_i,
            kafka_offset: e[:offset].to_i,
            batch_id:     e[:batch_id],
            lease_until:  nil,
            created_at:   now
          }
        end
        model.insert_all(rows)
        entries.map { |e| Member.build(e[:job_id], e[:partition], e[:offset]) }
      end

      # Claim due rows (run_at <= now, and either unleased or lease expired) and
      # extend their lease. FOR UPDATE SKIP LOCKED lets concurrent pollers grab
      # disjoint rows. Returns member strings for the uniform poller interface.
      def claim_due(now:, lease_seconds:, limit:)
        now_t       = to_time(now)
        lease_until = now_t + lease_seconds.to_i
        claimed     = []

        model.transaction do
          scope = model
                  .where("run_at <= ?", now_t)
                  .where("lease_until IS NULL OR lease_until <= ?", now_t)
                  .order(:run_at)
                  .limit(limit.to_i)
          scope = scope.lock(lock_clause) if lock_clause
          rows  = scope.to_a

          unless rows.empty?
            model.where(job_id: rows.map(&:job_id)).update_all(lease_until: lease_until)
            claimed = rows.map { |r| Member.build(r.job_id, r.partition_id, r.kafka_offset) }
          end
        end

        claimed
      end

      def ack(members)
        members = Array(members)
        return 0 if members.empty?

        job_ids = members.filter_map { |m| Member.job_id_of(m) }
        return 0 if job_ids.empty?

        model.where(job_id: job_ids).delete_all
      end

      # Rows whose lease has expired become claimable again simply because
      # claim_due's predicate includes `lease_until <= now`. We still clear the
      # stale lease here so reclaim is observable and the index stays tidy.
      def reclaim(now:)
        model.where("lease_until IS NOT NULL AND lease_until <= ?", to_time(now))
             .update_all(lease_until: nil)
      end

      # Native per-job cancel: delete a still-pending row (not yet leased). Returns
      # true if a pending row was removed. If it belonged to a batch, decrement the
      # batch's total_jobs so the batch can still complete without it.
      def cancel(job_id)
        rec = model.find_by(job_id: job_id)
        return false if rec.nil? || rec.lease_until

        batch_id = rec.batch_id
        deleted  = model.where(job_id: job_id, lease_until: nil).delete_all
        return false if deleted.zero?

        KafkaBatch.store.add_jobs(batch_id, -1) if batch_id rescue nil
        true
      end

      def list(limit: 100, offset: 0)
        model.order(:run_at).limit(limit.to_i).offset(offset.to_i).map do |r|
          { job_id: r.job_id, partition: r.partition_id, offset: r.kafka_offset,
            run_at: r.run_at, batch_id: r.batch_id }
        end
      end

      def size
        model.count
      end

      def find(job_id)
        r = model.find_by(job_id: job_id)
        return nil unless r

        { job_id: r.job_id, partition: r.partition_id, offset: r.kafka_offset,
          run_at: r.run_at, batch_id: r.batch_id, state: r.lease_until ? :leased : :pending }
      end

      private

      def to_time(t)
        return t if t.is_a?(Time)
        return Time.at(t) if t.is_a?(Numeric)
        Time.parse(t.to_s)
      end

      # SELECT ... FOR UPDATE SKIP LOCKED lets concurrent pollers claim disjoint
      # rows without blocking (MySQL 8.0+ / PostgreSQL). On adapters that don't
      # support it (e.g. SQLite in tests) we skip the row lock — the surrounding
      # transaction still keeps the claim + lease update atomic.
      def lock_clause
        return @lock_clause if defined?(@lock_clause)

        adapter = model.connection.adapter_name.to_s.downcase
        @lock_clause = %w[mysql mysql2 trilogy postgresql postgis].any? { |a| adapter.include?(a) } ? "FOR UPDATE SKIP LOCKED" : nil
      end

      def model
        @model ||= DatabaseConnection.bind(
          ScheduledJobRecord,
          connection: KafkaBatch.config.schedule_store_database_connection
        )
      end
    end
  end
end
