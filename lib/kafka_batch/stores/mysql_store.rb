require "active_record"
require_relative "base"

module KafkaBatch
  module Stores
    class MysqlStore < Base
      # ── Lazy model accessors ──────────────────────────────────────────────
      # Built on first use so ActiveRecord doesn't need to be booted at
      # require-time (avoids issues in tests and non-Rails environments).

      def batch_record_class
        @batch_record_class ||= begin
          klass = Class.new(ActiveRecord::Base)
          klass.table_name = "kafka_batch_records"
          klass
        end
      end

      def job_completion_class
        @job_completion_class ||= begin
          klass = Class.new(ActiveRecord::Base)
          klass.table_name = "kafka_batch_job_completions"
          klass
        end
      end

      # ── Public interface ──────────────────────────────────────────────────

      def create_batch(id:, total_jobs:, on_success: nil, on_complete: nil, meta: {})
        batch_record_class.create!(
          id:               id,
          total_jobs:       total_jobs,
          completed_count:  0,
          failed_count:     0,
          status:           "running",
          on_success:       on_success,
          on_complete:      on_complete,
          meta:             serialize(meta)
        )
      rescue ActiveRecord::RecordNotUnique
        nil  # idempotent – already created
      end

      def find_batch(id)
        record = batch_record_class.find_by(id: id)
        return nil unless record
        record_to_hash(record)
      end

      def record_job_completion(batch_id:, job_id:, status:)
        # ── Step 1: dedup insert ──────────────────────────────────────────
        begin
          job_completion_class.create!(batch_id: batch_id, job_id: job_id, status: status)
        rescue ActiveRecord::RecordNotUnique
          return { status: :duplicate }
        end

        # ── Step 2: atomic increment + completion check ───────────────────
        # FOR UPDATE: exclusive row lock prevents two processes from both
        # seeing completed+failed >= total and both firing the callback.
        result = nil

        batch_record_class.transaction do
          record = batch_record_class.lock.find_by(id: batch_id)

          return { status: :not_found } unless record
          return { status: :duplicate } if %w[success complete cancelled].include?(record.status)

          field = status == "success" ? "completed_count" : "failed_count"
          batch_record_class.where(id: batch_id).update_all("#{field} = #{field} + 1")
          record.reload

          if record.completed_count + record.failed_count >= record.total_jobs
            outcome = record.failed_count.positive? ? "complete" : "success"
            record.update!(status: outcome, finished_at: Time.now)
            result = { status: :done, outcome: outcome, batch: record_to_hash(record) }
          else
            result = { status: :continue }
          end
        end

        result
      end

      # Atomically claim callback dispatch rights.
      # Uses a conditional UPDATE (WHERE callback_dispatched_at IS NULL) as a
      # compare-and-swap.  Only the process whose UPDATE affected 1 row wins;
      # all others receive false and must skip callback invocation.
      def claim_callback(id)
        rows = batch_record_class
                 .where(id: id, callback_dispatched_at: nil)
                 .update_all(callback_dispatched_at: Time.now)
        rows > 0
      end

      def update_batch_status(id, status)
        batch_record_class.where(id: id).update_all(status: status)
      end

      def stale_batches(older_than:)
        batch_record_class
          .where(status: "running")
          .where("created_at < ?", older_than)
          .map { |r| record_to_hash(r) }
      end

      # Batches that finished but whose callback was never dispatched.
      # Identified by: terminal status + null callback_dispatched_at + finished_at is old.
      def done_batches_without_callback(older_than:)
        batch_record_class
          .where(status: %w[success complete])
          .where(callback_dispatched_at: nil)
          .where("finished_at < ?", older_than)
          .map { |r| record_to_hash(r) }
      end

      def delete_batch(id)
        batch_record_class.transaction do
          job_completion_class.where(batch_id: id).delete_all
          batch_record_class.where(id: id).delete_all
        end
      end

      private

      def record_to_hash(r)
        {
          id:                     r.id,
          total_jobs:             r.total_jobs,
          completed_count:        r.completed_count,
          failed_count:           r.failed_count,
          status:                 r.status,
          on_success:             r.on_success,
          on_complete:            r.on_complete,
          meta:                   deserialize(r.meta),
          created_at:             r.created_at,
          finished_at:            r.respond_to?(:finished_at)            ? r.finished_at            : nil,
          callback_dispatched_at: r.respond_to?(:callback_dispatched_at) ? r.callback_dispatched_at : nil
        }
      end

      def serialize(obj)
        return nil if obj.nil? || (obj.respond_to?(:empty?) && obj.empty?)
        Oj.dump(obj, mode: :compat)
      end

      def deserialize(str)
        return {} if str.nil? || str.empty?
        Oj.load(str)
      rescue Oj::ParseError
        {}
      end
    end
  end
end
