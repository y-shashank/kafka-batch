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

      def consumer_offset_class
        @consumer_offset_class ||= begin
          klass = Class.new(ActiveRecord::Base)
          klass.table_name = "kafka_batch_consumer_offsets"
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

      # Dedup by a monotonic per-partition cursor over the job message's source
      # coordinates. One row per (topic, partition), independent of batch size.
      def record_completion_by_offset(batch_id:, source_topic:, source_partition:, source_offset:, status:)
        offset = source_offset.to_i
        result = nil

        batch_record_class.transaction do
          cursor = consumer_offset_class.lock.find_by(
            source_topic: source_topic, source_partition: source_partition
          )

          if cursor && offset <= cursor.last_offset
            # Already applied (redelivered or re-produced) – skip.
            result = { status: :duplicate }
          else
            if cursor
              cursor.update!(last_offset: offset)
            else
              consumer_offset_class.create!(
                source_topic: source_topic, source_partition: source_partition, last_offset: offset
              )
            end
            result = apply_completion(batch_id, status)
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

      def callback_dispatched?(id)
        batch_record_class
          .where(id: id)
          .where.not(callback_dispatched_at: nil)
          .exists?
      end

      def update_batch_status(id, status)
        batch_record_class.where(id: id).update_all(status: status)
      end

      def mark_finished(id, outcome)
        batch_record_class
          .where(id: id)
          .update_all(status: outcome, finished_at: Time.now)
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
        batch_record_class.where(id: id).delete_all
      end

      # Distributed lock using MySQL advisory locks (GET_LOCK / RELEASE_LOCK).
      # GET_LOCK(name, 0) returns 1 if acquired, 0 if another session holds it.
      # Using timeout=0 so we don't block; the reconciler simply skips if locked.
      def with_reconciler_lock(ttl: 300)
        lock_name = "kafka_batch_reconciler"
        conn      = batch_record_class.connection

        acquired = conn.select_value("SELECT GET_LOCK(#{conn.quote(lock_name)}, 0)").to_i
        return unless acquired == 1

        begin
          yield
        ensure
          conn.execute("SELECT RELEASE_LOCK(#{conn.quote(lock_name)})")
        end
      rescue => e
        KafkaBatch.logger.error("[KafkaBatch][MysqlStore] Reconciler lock error: #{e.message}")
      end

      private

      # Increment the batch counter and detect completion. MUST be called from
      # within a transaction. FOR UPDATE locks the row so two processes can't
      # both observe completion and both fire the callback. Returning from this
      # helper is a normal method return (not a non-local return out of the
      # transaction block), so it does not roll the transaction back.
      def apply_completion(batch_id, status)
        record = batch_record_class.lock.find_by(id: batch_id)
        return { status: :not_found } if record.nil?
        return { status: :duplicate } if %w[success complete cancelled].include?(record.status)

        field = status == "success" ? "completed_count" : "failed_count"
        batch_record_class.where(id: batch_id).update_all("#{field} = #{field} + 1")
        record.reload

        if record.completed_count + record.failed_count >= record.total_jobs
          outcome = record.failed_count.positive? ? "complete" : "success"
          record.update!(status: outcome, finished_at: Time.now)
          { status: :done, outcome: outcome, batch: record_to_hash(record) }
        else
          { status: :continue }
        end
      end

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
