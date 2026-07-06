require "active_record"
require_relative "base"
require_relative "redis_store"

module KafkaBatch
  module Stores
    # MySQL-backed store for optional relational data. All hot batch-ledger work
    # (counters, completion dedup, reconciler indexes, callback claims) is
    # delegated to Redis to avoid hot-row contention on kafka_batch_records.
    #
    # MySQL holds:
    #   - kafka_batch_failures          (dashboard failure log)
    #   - kafka_batch_consumption_pauses (/lag pause state when Redis is down)
    class MysqlStore < Base
      attr_reader :ledger

      def initialize
        @ledger = RedisStore.new
      end

      # ── Batch ledger (Redis) ───────────────────────────────────────────────

      def create_batch(**kwargs)
        ledger.create_batch(**kwargs)
      end

      def find_batch(id)
        ledger.find_batch(id)
      end

      def add_jobs(id, count)
        ledger.add_jobs(id, count)
      end

      def seal_batch(id)
        ledger.seal_batch(id)
      end

      def record_completions_batch(events)
        ledger.record_completions_batch(events)
      end

      def record_completion_by_offset(**kwargs)
        ledger.record_completion_by_offset(**kwargs)
      end

      def claim_callback(id, dispatched_by = nil)
        ledger.claim_callback(id, dispatched_by)
      end

      def callback_dispatched?(id)
        ledger.callback_dispatched?(id)
      end

      def update_batch_status(id, status)
        ledger.update_batch_status(id, status)
      end

      def batch_status(id)
        ledger.batch_status(id)
      end

      def cancelled_batch_ids
        ledger.cancelled_batch_ids
      end

      def list_batches(**kwargs)
        ledger.list_batches(**kwargs)
      end

      def pending_jobs_total
        ledger.pending_jobs_total
      end

      def batch_counts
        ledger.batch_counts
      end

      def mark_finished(id, outcome)
        ledger.mark_finished(id, outcome)
      end

      def stale_batches(**kwargs)
        ledger.stale_batches(**kwargs)
      end

      def done_batches_without_callback(**kwargs)
        ledger.done_batches_without_callback(**kwargs)
      end

      def delete_batch(id)
        failure_class.where(batch_id: id).delete_all
        ledger.delete_batch(id)
      end

      def with_reconciler_lock(**kwargs, &block)
        ledger.with_reconciler_lock(**kwargs, &block)
      end

      # ── Failures (MySQL) ───────────────────────────────────────────────────

      def record_failure(batch_id:, job_id:, worker_class:, error_class:, error_message:, attempt: 0, status: "failed", next_retry_at: nil)
        attrs = {
          worker_class:  worker_class.to_s,
          error_class:   error_class.to_s,
          error_message: error_message.to_s,
          attempt:       attempt.to_i,
          status:        status,
          next_retry_at: next_retry_at,
          failed_at:     Time.now
        }
        retries = 0
        begin
          rec = failure_class.find_by(batch_id: batch_id, job_id: job_id)
          if rec
            rec.update!(attrs)
          else
            failure_class.create!(attrs.merge(batch_id: batch_id, job_id: job_id))
          end
        rescue ActiveRecord::RecordNotUnique
          retries += 1
          retry if retries < 3
          KafkaBatch.logger.error(
            "[KafkaBatch][MysqlStore] record_failure upsert failed after #{retries} retries " \
            "for batch_id=#{batch_id} job_id=#{job_id}"
          )
        end
      end

      def clear_failure(batch_id, job_id)
        failure_class.where(batch_id: batch_id, job_id: job_id).delete_all
      end

      def list_failures(batch_id, limit: 100, offset: 0)
        failures_scope(failure_class.where(batch_id: batch_id), limit, offset)
      end

      def list_all_failures(limit: 100, offset: 0, status: nil)
        scope = failure_class.all
        scope = scope.where(status: status) if status
        failures_scope(scope, limit, offset, include_batch_id: true)
      end

      # ── Consumption pause/resume (/lag dashboard) ─────────────────────────────

      TOPIC_PAUSE_PARTITION = -1

      def consumption_pauses_enabled?
        consumption_pause_class.table_exists?
      rescue StandardError
        false
      end

      def pause_consumption_topic(group:, topic:)
        consumption_pause_class.create!(
          consumer_group: group,
          topic_name:     topic,
          partition_id:   TOPIC_PAUSE_PARTITION,
          created_at:     Time.now
        )
      rescue ActiveRecord::RecordNotUnique
        nil
      end

      def resume_consumption_topic(group:, topic:)
        consumption_pause_class.where(
          consumer_group: group,
          topic_name:     topic,
          partition_id:   TOPIC_PAUSE_PARTITION
        ).delete_all
      end

      def pause_consumption_partition(group:, topic:, partition:)
        consumption_pause_class.create!(
          consumer_group: group,
          topic_name:     topic,
          partition_id:   partition.to_i,
          created_at:     Time.now
        )
      rescue ActiveRecord::RecordNotUnique
        nil
      end

      def resume_consumption_partition(group:, topic:, partition:)
        consumption_pause_class.where(
          consumer_group: group,
          topic_name:     topic,
          partition_id:   partition.to_i
        ).delete_all
      end

      def consumption_pause_snapshot
        topics     = Set.new
        partitions = Set.new

        consumption_pause_class.find_each do |r|
          if r.partition_id == TOPIC_PAUSE_PARTITION
            topics << KafkaBatch::ConsumptionControl.topic_key(r.consumer_group, r.topic_name)
          else
            partitions << KafkaBatch::ConsumptionControl.partition_key(
              r.consumer_group, r.topic_name, r.partition_id
            )
          end
        end

        { topics: topics, partitions: partitions }
      end

      private

      def failure_class
        @failure_class ||= begin
          klass = Class.new(ActiveRecord::Base)
          klass.table_name        = "kafka_batch_failures"
          klass.inheritance_column = nil
          klass
        end
      end

      def consumption_pause_class
        @consumption_pause_class ||= begin
          klass = Class.new(ActiveRecord::Base)
          klass.table_name        = "kafka_batch_consumption_pauses"
          klass.inheritance_column = nil
          klass
        end
      end

      def failures_scope(scope, limit, offset, include_batch_id: false)
        scope.order(failed_at: :desc).limit(limit).offset(offset).map do |r|
          h = {
            job_id:        r.job_id,
            worker_class:  r.worker_class,
            error_class:   r.error_class,
            error_message: r.error_message,
            attempt:       r.attempt,
            status:        r.status,
            next_retry_at: r.next_retry_at,
            failed_at:     r.failed_at
          }
          h[:batch_id] = r.batch_id if include_batch_id
          h
        end
      end
    end
  end
end
