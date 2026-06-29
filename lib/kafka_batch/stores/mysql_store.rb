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

      def failure_class
        @failure_class ||= begin
          klass = Class.new(ActiveRecord::Base)
          klass.table_name = "kafka_batch_failures"
          klass
        end
      end

      def heartbeat_class
        @heartbeat_class ||= begin
          klass = Class.new(ActiveRecord::Base)
          klass.table_name = "kafka_batch_consumer_heartbeats"
          klass.primary_key = "consumer_id"
          klass
        end
      end

      def consumption_pause_class
        @consumption_pause_class ||= begin
          klass = Class.new(ActiveRecord::Base)
          klass.table_name = "kafka_batch_consumption_pauses"
          klass
        end
      end

      # ── Public interface ──────────────────────────────────────────────────

      def create_batch(id:, total_jobs:, on_success: nil, on_complete: nil, meta: {}, description: nil, sealed: true)
        attrs = {
          id:               id,
          total_jobs:       total_jobs,
          completed_count:  0,
          failed_count:     0,
          status:           "running",
          on_success:       on_success,
          on_complete:      on_complete,
          meta:             serialize(meta),
          locked_at:        sealed ? Time.now : nil  # locked_at == "sealed_at"
        }
        # Tolerate apps that haven't run the description migration yet.
        attrs[:description] = description if batch_record_class.column_names.include?("description")
        batch_record_class.create!(attrs)
      rescue ActiveRecord::RecordNotUnique
        nil  # idempotent – already created
      end

      def add_jobs(id, count)
        result = nil
        batch_record_class.transaction do
          record = batch_record_class.lock.find_by(id: id)
          if record.nil?
            result = :not_found
          elsif record.status == "cancelled"
            result = :cancelled
          elsif %w[success complete].include?(record.status) || record.callback_dispatched_at
            # Completed/closed – cannot accept more jobs. An open batch (even one
            # that is sealed and currently running jobs) always accepts more.
            result = :closed
          else
            batch_record_class.where(id: id).update_all("total_jobs = total_jobs + #{count.to_i}")
            result = :ok
          end
        end
        result
      end

      def seal_batch(id)
        result = nil
        batch_record_class.transaction do
          record = batch_record_class.lock.find_by(id: id)
          if record.nil?
            result = :not_found
          else
            was_running = record.status == "running"
            record.update!(locked_at: Time.now) if record.locked_at.nil?

            if was_running && record.completed_count + record.failed_count >= record.total_jobs
              outcome = record.failed_count.positive? ? "complete" : "success"
              record.update!(status: outcome, finished_at: Time.now)
              result = { status: :done, outcome: outcome, batch: record_to_hash(record) }
            else
              result = { status: :sealed }
            end
          end
        end
        result.is_a?(Hash) ? result : { status: result }
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

      # Batched counter application for a whole Kafka poll. One transaction:
      #   1. dedup per source partition + advance each cursor once (to its max
      #      new offset) — re-delivered offsets are skipped, none are missed;
      #   2. aggregate +N per (batch, status);
      #   3. apply one UPDATE per batch (rows locked in sorted id order to avoid
      #      deadlocks) and finalize any batch that reached its total.
      # @return [Array<Hash>] { batch:, outcome: } for batches that just finished
      def record_completions_batch(events)
        return [] if events.empty?

        finalized = []
        batch_record_class.transaction do
          # ── Phase 1: dedup + advance cursors (sorted for stable lock order) ──
          applicable = []
          events.group_by { |e| [e[:source_topic], e[:source_partition]] }
                .sort_by { |(t, p), _| [t.to_s, p.to_i] }
                .each do |(topic, partition), evs|
            cursor = consumer_offset_class.lock.find_by(source_topic: topic, source_partition: partition)
            last   = cursor ? cursor.last_offset : -1
            maxoff = last
            seen   = {}
            evs.each do |e|
              off    = e[:source_offset].to_i
              maxoff = off if off > maxoff
              next if off <= last || seen[off]  # duplicate / already applied
              seen[off] = true
              applicable << e
            end
            if maxoff > last
              cursor ? cursor.update!(last_offset: maxoff)
                     : consumer_offset_class.create!(source_topic: topic, source_partition: partition, last_offset: maxoff)
            end
          end

          # ── Phase 2: aggregate increments per batch ─────────────────────────
          incr = Hash.new { |h, k| h[k] = [0, 0] }  # batch_id => [completed, failed]
          applicable.each do |e|
            idx = e[:status] == "success" ? 0 : 1
            incr[e[:batch_id]][idx] += 1
          end

          # ── Phase 3: apply per batch + finalize (sorted id => no deadlocks) ──
          incr.keys.sort.each do |batch_id|
            done, failed = incr[batch_id]
            record = batch_record_class.lock.find_by(id: batch_id)
            next if record.nil?
            next if %w[success complete cancelled].include?(record.status)

            sets = []
            sets << "completed_count = completed_count + #{done.to_i}" if done.positive?
            sets << "failed_count = failed_count + #{failed.to_i}"     if failed.positive?
            batch_record_class.where(id: batch_id).update_all(sets.join(", ")) unless sets.empty?
            record.reload

            if record.locked_at && record.completed_count + record.failed_count >= record.total_jobs
              outcome = record.failed_count.positive? ? "complete" : "success"
              record.update!(status: outcome, finished_at: Time.now)
              finalized << { batch: record_to_hash(record), outcome: outcome }
            end
          end
        end
        finalized
      end

      # Atomically claim callback dispatch rights.
      # Uses a conditional UPDATE (WHERE callback_dispatched_at IS NULL) as a
      # compare-and-swap.  Only the process whose UPDATE affected 1 row wins;
      # all others receive false and must skip callback invocation.
      def claim_callback(id, dispatched_by = nil)
        attrs = { callback_dispatched_at: Time.now }
        if dispatched_by && batch_record_class.column_names.include?("callback_dispatched_by")
          attrs[:callback_dispatched_by] = dispatched_by
        end
        rows = batch_record_class
                 .where(id: id, callback_dispatched_at: nil)
                 .update_all(attrs)
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

      def batch_status(id)
        batch_record_class.where(id: id).limit(1).pluck(:status).first
      end

      # Uses the existing index on :status.
      def cancelled_batch_ids
        batch_record_class.where(status: "cancelled").pluck(:id)
      end

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
        # Upsert per (batch_id, job_id): reflect the latest failed attempt.
        rec = failure_class.find_by(batch_id: batch_id, job_id: job_id)
        if rec
          rec.update!(attrs)
        else
          failure_class.create!(attrs.merge(batch_id: batch_id, job_id: job_id))
        end
      rescue ActiveRecord::RecordNotUnique
        retry  # lost a create race – the row now exists, update it
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

      # ── Liveness (:store backend) ────────────────────────────────────────────

      def record_heartbeat(consumer_id, data)
        attrs = data.slice(
          :hostname, :pid, :topic, :current_job_id, :current_worker,
          :current_batch_id, :current_topic, :current_partition, :jobs_done
        ).merge(last_seen: Time.now)

        rec = heartbeat_class.find_by(consumer_id: consumer_id)
        if rec
          rec.update!(attrs)
        else
          heartbeat_class.create!(attrs.merge(consumer_id: consumer_id))
        end
      rescue ActiveRecord::RecordNotUnique
        retry
      end

      def list_heartbeats(since)
        heartbeat_class.where("last_seen >= ?", since).order(last_seen: :desc).map { |r| heartbeat_to_hash(r) }
      end

      def sweep_stale_heartbeats(older_than)
        heartbeat_class.where("last_seen < ?", older_than).delete_all
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

      def list_batches(status: nil, limit: 50, offset: 0, search: nil)
        scope = batch_record_class.order(created_at: :desc)
        scope = scope.where(status: status) if status
        if (q = presence(search))
          like = "%#{sanitize_like(q)}%"
          if batch_record_class.column_names.include?("description")
            scope = scope.where("id LIKE :q OR description LIKE :q", q: like)
          else
            scope = scope.where("id LIKE :q", q: like)
          end
        end
        scope.limit(limit).offset(offset).map { |r| record_to_hash(r) }
      end

      def batch_counts
        batch_record_class.group(:status).count
      end

      def mark_finished(id, outcome)
        batch_record_class
          .where(id: id)
          .update_all(status: outcome, finished_at: Time.now)
      end

      def pending_jobs_total
        sum = batch_record_class.where(status: "running")
                                .sum("total_jobs - completed_count - failed_count")
        [sum.to_i, 0].max
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
          failure_class.where(batch_id: id).delete_all
          batch_record_class.where(id: id).delete_all
        end
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

      def heartbeat_to_hash(r)
        {
          consumer_id:       r.consumer_id,
          hostname:          r.hostname,
          pid:               r.pid,
          topic:             r.topic,
          current_job_id:    r.current_job_id,
          current_worker:    r.current_worker,
          current_batch_id:  r.current_batch_id,
          current_topic:     r.current_topic,
          current_partition: r.current_partition,
          jobs_done:         r.jobs_done,
          last_seen:         r.last_seen
        }
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

        # Only finalize (and fire callbacks) once the batch is sealed – a held
        # (block-form, not-yet-sealed) batch may still receive more jobs even if
        # currently at its total. locked_at doubles as the "sealed_at" marker.
        if record.locked_at && record.completed_count + record.failed_count >= record.total_jobs
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
          description:            (r.respond_to?(:description) ? r.description : nil),
          meta:                   deserialize(r.meta),
          created_at:             r.created_at,
          finished_at:            r.respond_to?(:finished_at)            ? r.finished_at            : nil,
          callback_dispatched_at: r.respond_to?(:callback_dispatched_at) ? r.callback_dispatched_at : nil,
          callback_dispatched_by: (r.respond_to?(:callback_dispatched_by) ? r.callback_dispatched_by : nil),
          locked_at:              r.respond_to?(:locked_at)              ? r.locked_at              : nil
        }
      end

      def presence(value)
        return nil if value.nil?
        s = value.to_s
        s.empty? ? nil : s
      end

      # Escape LIKE wildcards so user input is matched literally.
      def sanitize_like(str)
        str.to_s.gsub(/[\\%_]/) { |c| "\\#{c}" }
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
