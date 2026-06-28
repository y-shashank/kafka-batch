module KafkaBatch
  module Stores
    # Abstract interface every store must implement.
    # All methods that mutate state must be safe to call concurrently from
    # multiple processes / threads.
    class Base
      # Create a new batch record.
      # @param sealed [Boolean] when true (default) the completion gate is open
      #   immediately, so the batch finalizes as soon as completed+failed reaches
      #   total_jobs. When false the batch is "held" – it accepts jobs but will
      #   NOT finalize until #seal_batch is called (used by the block form to
      #   bracket initial population). Either way an open batch always accepts
      #   more jobs until it completes or is cancelled.
      def create_batch(id:, total_jobs:, on_success: nil, on_complete: nil, meta: {}, description: nil, sealed: true)
        raise NotImplementedError, "#{self.class}#create_batch"
      end

      # Atomically grow a batch's total_jobs by +count+. Only completed or
      # cancelled batches reject the push.
      # @return [Symbol] :ok | :closed | :cancelled | :not_found
      def add_jobs(id, count)
        raise NotImplementedError, "#{self.class}#add_jobs"
      end

      # Seal a held batch (open its completion gate), then evaluate completion.
      # @return [Hash]
      #   { status: :done, outcome: "success"|"complete", batch: <hash> } – just finalized
      #   { status: :sealed }     – sealed, still has outstanding jobs
      #   { status: :not_found }
      def seal_batch(id)
        raise NotImplementedError, "#{self.class}#seal_batch"
      end

      # Apply a batch of completion events in one go (per-poll batching), so a
      # whole Kafka poll's worth of counter updates costs far fewer row locks /
      # round-trips than one transaction per event.
      #
      # MUST be exactly-once per event: each event is deduplicated by the same
      # monotonic per-(source_topic, source_partition) cursor used by
      # #record_completion_by_offset, so re-delivered or re-produced events are
      # never double-counted and none are missed.
      #
      # @param events [Array<Hash>] each: { batch_id:, source_topic:,
      #   source_partition:, source_offset:, status: } in delivery (offset) order
      # @return [Array<Hash>] batches that JUST finalized: { batch:, outcome: }
      def record_completions_batch(events)
        raise NotImplementedError, "#{self.class}#record_completions_batch"
      end

      # Fetch a batch by id.
      # @return [Hash, nil]
      def find_batch(id)
        raise NotImplementedError, "#{self.class}#find_batch"
      end

      # Atomically record that a single job finished, deduplicating by the job
      # message's immutable source coordinates. A completion is applied only if
      # +source_offset+ is strictly greater than the stored monotonic cursor for
      # (source_topic, source_partition); this absorbs both redelivered and
      # re-produced events with O(num_partitions) state regardless of batch size.
      #
      # @return [Hash]
      #   { status: :done,      outcome: "success"|"complete", batch: <hash> }
      #   { status: :continue                                                 }
      #   { status: :duplicate                                                }
      #   { status: :not_found                                                }
      def record_completion_by_offset(batch_id:, source_topic:, source_partition:, source_offset:, status:)
        raise NotImplementedError, "#{self.class}#record_completion_by_offset"
      end

      # Atomically claim the right to dispatch the batch callback.
      # Uses a compare-and-swap / conditional update so that only one consumer
      # process can win the claim, preventing double-invocation of callbacks.
      #
      # @param id [String] batch ID
      # @return [Boolean] true if this caller won the claim, false if already claimed
      # @param dispatched_by [String, nil] identifier of the pod/process that ran
      #   the callbacks, recorded atomically with the claim for tracking.
      def claim_callback(id, dispatched_by = nil)
        raise NotImplementedError, "#{self.class}#claim_callback"
      end

      # Whether the batch's callback has already been dispatched.
      # Used as a cheap pre-invocation duplicate check by the CallbackConsumer.
      # @param id [String] batch ID
      # @return [Boolean]
      def callback_dispatched?(id)
        raise NotImplementedError, "#{self.class}#callback_dispatched?"
      end

      # Update the batch's top-level status field.
      def update_batch_status(id, status)
        raise NotImplementedError, "#{self.class}#update_batch_status"
      end

      # Cheap status read for a single batch.
      # @return [String, nil] the status, or nil if the batch is unknown
      def batch_status(id)
        raise NotImplementedError, "#{self.class}#batch_status"
      end

      # All currently-cancelled batch ids. Fetched periodically (not per-job) by
      # the cancellation cache, so it should be cheap.
      # @return [Array<String>]
      def cancelled_batch_ids
        raise NotImplementedError, "#{self.class}#cancelled_batch_ids"
      end

      # Record that a job failed (always-on failure tracking). Called on EVERY
      # failed attempt – status "retrying" while retries remain, "failed" once
      # exhausted – so problems surface immediately, not hours later. Upserted
      # per (batch_id, job_id); bounded by the number of failing jobs.
      def record_failure(batch_id:, job_id:, worker_class:, error_class:, error_message:, attempt: 0, status: "failed", next_retry_at: nil)
        raise NotImplementedError, "#{self.class}#record_failure"
      end

      # Remove a recorded failure for a job (called when a retry finally
      # succeeds, so it no longer shows as "retrying"). No-op if none exists.
      def clear_failure(batch_id, job_id)
        raise NotImplementedError, "#{self.class}#clear_failure"
      end

      # List recorded failures for a batch, newest-first.
      # @return [Array<Hash>] each: { job_id:, worker_class:, error_class:, error_message:, attempt:, status:, failed_at: }
      def list_failures(batch_id, limit: 100, offset: 0)
        raise NotImplementedError, "#{self.class}#list_failures"
      end

      # List failures across ALL batches, newest-first (for the global view).
      # Each hash additionally includes :batch_id.
      # @return [Array<Hash>]
      def list_all_failures(limit: 100, offset: 0, status: nil)
        raise NotImplementedError, "#{self.class}#list_all_failures"
      end

      # ── Liveness (:store backend) ────────────────────────────────────────────
      # Upsert a consumer process heartbeat (one row per consumer_id). +data+ is
      # a hash of: hostname, pid, topic, current_job_id, current_worker,
      # current_batch_id, current_topic, current_partition, jobs_done.
      def record_heartbeat(consumer_id, data)
        raise NotImplementedError, "#{self.class}#record_heartbeat"
      end

      # Active consumer heartbeats with last_seen >= +since+ (a Time).
      # @return [Array<Hash>]
      def list_heartbeats(since)
        raise NotImplementedError, "#{self.class}#list_heartbeats"
      end

      # Delete heartbeats older than +older_than+ (a Time). Returns count.
      def sweep_stale_heartbeats(older_than)
        raise NotImplementedError, "#{self.class}#sweep_stale_heartbeats"
      end

      # List batches newest-first for the admin UI.
      # @param status [String, nil] optional status filter
      # @param limit  [Integer]
      # @param offset [Integer]
      # @return [Array<Hash>]
      # @param search [String, nil] optional case-insensitive filter matching the
      #   batch id or description.
      def list_batches(status: nil, limit: 50, offset: 0, search: nil)
        raise NotImplementedError, "#{self.class}#list_batches"
      end

      # Total pending jobs across all running batches (sum of
      # total_jobs - completed_count - failed_count). For the dashboard.
      # @return [Integer]
      def pending_jobs_total
        raise NotImplementedError, "#{self.class}#pending_jobs_total"
      end

      # Count of batches grouped by status, for the UI summary.
      # @return [Hash{String=>Integer}]
      def batch_counts
        raise NotImplementedError, "#{self.class}#batch_counts"
      end

      # Transition a batch to a terminal outcome ("success"|"complete"),
      # stamping finished_at and registering it for lost-callback recovery.
      # Used by the reconciler when it discovers a stuck-running batch whose
      # jobs have all actually completed.
      def mark_finished(id, outcome)
        raise NotImplementedError, "#{self.class}#mark_finished"
      end

      # Return batches in "running" state created before +older_than+.
      # Used by the reconciler to detect stuck (never-completed) batches.
      # @return [Array<Hash>]
      def stale_batches(older_than:)
        raise NotImplementedError, "#{self.class}#stale_batches"
      end

      # Return batches in terminal state (success/complete) whose callback was
      # never dispatched (callback_dispatched_at IS NULL) and that finished
      # before +older_than+.  Used by the reconciler to detect lost callbacks.
      # @return [Array<Hash>]
      def done_batches_without_callback(older_than:)
        raise NotImplementedError, "#{self.class}#done_batches_without_callback"
      end

      # Hard-delete a batch and all associated job completion records.
      # Called by Batch#create on a partial Kafka produce failure to prevent
      # the batch from being permanently stuck in "running".
      # @param id [String]
      def delete_batch(id)
        raise NotImplementedError, "#{self.class}#delete_batch"
      end

      # Acquire a distributed lock before running the reconciler body.
      # Yields only if the lock was acquired; silently skips if another process
      # already holds it.  Implementations must release the lock in an ensure block.
      # @param ttl [Integer] lock expiry in seconds
      def with_reconciler_lock(ttl: 300)
        raise NotImplementedError, "#{self.class}#with_reconciler_lock"
      end
    end
  end
end
