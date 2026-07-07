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
      # cancelled batches reject the push. For positive counts also reserves a
      # contiguous run of 1-based batch_seq values.
      # @return [Symbol] :ok | :closed | :cancelled | :not_found
      # @return [Hash] { status: :ok, seq_start:, seq_end: } when count > 0
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
      # MUST be exactly-once per event: each event is deduplicated by batch_seq
      # (bitmap) so re-delivered events are never double-counted and out-of-order
      # completions on the same source partition are all counted.
      #
      # @param events [Array<Hash>] each: { batch_id:, job_id:, batch_seq:,
      #   source_topic:, source_partition:, source_offset:, status: }
      # @return [Hash] { finished: [{ batch:, outcome: }], replays: [batch_id, …] }
      #   :finished — batches that JUST finalized (fire their callback now)
      #   :replays  — batch_ids whose events were deduped on redelivery (candidates
      #               for an inline callback re-fire; empty on first delivery)
      def record_completions_batch(events)
        raise NotImplementedError, "#{self.class}#record_completions_batch"
      end

      # Fetch a batch by id.
      # @return [Hash, nil]
      def find_batch(id)
        raise NotImplementedError, "#{self.class}#find_batch"
      end

      # Atomically record that a single job finished, deduplicating by batch_seq
      # (bitmap). Each job counts at most once regardless of completion order on
      # the source partition. source_* coords are retained for provenance.
      #
      # @param batch_seq [Integer] 1-based slot assigned at enqueue (required).
      #
      # @return [Hash]
      #   { status: :done,      outcome: "success"|"complete", batch: <hash> }
      #   { status: :continue                                                 }
      #   { status: :duplicate                                                }
      #   { status: :not_found                                                }
      #   { status: :invalid   }  – batch_seq missing or <= 0
      def record_completion_by_offset(batch_id:, job_id:, source_topic:, source_partition:, source_offset:, status:, batch_seq:)
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

      # Raw Redis access for UI metadata (reconciler summary, DLT stats cache).
      def with_redis(&block)
        raise NotImplementedError, "#{self.class}#with_redis"
      end
    end
  end
end
