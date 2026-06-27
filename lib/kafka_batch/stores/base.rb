module KafkaBatch
  module Stores
    # Abstract interface every store must implement.
    # All methods that mutate state must be safe to call concurrently from
    # multiple processes / threads.
    class Base
      # Create a new batch record.
      # @param locked [Boolean] when false the batch is "open" – it accepts more
      #   jobs (via #add_jobs) and will NOT finalize/fire callbacks until #lock_batch
      #   is called. When true (default) the batch behaves as a fixed-size batch
      #   that finalizes as soon as completed+failed reaches total_jobs.
      def create_batch(id:, total_jobs:, on_success: nil, on_complete: nil, meta: {}, locked: true)
        raise NotImplementedError, "#{self.class}#create_batch"
      end

      # Atomically grow an open batch's total_jobs by +count+.
      # @return [Symbol] :ok | :locked | :cancelled | :not_found
      def add_jobs(id, count)
        raise NotImplementedError, "#{self.class}#add_jobs"
      end

      # Lock an open batch so no more jobs may be added, then evaluate completion.
      # @return [Hash]
      #   { status: :done, outcome: "success"|"complete", batch: <hash> } – just finalized
      #   { status: :locked }     – locked, still has outstanding jobs
      #   { status: :not_found }
      def lock_batch(id)
        raise NotImplementedError, "#{self.class}#lock_batch"
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
      def claim_callback(id)
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

      # List batches newest-first for the admin UI.
      # @param status [String, nil] optional status filter
      # @param limit  [Integer]
      # @param offset [Integer]
      # @return [Array<Hash>]
      def list_batches(status: nil, limit: 50, offset: 0)
        raise NotImplementedError, "#{self.class}#list_batches"
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
