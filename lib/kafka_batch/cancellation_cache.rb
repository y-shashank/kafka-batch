require "set"

module KafkaBatch
  # Process-local cache of cancelled batch ids.
  #
  # Instead of reading the store on every job to check for cancellation, the
  # JobConsumer asks this cache, which refreshes the full set of cancelled batch
  # ids at most once per KafkaBatch.config.cancellation_cache_ttl seconds. This
  # turns "one store read per job" into "one store read per process per window".
  #
  # Consequence: cancellation is eventually-consistent – jobs already queued may
  # still run until the next refresh. That is an accepted trade-off for throughput.
  module CancellationCache
    @mutex    = Mutex.new
    # Bug #8 fix: store ids + timestamp as a single snapshot object so the fast
    # path reads one reference (atomic on all Ruby runtimes including JRuby /
    # TruffleRuby) instead of two separate instance-variable reads that could
    # race on non-MRI runtimes (split-brain: @ids from one epoch, @fetched_at
    # from another).
    @snapshot = nil  # { ids: Set<String>, at: Float } or nil before first load

    class << self
      # @return [Boolean] whether the batch is known-cancelled as of the last refresh
      def cancelled?(batch_id)
        return false if batch_id.nil?
        current_ids.include?(batch_id)
      end

      # Optimistically add a batch_id to the local cache immediately after an
      # explicit cancel (e.g. from the Web UI or Batch.cancel). The cache will
      # also pick it up on the next full store refresh; this just avoids waiting
      # for the TTL window so the UI's cancel takes effect immediately in this
      # process.
      def add(batch_id)
        return if batch_id.nil?
        @mutex.synchronize do
          snap = @snapshot || { ids: Set.new, at: -Float::INFINITY }
          @snapshot = { ids: snap[:ids].dup.add(batch_id.to_s), at: snap[:at] }
        end
      end

      # Drop the cache (tests / after fork).
      def reset!
        @mutex.synchronize { @snapshot = nil }
      end

      private

      def current_ids
        # Fast path: single atomic reference read – safe on all runtimes.
        snap = @snapshot
        return snap[:ids] if snap && fresh?(snap[:at])

        @mutex.synchronize do
          snap = @snapshot
          return snap[:ids] if snap && fresh?(snap[:at])

          ids = fetch_ids
          @snapshot = { ids: ids, at: now }
          ids
        end
      end

      def fetch_ids
        Set.new(KafkaBatch.store.cancelled_batch_ids)
      rescue StandardError => e
        KafkaBatch.logger.warn(
          "[KafkaBatch][CancellationCache] refresh failed: #{e.message} – keeping previous set"
        )
        @snapshot&.dig(:ids) || Set.new
      end

      def fresh?(stamp)
        stamp && (now - stamp) < KafkaBatch.config.cancellation_cache_ttl
      end

      def now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end
