# frozen_string_literal: true

module KafkaBatch
  module Workset
    # Control-plane background loop: re-produce SuperFetch orphans whose
    # consumer heartbeat is gone past orphan grace. Mirrors Go
    # workset.RunReclaimScheduler so Ruby and Go control planes are interchangeable.
    #
    # Safe with multiple replicas (BeginReclaim NX lock). Prefer running on
    # control-tier Karafka processes; execution pods may also run it harmlessly.
    class ReclaimScheduler
      @mutex   = Mutex.new
      @running = false
      @thread  = nil

      class << self
        attr_reader :thread

        def ensure_running!
          return if KafkaBatch.config.daemon_mode?
          return unless KafkaBatch.config.super_fetch_reclaim_enabled
          return unless KafkaBatch.config.redis_configured?

          @mutex.synchronize do
            return if @running && @thread&.alive?

            @running = true
            @thread = Thread.new { new.run }
            @thread.name = "kafka-batch-workset-reclaim" if @thread.respond_to?(:name=)
          end
        end

        def stop!(timeout: 5)
          t = nil
          @mutex.synchronize do
            @running = false
            t = @thread
            @thread = nil
          end
          t&.join(timeout)
          nil
        end

        def running?
          @running && @thread&.alive?
        end

        def reset!
          @mutex.synchronize do
            @running = false
            @thread  = nil
          end
        end
      end

      def initialize(store: nil)
        @store = store
      end

      def running?
        self.class.running?
      end

      def run
        every = KafkaBatch.config.super_fetch_reclaim_interval.to_f
        every = 30.0 if every <= 0
        limit = KafkaBatch.config.super_fetch_reclaim_limit.to_i
        limit = 100 if limit < 1
        grace = KafkaBatch.config.super_fetch_orphan_grace

        KafkaBatch.logger.info(
          "[KafkaBatch][Workset] reclaim scheduler started every=#{every}s " \
          "limit=#{limit} grace=#{grace}s"
        )

        while running?
          sleep(every)
          break unless running?

          begin
            tick(limit: limit, grace: grace)
          rescue StandardError => e
            KafkaBatch.logger.warn(
              "[KafkaBatch][Workset] reclaim sweep error: #{e.class}: #{e.message}"
            )
          end
        end

        KafkaBatch.logger.info("[KafkaBatch][Workset] reclaim scheduler stopped")
      end

      def tick(limit: nil, grace: nil)
        lim = (limit || KafkaBatch.config.super_fetch_reclaim_limit).to_i
        lim = 100 if lim < 1
        g   = grace.nil? ? KafkaBatch.config.super_fetch_orphan_grace : grace

        res = (@store || Workset.store).reclaim_orphans(
          producer: method(:produce),
          limit:    lim,
          lock_ttl: Workset::DEFAULT_RECLAIM_LOCK,
          grace:    g
        )
        if res.reclaimed.positive? || res.failed.positive?
          KafkaBatch.logger.info(
            "[KafkaBatch][Workset] reclaim sweep checked=#{res.checked} " \
            "reclaimed=#{res.reclaimed} failed=#{res.failed} skipped=#{res.skipped}"
          )
        end
        res
      end

      private

      def produce(topic, key, body)
        KafkaBatch::Producer.produce_sync(topic: topic, payload: body, key: key)
      end
    end
  end
end
