# frozen_string_literal: true

require_relative "reader"
require_relative "ledger"
require_relative "planner"
require_relative "lock"

module KafkaBatch
  module Recurring
    # The Ruby recurring scheduler — the runtime twin of the Go pkg/cron Ticker.
    # Once per window, on the elected leader, it claims due schedules, enqueues
    # their jobs via Batch.enqueue_job (same routing as immediate enqueues),
    # advances next_run_at, and periodically recovers/prunes and emits a
    # stale-schedule heartbeat.
    #
    # Runs as a singleton thread per process (like SchedulePoller). Enable on the
    # pods that should run it via config.recurring_scheduler_enabled. Safe to run
    # alongside the Go daemon's ticker: they share the leader lock and the fire
    # ledger, so at most one fires each instant.
    class Ticker
      @mutex   = Mutex.new
      @running = false
      @thread  = nil

      class << self
        attr_reader :thread

        def ensure_running!
          return unless KafkaBatch.config.recurring_scheduler_enabled

          @mutex.synchronize do
            return if @running && @thread&.alive?

            @running = true
            @thread  = Thread.new { new.run }
            @thread.name = "kafka-batch-recurring" if @thread.respond_to?(:name=)
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

      def initialize(lock: nil)
        cfg = KafkaBatch.config
        @window       = positive(cfg.recurring_window, 30.0)
        @batch_size   = positive_int(cfg.recurring_batch_size, 100)
        @grace        = positive(cfg.recurring_misfire_grace, 60.0)
        @max_backfill = positive_int(cfg.recurring_max_backfill, 1000)
        @recover_every   = positive(cfg.recurring_recover_every, 300.0)
        @recover_grace   = positive(cfg.recurring_recover_grace, 120.0)
        @prune_every     = positive(cfg.recurring_prune_every, 3600.0)
        @prune_retention = positive(cfg.recurring_prune_retention, 7 * 24 * 3600.0)
        @heartbeat_every = positive(cfg.recurring_heartbeat_every, 60.0)
        @stale_factor    = positive(cfg.recurring_stale_factor, 2.0)
        @lock = lock || Lock.new(ttl_seconds: positive_int(cfg.recurring_lock_ttl, 60))
        @last_recover = 0.0
        @last_prune = 0.0
        @last_heartbeat = 0.0
      end

      def running?
        self.class.running?
      end

      def run
        KafkaBatch.logger.info(
          "[KafkaBatch][Recurring] started window=#{@window}s batch=#{@batch_size} grace=#{@grace}s"
        )
        tick while running? && sleep_window
        KafkaBatch.logger.info("[KafkaBatch][Recurring] stopped")
      end

      # tick runs one leader-gated pass. Public for tests.
      def tick
        token = @lock.acquire
        return :not_leader unless token

        begin
          now = Time.now.utc
          dispatch_due(now)
          maybe_periodic(now)
        rescue StandardError => e
          KafkaBatch.logger.error("[KafkaBatch][Recurring] tick error: #{e.class}: #{e.message}")
        ensure
          @lock.release(token)
        end
        :ok
      end

      # deterministic job id for a (schedule, instant) pair — identical to Go's
      # JobIDForFire so a Go/Ruby recovery re-enqueue dedups via uniq handlers.
      def self.job_id_for(schedule_id, fire_at)
        "sched-#{schedule_id}-#{fire_at.getutc.to_i}"
      end

      private

      def dispatch_due(now)
        planner = ->(sc) { Planner.plan(sc, now: now, grace: @grace, max_backfill: @max_backfill) }
        Ledger.claim_and_advance(now: now, limit: @batch_size, planner: planner).each { |cf| enqueue(cf) }
      end

      def maybe_periodic(now)
        mono = monotonic
        if mono - @last_recover >= @recover_every
          @last_recover = mono
          recover(now)
        end
        if mono - @last_prune >= @prune_every
          @last_prune = mono
          pruned = Ledger.prune(older_than: now - @prune_retention)
          KafkaBatch.logger.info("[KafkaBatch][Recurring] pruned #{pruned} dispatched fire rows") if pruned.to_i.positive?
        end
        if mono - @last_heartbeat >= @heartbeat_every
          @last_heartbeat = mono
          heartbeat(now)
        end
      end

      def recover(now)
        pending = Ledger.recover_pending(older_than: now - @recover_grace, limit: @batch_size)
        KafkaBatch.logger.info("[KafkaBatch][Recurring] recovering #{pending.size} pending fires") unless pending.empty?
        pending.each { |cf| enqueue(cf) }
      end

      # enqueue pushes one fire and, on success, marks it dispatched. On failure
      # it stays 'pending' for the recovery sweep. enqueue_job returns nil on a
      # uniq-duplicate, which we treat as already-dispatched.
      def enqueue(cf)
        job_id = self.class.job_id_for(cf.schedule_id, cf.fire_at)
        begin
          KafkaBatch::Batch.enqueue_job(
            cf.job_type, cf.args, job_id: job_id,
            tenant_id: (cf.tenant_id.to_s.empty? ? nil : cf.tenant_id)
          )
        rescue StandardError => e
          KafkaBatch.logger.error(
            "[KafkaBatch][Recurring] enqueue schedule=#{cf.name} job_type=#{cf.job_type} " \
            "fire_at=#{cf.fire_at.iso8601}: #{e.message} — left pending for recovery"
          )
          KafkaBatch::Instrumentation.cron_enqueue_failed(schedule: cf.name, job_type: cf.job_type, error: e)
          return
        end
        KafkaBatch::Instrumentation.cron_fired(
          schedule: cf.name, job_type: cf.job_type, job_id: job_id, tenant_id: cf.tenant_id
        )
        Ledger.mark_dispatched(cf.schedule_id, cf.fire_at, job_id)
      end

      # heartbeat flags enabled schedules idle beyond their staleness threshold
      # (stale_factor × interval) and emits cron.stale + a cron.heartbeat pulse.
      # Reuses Reader.list, which already computes stale/interval/idle.
      def heartbeat(_now)
        rows = Reader.list(stale_factor: @stale_factor)
        enabled = rows.count { |r| r[:enabled] }
        stale = rows.select { |r| r[:stale] }
        max_stale = stale.map { |r| r[:idle_seconds].to_i }.max || 0
        stale.each do |r|
          KafkaBatch.logger.warn(
            "[KafkaBatch][Recurring] STALE schedule=#{r[:name]} job_type=#{r[:job_type]} " \
            "idle=#{r[:idle_seconds]}s threshold=#{r[:stale_threshold_seconds]}s"
          )
          KafkaBatch::Instrumentation.cron_stale(
            schedule: r[:name], job_type: r[:job_type],
            stale_seconds: r[:idle_seconds], threshold_seconds: r[:stale_threshold_seconds]
          )
        end
        KafkaBatch::Instrumentation.cron_heartbeat(
          enabled_count: enabled, stale_count: stale.size, max_stale_seconds: max_stale
        )
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Recurring] heartbeat error: #{e.message}")
      end

      # sleep_window sleeps one resolution window, returning true if still running.
      def sleep_window
        sleep(@window)
        running?
      end

      def monotonic
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      def positive(v, default)
        v = v.to_f
        v.positive? ? v : default
      end

      def positive_int(v, default)
        v = v.to_i
        v.positive? ? v : default
      end
    end
  end
end
