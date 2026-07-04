require "oj"
require_relative "schedule/scheduled_reader"

module KafkaBatch
  # Per-process background thread that turns the delayed-job index into real
  # execution — the runtime half of perform_in / perform_at.
  #
  # Loop, each tick:
  #   1. reclaim (throttled) — return lease-expired in-flight jobs to pending so a
  #      crashed poller's claims are recovered (at-least-once).
  #   2. claim_due — atomically lease up to schedule_batch_size due pointers.
  #   3. group by partition, sort by offset, read payloads from scheduled_topic
  #      (near-sequential; see Schedule::ScheduledReader).
  #   4. skip jobs whose batch was cancelled; re-produce the rest onto their real
  #      topic via Batch.route_for.
  #   5. ack only what was produced (or intentionally dropped). Anything left
  #      leased is recovered by a later reclaim.
  #
  # Crash safety: because claim → produce → ack is not atomic, a crash between
  # claim and ack leaves entries leased; reclaim (run by ANY poller in ANY
  # process) returns them to pending after schedule_lease_seconds. The only cost
  # is a rare duplicate re-produce, which is safe (JobConsumer dedups completions
  # by job_id; the producer is idempotent).
  #
  # Safe to run in every consumer process at once: claim_due is atomic per
  # backend (Redis Lua / MySQL SELECT..FOR UPDATE SKIP LOCKED), so no two pollers
  # ever claim the same entry — no leader election needed.
  class SchedulePoller
    DEFAULT_POLL_INTERVAL = 5.0
    DEFAULT_LEASE         = 60
    DEFAULT_RECLAIM_EVERY = 30
    DEFAULT_BATCH_SIZE    = 100

    @mutex   = Mutex.new
    @running = false
    @thread  = nil

    class << self
      attr_reader :thread

      # Start the singleton poller thread for this process (idempotent).
      def ensure_running!
        return unless KafkaBatch.config.schedule_poller_enabled

        @mutex.synchronize do
          return if @running && @thread&.alive?
          @running = true
          @thread  = Thread.new { new.run }
          @thread.name = "kafka-batch-schedule-poller" if @thread.respond_to?(:name=)
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

    def initialize(store: nil, reader: nil)
      @store        = store
      @reader       = reader
      @last_reclaim = 0.0
    end

    def running?
      self.class.running?
    end

    def run
      KafkaBatch.logger.info(
        "[KafkaBatch][SchedulePoller] started (backend=#{KafkaBatch.config.schedule_store})"
      )
      base = poll_interval
      cap  = [max_poll_interval, base].max
      wait = base

      while running?
        begin
          dispatched = tick
          if dispatched.zero?
            # Nothing was due — back off (exponentially, capped) so idle pods stop
            # hammering the schedule store. With N pods this is the main throttle on
            # idle DB/Redis load; jitter de-syncs them so they don't poll in lockstep.
            sleep(jittered(wait))
            wait = [wait * 2, cap].min
          else
            # Work is flowing — poll at full speed to drain the backlog.
            wait = base
          end
        rescue StandardError => e
          KafkaBatch.logger.error(
            "[KafkaBatch][SchedulePoller] loop error: #{e.class}: #{e.message}"
          )
          sleep(jittered(wait))
          wait = [wait * 2, cap].min
        end
      end

      @reader&.close
      KafkaBatch.logger.info("[KafkaBatch][SchedulePoller] stopped")
    end

    # One drain cycle. Returns the number of jobs dispatched (or dropped) so the
    # loop knows whether to sleep. Public for testing.
    def tick
      st = store
      return 0 unless st

      maybe_reclaim(st)

      members = st.claim_due(
        now:           Time.now,
        lease_seconds: lease_seconds,
        limit:         batch_size
      )
      return 0 if members.nil? || members.empty?

      dispatch(st, members)
    end

    private

    # Group claimed members by partition (sorted by offset), read their payloads
    # from the scheduled topic, then re-produce each due job. ack everything we
    # resolved (produced, cancelled-drop, or permanently lost); leave the rest
    # leased for reclaim to retry.
    def dispatch(st, members)
      parsed = members.filter_map do |m|
        p = Schedule::Member.parse(m)
        p && p.merge(member: m)
      end

      by_partition = Hash.new { |h, k| h[k] = [] }
      parsed.each { |p| by_partition[p[:partition]] << p[:offset] }

      result = reader.read(by_partition)
      found  = result[:found]
      lost   = result[:lost].to_a

      acked = 0
      done  = []

      parsed.each do |p|
        loc = Schedule::Member.build_key(p[:partition], p[:offset])

        if lost.include?(loc)
          # Payload gone from Kafka (retention). Can't recover — drop, don't loop.
          KafkaBatch.logger.error(
            "[KafkaBatch][SchedulePoller] job_id=#{p[:job_id]} payload missing at " \
            "#{KafkaBatch.config.scheduled_topic}/#{loc} (retention?) — dropping."
          )
          done << p[:member]
          next
        end

        payload = found[loc]
        next unless payload  # not fetched this pass → leave leased, reclaim retries

        if produce_due(payload, p[:job_id])
          acked += 1
          done  << p[:member]
        end
      end

      st.ack(done) unless done.empty?
      acked
    end

    # Re-produce one due job onto its real destination topic. Returns true if the
    # job was handled (produced OR intentionally skipped) and may be acked; false
    # to leave it leased for retry.
    def produce_due(payload, job_id)
      data         = Oj.load(payload)
      worker_name  = data["worker_class"]
      batch_id     = data["batch_id"]
      tenant_id    = data["tenant_id"]

      if cancelled?(batch_id)
        KafkaBatch::Instrumentation.job_cancelled(
          job_id: job_id, batch_id: batch_id, worker_class: worker_name
        )
        return true  # drop: batch is cancelled, ack it
      end

      worker_class = resolve_worker(worker_name)
      unless worker_class
        KafkaBatch.logger.error(
          "[KafkaBatch][SchedulePoller] unknown worker_class=#{worker_name.inspect} " \
          "for job_id=#{job_id} — dropping."
        )
        return true
      end

      route = KafkaBatch::Batch.route_for(
        worker_class, job_id: job_id, tenant_id: tenant_id, batch_id: batch_id
      )
      KafkaBatch::Producer.produce_sync(
        topic:     route[:topic],
        payload:   payload,
        key:       route[:key],
        partition: route[:partition]
      )
      KafkaBatch::Instrumentation.scheduled_dispatched(
        job_id: job_id, batch_id: batch_id, worker_class: worker_name, topic: route[:topic]
      )
      true
    rescue KafkaBatch::ProducerError => e
      # Transient produce failure — leave leased so reclaim re-dispatches it.
      KafkaBatch.logger.error(
        "[KafkaBatch][SchedulePoller] produce failed for job_id=#{job_id}: #{e.message}"
      )
      false
    rescue Oj::ParseError => e
      KafkaBatch.logger.error(
        "[KafkaBatch][SchedulePoller] malformed scheduled payload for job_id=#{job_id}: #{e.message} — dropping."
      )
      true
    end

    def maybe_reclaim(st)
      now = monotonic
      return if (now - @last_reclaim) < reclaim_interval

      @last_reclaim = now
      n = st.reclaim(now: Time.now)
      KafkaBatch.logger.info("[KafkaBatch][SchedulePoller] reclaimed #{n} leased job(s)") if n.to_i.positive?
    rescue StandardError => e
      KafkaBatch.logger.error("[KafkaBatch][SchedulePoller] reclaim error: #{e.message}")
    end

    def cancelled?(batch_id)
      return false unless batch_id && KafkaBatch.config.skip_cancelled_jobs
      return false unless defined?(KafkaBatch::CancellationCache)

      KafkaBatch::CancellationCache.cancelled?(batch_id)
    rescue StandardError
      false
    end

    def resolve_worker(name)
      return nil if name.nil? || name.to_s.empty?

      KafkaBatch.workers.find { |w| w.name == name } || Object.const_get(name)
    rescue NameError
      nil
    end

    def store
      @store ||= KafkaBatch.schedule_store
    end

    def reader
      @reader ||= Schedule::ScheduledReader.new
    end

    def poll_interval
      v = KafkaBatch.config.schedule_poll_interval.to_f
      v.positive? ? v : DEFAULT_POLL_INTERVAL
    end

    def max_poll_interval
      v = KafkaBatch.config.schedule_poll_max_interval.to_f
      v.positive? ? v : poll_interval
    end

    def lease_seconds
      v = KafkaBatch.config.schedule_lease_seconds.to_i
      v.positive? ? v : DEFAULT_LEASE
    end

    def reclaim_interval
      v = KafkaBatch.config.schedule_reclaim_interval.to_i
      v.positive? ? v : DEFAULT_RECLAIM_EVERY
    end

    def batch_size
      v = KafkaBatch.config.schedule_batch_size.to_i
      v.positive? ? v : DEFAULT_BATCH_SIZE
    end

    def jittered(seconds)
      j = KafkaBatch.config.schedule_poll_jitter.to_f
      return seconds if j <= 0

      seconds * (1 + ((rand * 2) - 1) * j)
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
