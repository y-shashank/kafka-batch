require "oj"

module KafkaBatch
  module Fairness
    # Per-process background thread(s) that turn a fairness lane's Redis WFQ
    # Scheduler into real dispatch: repeatedly #checkout the fairest next job and
    # forward it to that lane's Kafka ready topic, where the JobConsumer swarm
    # executes it.
    #
    #   ingest topic → Dispatcher (Scheduler#enqueue → bounded Redis window)
    #                → Forwarder  (Scheduler#checkout → ready topic)   ← this file
    #                → ready topic → JobConsumer → perform → Scheduler#complete
    #
    # Two lanes (:time, :throughput) run at once — this manages ONE thread per
    # active lane in the process (started lazily by the lane's Dispatcher). Each
    # thread is bound to its lane's Scheduler(type) and ready topic. Safe in many
    # processes at once: #checkout is a single atomic Redis Lua call.
    class Forwarder
      DEFAULT_IDLE_SLEEP = 0.05  # seconds
      DEFAULT_BURST      = 50    # max forwards per loop iteration before yielding

      @mutex   = Mutex.new
      @threads = {}   # type => Thread
      @running = {}   # type => Boolean

      class << self
        # The running thread for a lane (nil if none).
        def thread(type = :time)
          @threads[type.to_sym]
        end

        # Start the singleton forwarder thread for +type+ in this process (idempotent).
        def ensure_running!(type = :time)
          type = type.to_sym
          @mutex.synchronize do
            return if @running[type] && @threads[type]&.alive?
            @running[type] = true
            t = Thread.new { new(type).run }
            t.name = "kafka-batch-fairness-forwarder-#{type}" if t.respond_to?(:name=)
            @threads[type] = t
          end
        end

        # Stop one lane's forwarder, or all when type is nil.
        def stop!(type = nil, timeout: 5)
          targets = nil
          @mutex.synchronize do
            targets = type ? [type.to_sym] : @running.keys.dup
            targets.each do |ty|
              @running[ty] = false
            end
          end
          joined = targets.map do |ty|
            th = nil
            @mutex.synchronize { th = @threads.delete(ty) }
            th&.join(timeout)
          end
          @mutex.synchronize { targets.each { |ty| @running.delete(ty) } }
          joined
        end

        def running?(type = nil)
          if type
            ty = type.to_sym
            !!(@running[ty] && @threads[ty]&.alive?)
          else
            @running.any? { |ty, on| on && @threads[ty]&.alive? }
          end
        end

        # Test/reset seam: forget all thread references without joining.
        def reset!
          @mutex.synchronize do
            @running = {}
            @threads = {}
          end
        end

        def running_types
          @mutex.synchronize { @threads.keys.select { |ty| @running[ty] && @threads[ty]&.alive? } }
        end
      end

      def initialize(type = :time)
        @type = type.to_sym
      end

      def running?
        self.class.running?(@type)
      end

      # Main loop: forward a burst of fairly-selected jobs, then sleep briefly if
      # nothing was ready (or the global in-flight window is full).
      def run
        KafkaBatch.logger.info("[KafkaBatch][Fairness::Forwarder] started (lane=#{@type})")
        idle  = idle_sleep
        burst = DEFAULT_BURST

        while running?
          begin
            forwarded = 0
            forwarded += 1 while forwarded < burst && running? && forward_once
            maybe_reclaim_leases
            maybe_reclaim_stale_forwards
            sleep(idle) if forwarded.zero?
          rescue StandardError => e
            KafkaBatch.logger.error(
              "[KafkaBatch][Fairness::Forwarder] lane=#{@type} loop error: #{e.class}: #{e.message}"
            )
            sleep(idle)
          end
        end

        KafkaBatch.logger.info("[KafkaBatch][Fairness::Forwarder] stopped (lane=#{@type})")
      end

      # Interval (seconds) between full lease-reclaim sweeps. The checkout pre-pass
      # already heals any tenant with ready work; this sweep only mops up leases
      # leaked by a tenant that then went idle, so it can run infrequently.
      RECLAIM_INTERVAL = 30.0

      # Check out one fairly-selected job and forward it to this lane's ready topic.
      # @return [Boolean] true if a job was forwarded; false when nothing is
      #   ready or the global in-flight window is full.
      def forward_once
        sched = KafkaBatch.scheduler(@type)
        return false unless sched

        job = sched.checkout
        return false unless job

        begin
          data = Oj.load(job[:payload])
          if KafkaBatch::JobExpiry.expired?(data)
            drop_expired_forwarded!(sched, job, data)
            sched.confirm_forward(job[:slot_id])
            return true
          end

          payload = mark_slot!(job[:payload], job[:tenant_id], job[:slot_id])
          KafkaBatch::Producer.produce_sync(
            topic:   ready_topic_for(data),
            payload: payload,
            key:     job_key(payload)
          )
          sched.confirm_forward(job[:slot_id])
          true
        rescue StandardError => e
          abort_forward_failure!(sched, job, e)
          false
        end
      end

      private

      def ready_topic_for(data)
        cfg = KafkaBatch.config
        if cfg.runtime_split_fair_ready?(@type)
          runtime = KafkaBatch::HandlerRegistry.runtime_for_payload(data)
          cfg.fairness_ready_topic(@type, runtime)
        else
          cfg.fairness_ready_topic(@type)
        end
      end

      # Run a full lease-reclaim sweep at most once per RECLAIM_INTERVAL, so leases
      # leaked by a now-idle tenant don't linger in the global in-flight total.
      def maybe_reclaim_leases
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @last_reclaim_at ||= 0.0
        return unless (now - @last_reclaim_at) >= RECLAIM_INTERVAL

        @last_reclaim_at = now
        sched = KafkaBatch.scheduler(@type)
        n = sched&.reclaim_expired_leases!.to_i
        KafkaBatch.logger.info("[KafkaBatch][Fairness::Forwarder] lane=#{@type} reclaimed #{n} expired lease(s)") if n.positive?
      end

      # Re-deliver jobs orphaned in the forwarding buffer (crash between checkout
      # and produce/confirm). Runs on the same interval as lease reclaim.
      def maybe_reclaim_stale_forwards
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        @last_forward_reclaim_at ||= 0.0
        return unless (now - @last_forward_reclaim_at) >= RECLAIM_INTERVAL

        @last_forward_reclaim_at = now
        sched = KafkaBatch.scheduler(@type)
        return unless sched

        sched.list_stale_forwards.each { |entry| reclaim_stale_forward!(sched, entry) }
      end

      def reclaim_stale_forward!(sched, entry)
        data = Oj.load(entry[:payload])
        payload = mark_slot!(entry[:payload], entry[:tenant_id], entry[:slot_id])
        KafkaBatch::Producer.produce_sync(
          topic:   ready_topic_for(data),
          payload: payload,
          key:     job_key(payload)
        )
        sched.confirm_forward(entry[:slot_id])
        sched.rearm_lease(entry[:tenant_id], slot_id: entry[:slot_id])
        KafkaBatch.logger.warn(
          "[KafkaBatch][Fairness::Forwarder] lane=#{@type} reclaimed stale forward " \
          "slot=#{entry[:slot_id]} tenant=#{entry[:tenant_id]}"
        )
      rescue StandardError => e
        if sched.abort_forward(entry[:slot_id], entry[:tenant_id])
          KafkaBatch.logger.warn(
            "[KafkaBatch][Fairness::Forwarder] lane=#{@type} re-enqueued stale forward " \
            "slot=#{entry[:slot_id]} after produce failure: #{e.class}: #{e.message}"
          )
        else
          KafkaBatch.logger.error(
            "[KafkaBatch][Fairness::Forwarder] lane=#{@type} could not recover stale forward " \
            "slot=#{entry[:slot_id]}: #{e.class}: #{e.message}"
          )
        end
      end

      def idle_sleep
        v = KafkaBatch.config.fairness_forwarder_idle_sleep.to_f
        v.positive? ? v : DEFAULT_IDLE_SLEEP
      end

      # Stamp the fair-slot marker, tenant_id, lane type, and in-flight LEASE id
      # into the raw job JSON so the JobConsumer knows this ready message holds one
      # Scheduler in-flight slot on the CORRECT lane, and releases exactly that
      # lease (Scheduler(type)#complete(slot_id:)) once.
      def mark_slot!(raw, tenant_id, slot_id = nil)
        data = Oj.load(raw)
        data["_fair_slot"]    = true
        data["_fair_type"]    = @type.to_s
        data["_fair_slot_id"] = slot_id if slot_id
        data["tenant_id"] ||= tenant_id
        Oj.dump(data, mode: :compat)
      end

      # Checkout wrote a forwarding copy + lease. On produce failure, roll back.
      def abort_forward_failure!(sched, job, error)
        if sched.abort_forward(job[:slot_id], job[:tenant_id])
          KafkaBatch.logger.warn(
            "[KafkaBatch][Fairness::Forwarder] lane=#{@type} aborted forward " \
            "for tenant=#{job[:tenant_id]}: #{error.class}: #{error.message}"
          )
        else
          KafkaBatch.logger.error(
            "[KafkaBatch][Fairness::Forwarder] lane=#{@type} forward failed and abort " \
            "did not find forwarding copy tenant=#{job[:tenant_id]}: #{error.class}: #{error.message}"
          )
        end
      rescue StandardError => e
        KafkaBatch.logger.error(
          "[KafkaBatch][Fairness::Forwarder] lane=#{@type} failed to abort forward " \
          "for tenant=#{job[:tenant_id]}: #{e.class}: #{e.message}"
        )
      end

      # Spread across ready-topic partitions by job_id.
      def job_key(raw)
        Oj.load(raw)["job_id"]
      rescue StandardError
        nil
      end

      # Checkout already claimed an in-flight lease — release it and drop the job.
      def drop_expired_forwarded!(sched, job, data)
        sched.complete(job[:tenant_id], slot_id: job[:slot_id], duration: 0)
        sched.confirm_forward(job[:slot_id])
        topic, partition, offset = KafkaBatch::JobExpiry.source_coords(data)
        KafkaBatch::JobExpiry.drop!(
          data: data, topic: topic, partition: partition, offset: offset,
          log_tag: "Fairness::Forwarder"
        )
      rescue StandardError => e
        KafkaBatch.logger.error(
          "[KafkaBatch][Fairness::Forwarder] expired drop failed lane=#{@type}: #{e.message}"
        )
      end
    end
  end
end
