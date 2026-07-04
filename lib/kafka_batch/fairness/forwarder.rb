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

      # Check out one fairly-selected job and forward it to this lane's ready topic.
      # @return [Boolean] true if a job was forwarded; false when nothing is
      #   ready or the global in-flight window is full.
      def forward_once
        sched = KafkaBatch.scheduler(@type)
        return false unless sched

        job = sched.checkout
        return false unless job

        payload = mark_slot(job[:payload], job[:tenant_id])
        KafkaBatch::Producer.produce_sync(
          topic:   KafkaBatch.config.fairness_ready_topic(@type),
          payload: payload,
          key:     job_key(payload)
        )
        true
      end

      private

      def idle_sleep
        v = KafkaBatch.config.fairness_forwarder_idle_sleep.to_f
        v.positive? ? v : DEFAULT_IDLE_SLEEP
      end

      # Stamp the fair-slot marker, tenant_id, and lane type into the raw job JSON
      # so the JobConsumer knows this ready message holds one Scheduler in-flight
      # slot on the CORRECT lane, and releases it (Scheduler(type)#complete) once.
      def mark_slot(raw, tenant_id)
        data = Oj.load(raw)
        data["_fair_slot"] = true
        data["_fair_type"] = @type.to_s
        data["tenant_id"] ||= tenant_id
        Oj.dump(data, mode: :compat)
      rescue StandardError
        raw
      end

      # Spread across ready-topic partitions by job_id.
      def job_key(raw)
        Oj.load(raw)["job_id"]
      rescue StandardError
        nil
      end
    end
  end
end
