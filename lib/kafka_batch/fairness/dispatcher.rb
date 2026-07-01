require "karafka"
require "oj"
require "set"

module KafkaBatch
  module Fairness
    # Karafka consumer for the fairness INGEST topic — the whole fairness
    # mechanism, with no Redis and no separate forwarder.
    #
    # It simply forwards each job from the ingest topic onto the ready topic
    # (which a swarm of normal JobConsumers drains). Two things make this fair:
    #
    #   1. Kafka fetches roughly evenly across a consumer's assigned partitions,
    #      so when ingest is keyed one-tenant-per-partition the dispatcher
    #      naturally receives — and forwards — a balanced mix across tenants.
    #      One active tenant fills the ready topic alone (100%); N active split
    #      ~1/N; idle tenants contribute nothing (work-conserving).
    #
    #   2. A global THROTTLE keeps the ready topic shallow: when its un-consumed
    #      depth (the swarm's consumer-group lag) reaches fairness_ready_lag_high
    #      the dispatcher pauses forwarding, resuming below fairness_ready_lag_low.
    #      A shallow ready topic is what keeps fairness *dynamic* — a newly active
    #      tenant only ever waits behind ~the watermark, not the whole backlog.
    #
    # Fairness is approximate (fetch-batch granularity; assumes ~even partition
    # assignment per tenant and similar job sizes) — "good enough" by design.
    # Strict weighted shares would use KafkaBatch::Fairness::Scheduler instead.
    class Dispatcher < Karafka::BaseConsumer
      prepend KafkaBatch::Consumers::ConsumptionGate
      PAUSE_MS      = 500
      LAG_CACHE_TTL = 1.0  # seconds – Admin lag is polled at most this often per process

      @lag_cache       = { value: 0, at: -Float::INFINITY }
      @lag_cache_pid   = Process.pid   # #16 fix: fork-detection sentinel
      @lag_mutex       = Mutex.new
      # Bug #6 fix: promote @throttled to class-level with its own mutex so all
      # Dispatcher instances in this pod (one per assigned ingest partition) share
      # a single consistent throttle state, matching the class-wide lag cache.
      @throttled       = false
      @throttled_mutex = Mutex.new
      # Process-local cache of tenant IDs already written to the Scheduler's
      # VTIME hash. Once a tenant is in here, touch_tenants is not called again
      # for that tenant in this process — HSETNX would be a no-op anyway, so
      # we avoid the Redis round-trip entirely after the first touch.
      @touched_tenants     = Set.new
      @touched_tenants_pid = Process.pid
      @touched_mutex       = Mutex.new
      class << self
        attr_accessor :lag_cache, :lag_cache_pid, :lag_mutex, :throttled, :throttled_mutex,
                      :touched_tenants, :touched_tenants_pid, :touched_mutex
      end

      # Bug #7 fix: batch all messages into produce_many_sync (single broker
      # round-trip) and commit only the last offset, instead of produce_sync +
      # mark_as_consumed! per message in a loop.
      def consume
        if throttled?
          pause(messages.first.offset, PAUSE_MS)  # ready topic too deep – wait
          return
        end

        msgs_to_produce = messages.map do |message|
          {
            topic:   KafkaBatch.config.fairness_ready_topic,
            payload: message.raw_payload,           # forward verbatim (already JSON)
            key:     job_key(message.raw_payload)   # spread across ready partitions
          }
        end

        KafkaBatch::Producer.produce_many_sync(msgs_to_produce)
        mark_as_consumed!(messages.last)

        # Optimistically add the just-forwarded count to the cached lag so that
        # other Dispatcher threads (on other ingest partitions) see a realistic
        # estimate immediately — without waiting for the 1-second Admin refresh.
        # This prevents a burst where every thread sees cached_lag=0 for a full
        # second and collectively forwards far more than fairness_ready_lag_high
        # messages. When the next Admin read fires it replaces this estimate with
        # the real broker value (which also accounts for messages already consumed
        # from the ready topic by job consumers).
        optimistically_add_lag(msgs_to_produce.size)

        # Register tenant IDs with the optional Scheduler so they surface on
        # the /weights page automatically. Uses a process-local Set to skip
        # tenants already written — avoids a Redis round-trip on every batch
        # once a tenant has been touched once in this process's lifetime.
        if (sched = KafkaBatch.scheduler)
          tids = messages.filter_map { |m| extract_tenant_id(m.raw_payload) }.uniq
          new_tids = unseen_tenants(tids)
          unless new_tids.empty?
            sched.touch_tenants(new_tids)
            mark_tenants_seen(new_tids)
          end
        end
      end

      private

      # Hysteresis around the ready-topic depth so we don't flap on one threshold.
      # Bug #6 fix: use class-level throttle state (with mutex) so all Dispatcher
      # instances in this pod observe the same watermark consistently.
      def throttled?
        lag  = cached_ready_lag
        high = KafkaBatch.config.fairness_ready_lag_high.to_i
        low  = KafkaBatch.config.fairness_ready_lag_low.to_i
        self.class.throttled_mutex.synchronize do
          if self.class.throttled
            self.class.throttled = false if lag < low
          elsif lag >= high
            self.class.throttled = true
          end
          self.class.throttled
        end
      end

      # Process-wide cached ready-topic lag (avoids an Admin call per poll).
      # #15 fix: release lag_mutex before making the Admin API call so other
      # Dispatcher instances in this pod are not blocked for 100ms+ while one
      # fetches lag. Re-acquire to write the result only if the cache is still
      # stale (a concurrent caller may have already refreshed it).
      # #16 fix: PID-based fork detection — if Process.pid changed we are in a
      # forked child that inherited stale state; reset before proceeding.
      def cached_ready_lag
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        # Fast path (or fork-reset path): brief mutex window.
        self.class.lag_mutex.synchronize do
          # #16 fix: detect fork by PID change and reset inherited state.
          if Process.pid != self.class.lag_cache_pid
            self.class.lag_cache     = { value: 0, at: -Float::INFINITY }
            self.class.lag_cache_pid = Process.pid
            self.class.throttled     = false
          end

          c = self.class.lag_cache
          return c[:value] if now - c[:at] < LAG_CACHE_TTL
        end

        # Slow path: fetch OUTSIDE the mutex so we don't block other threads.
        new_value = read_ready_lag

        # Write back under the mutex; skip if a concurrent caller already wrote
        # a fresher value while we were fetching.
        # Always reset :at to now so the 1-second window restarts from the real
        # read, even if an optimistic increment bumped :value in the meantime.
        self.class.lag_mutex.synchronize do
          c = self.class.lag_cache
          if now - c[:at] >= LAG_CACHE_TTL
            c[:value] = new_value
            c[:at]    = now
          end
          c[:value]
        end
      end

      # Immediately increment the cached lag by +count+ after forwarding a batch
      # to the ready topic. This prevents sibling Dispatcher threads (each on a
      # different ingest partition) from reading stale lag=0 and collectively
      # overshooting the high-watermark within the 1-second Admin-read window.
      # The cache timestamp is intentionally NOT updated so the normal TTL expiry
      # still triggers a real Admin read within LAG_CACHE_TTL seconds.
      def optimistically_add_lag(count)
        self.class.lag_mutex.synchronize do
          c = self.class.lag_cache
          c[:value] = c[:value].to_i + count
        end
      end

      def read_ready_lag
        return 0 unless KafkaBatch::Lag.available?

        group = KafkaBatch.jobs_fair_consumer_group
        topic = KafkaBatch.config.fairness_ready_topic
        data  = KafkaBatch::Lag.read_group(group, [topic])
        (data[group] || {}).values.sum { |parts| parts.values.sum { |i| [i[:lag].to_i, 0].max } }
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Fairness::Dispatcher] lag read failed: #{e.message}")
        0
      end

      def job_key(raw)
        Oj.load(raw)["job_id"]
      rescue StandardError
        nil
      end

      # Extract the tenant_id from a raw JSON payload. Returns nil when the
      # payload has no tenant_id (e.g. batch-level fallback keys a job by
      # batch_id — we don't want batch IDs appearing as "tenants" on /weights).
      def extract_tenant_id(raw)
        t = Oj.load(raw)["tenant_id"]
        t.is_a?(String) && !t.empty? ? t : nil
      rescue StandardError
        nil
      end

      # Returns tenant IDs not yet touched in this process. Also resets the
      # cache on fork (Karafka forks workers; inherited state is stale).
      def unseen_tenants(tids)
        self.class.touched_mutex.synchronize do
          if Process.pid != self.class.touched_tenants_pid
            self.class.touched_tenants     = Set.new
            self.class.touched_tenants_pid = Process.pid
          end
          tids.reject { |t| self.class.touched_tenants.include?(t) }
        end
      end

      def mark_tenants_seen(tids)
        self.class.touched_mutex.synchronize do
          tids.each { |t| self.class.touched_tenants.add(t) }
        end
      end
    end

  end
end
