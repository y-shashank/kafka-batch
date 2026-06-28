require "karafka"
require "oj"

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
      PAUSE_MS      = 500
      LAG_CACHE_TTL = 1.0  # seconds – Admin lag is polled at most this often per process

      @lag_cache = { value: 0, at: -1.0 }
      @lag_mutex = Mutex.new
      class << self; attr_accessor :lag_cache, :lag_mutex; end

      def consume
        if throttled?
          pause(messages.first.offset, PAUSE_MS)  # ready topic too deep – wait
          return
        end

        messages.each do |message|
          KafkaBatch::Producer.produce_sync(
            topic:   KafkaBatch.config.fairness_ready_topic,
            payload: message.raw_payload,           # forward verbatim
            key:     job_key(message.raw_payload)   # spread across ready partitions
          )
          mark_as_consumed!(message)
        end
      end

      private

      # Hysteresis around the ready-topic depth so we don't flap on one threshold.
      def throttled?
        lag  = cached_ready_lag
        high = KafkaBatch.config.fairness_ready_lag_high.to_i
        low  = KafkaBatch.config.fairness_ready_lag_low.to_i
        @throttled = false if @throttled.nil?
        if @throttled
          @throttled = false if lag < low
        elsif lag >= high
          @throttled = true
        end
        @throttled
      end

      # Process-wide cached ready-topic lag (avoids an Admin call per poll).
      def cached_ready_lag
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        self.class.lag_mutex.synchronize do
          c = self.class.lag_cache
          if now - c[:at] >= LAG_CACHE_TTL
            c[:value] = read_ready_lag
            c[:at]    = now
          end
          c[:value]
        end
      end

      def read_ready_lag
        return 0 unless KafkaBatch::Lag.available?

        group = "#{KafkaBatch.config.consumer_group}-jobs"
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
    end
  end
end
