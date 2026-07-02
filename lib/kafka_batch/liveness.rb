require "redis"
require "connection_pool"
require "securerandom"
require "socket"
require "oj"
require_relative "process_stats"

module KafkaBatch
  # Liveness for the dashboard: which jobs are running, on which consumer, and
  # how many consumers are alive. Backed entirely by Redis (config.redis_url) —
  # Redis is a required dependency of the gem.
  #
  #   config.liveness_backend:
  #     :redis – (default) full per-job + per-consumer tracking in Redis with a
  #              short TTL, best-effort behind a circuit breaker.
  #     :off   – disabled.
  #
  # Consumer heartbeats run whenever the backend is :redis. Per-job running-state
  # writes are gated by config.track_running_jobs.
  #
  # RSS/CPU for /live are sampled in-process at most once per
  # config.liveness_stats_interval seconds (default 15s) and piggyback on the
  # existing Redis heartbeat key — no extra round-trips.
  #
  # All entry points (job_started/job_finished/heartbeat) are best-effort and
  # never raise into the job hot path.
  module Liveness
    JOB_PREFIX       = "kafka_batch:live:job:"
    CONSUMER_PREFIX  = "kafka_batch:live:consumer:"
    CIRCUIT_COOLDOWN = 30

    class << self
      def consumer_id
        @consumer_id ||= "#{Socket.gethostname}:#{Process.pid}:#{SecureRandom.hex(3)}"
      end

      def backend
        KafkaBatch.config.liveness_backend
      end

      def ttl
        KafkaBatch.config.liveness_ttl
      end

      def available?
        backend == :redis && redis_available?
      end

      # ── Entry points ───────────────────────────────────────────────────────

      def job_started(job_id:, batch_id:, worker_class:, topic: nil, partition: nil)
        return unless backend == :redis
        return unless track_running_jobs?
        redis_job_started(job_id: job_id, batch_id: batch_id, worker_class: worker_class, topic: topic, partition: partition)
      end

      def job_finished(job_id)
        return unless backend == :redis
        return unless track_running_jobs?
        redis_job_finished(job_id)
      end

      def heartbeat(topic: nil)
        return unless backend == :redis
        redis_heartbeat(topic: topic)
      end

      def running_jobs(limit: 500)
        return [] unless backend == :redis
        redis_scan(JOB_PREFIX, limit)
      end

      def consumers(limit: 200)
        return [] unless backend == :redis
        redis_scan(CONSUMER_PREFIX, limit)
      end

      def reset!
        @pool&.shutdown(&:close) rescue nil
        @pool = nil
        @circuit_open_until = nil
        @stats_mutex = nil
        @stats_cache = nil
        @stats_sampled_at = nil
        ProcessStats.reset!
      end

      # ── :redis backend ─────────────────────────────────────────────────────
      private

      def track_running_jobs?
        KafkaBatch.config.track_running_jobs
      end

      def redis_available?
        !redis_with { |r| r.ping }.nil?
      end

      def redis_job_started(job_id:, batch_id:, worker_class:, topic:, partition:)
        payload = dump(
          "job_id" => job_id, "batch_id" => batch_id, "worker_class" => worker_class.to_s,
          "consumer_id" => consumer_id, "topic" => topic, "partition" => partition,
          "started_at" => Time.now.iso8601
        )
        redis_with { |r| r.set("#{JOB_PREFIX}#{consumer_id}:#{job_id}", payload, ex: ttl) }
      end

      def redis_job_finished(job_id)
        redis_with { |r| r.del("#{JOB_PREFIX}#{consumer_id}:#{job_id}") }
      end

      def redis_heartbeat(topic:)
        payload = {
          "consumer_id" => consumer_id,
          "hostname"    => Socket.gethostname,
          "pid"         => Process.pid,
          "topic"       => topic,
          "last_seen"   => Time.now.iso8601
        }
        payload.merge!(current_stats)

        redis_with { |r| r.set("#{CONSUMER_PREFIX}#{consumer_id}", dump(payload), ex: ttl) }
      end

      # Throttled process stats (RSS + CPU) for the /live page.
      def current_stats
        interval = KafkaBatch.config.liveness_stats_interval.to_f
        return {} if interval <= 0

        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        stats_mutex.synchronize do
          if @stats_cache.nil? || (now - @stats_sampled_at.to_f) >= interval
            @stats_cache       = ProcessStats.sample
            @stats_sampled_at  = now
          end
          @stats_cache || {}
        end
      rescue StandardError
        {}
      end

      def stats_mutex
        @stats_mutex ||= Mutex.new
      end

      def redis_scan(prefix, limit)
        result = []
        redis_with do |r|
          cursor = "0"
          loop do
            cursor, keys = r.scan(cursor, match: "#{prefix}*", count: 100)
            unless keys.empty?
              r.mget(*keys).each { |v| (h = load(v)) && result << h }
            end
            break if cursor == "0" || result.size >= limit
          end
        end
        result.first(limit)
      end

      def redis_with
        return nil unless redis_circuit_closed?
        redis_pool.with { |r| yield r }
      rescue StandardError => e
        redis_trip_circuit!
        KafkaBatch.logger.debug("[KafkaBatch::Liveness] Redis unavailable: #{e.message}")
        nil
      end

      def redis_pool
        @pool ||= ConnectionPool.new(size: 3, timeout: 1) do
          KafkaBatch::RedisClient.new(KafkaBatch.config, timeout: 1, reconnect_attempts: 0) ||
            raise(ConfigurationError, "Redis is not configured")
        end
      end

      def redis_circuit_closed?
        return false unless KafkaBatch.config.redis_configured?
        @circuit_open_until.nil? || Time.now >= @circuit_open_until
      end

      def redis_trip_circuit!
        @circuit_open_until = Time.now + CIRCUIT_COOLDOWN
        @pool = nil
      end

      def dump(hash)
        Oj.dump(hash, mode: :compat)
      end

      def load(str)
        Oj.load(str)
      rescue StandardError
        nil
      end
    end
  end
end
