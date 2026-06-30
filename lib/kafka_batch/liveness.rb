require "redis"
require "connection_pool"
require "securerandom"
require "socket"
require "oj"

module KafkaBatch
  # Liveness for the dashboard: which jobs are running, on which consumer, and
  # how many consumers are alive. Two backends (config.liveness_backend):
  #
  #   :redis – (default) full per-job tracking in Redis (config.redis_url), short
  #            TTL, best-effort behind a circuit breaker. Most detailed.
  #   :store – consumer heartbeat + SAMPLED "current job" in the configured store
  #            (e.g. MySQL). Writes scale with the number of consumers, NOT job
  #            throughput, so it's reliable and low-impact. No per-job row churn.
  #   :off   – disabled.
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
        case backend
        when :redis then redis_available?
        when :store then true  # backed by the app's store (DB); read errors are surfaced in the UI
        else false
        end
      end

      # ── Entry points (dispatch by backend) ─────────────────────────────────

      def job_started(job_id:, batch_id:, worker_class:, topic: nil, partition: nil)
        case backend
        when :redis
          redis_job_started(job_id: job_id, batch_id: batch_id, worker_class: worker_class, topic: topic, partition: partition)
        when :store
          set_current_job(job_id: job_id, batch_id: batch_id, worker_class: worker_class.to_s, topic: topic, partition: partition)
          store_heartbeat(topic: topic)  # throttled
        end
      end

      def job_finished(job_id)
        case backend
        when :redis then redis_job_finished(job_id)
        when :store then clear_current_job(job_id)
        end
      end

      def heartbeat(topic: nil)
        case backend
        when :redis then redis_heartbeat(topic: topic)
        when :store then store_heartbeat(topic: topic)
        end
      end

      def running_jobs(limit: 500)
        case backend
        when :redis then redis_scan(JOB_PREFIX, limit)
        when :store then store_running_jobs(limit)
        else []
        end
      end

      def consumers(limit: 200)
        case backend
        when :redis then redis_scan(CONSUMER_PREFIX, limit)
        when :store then store_consumers(limit)
        else []
        end
      end

      def reset!
        @pool&.shutdown(&:close) rescue nil
        @pool = nil
        @circuit_open_until = nil
        hb_mutex.synchronize do
          @current_job       = nil
          @jobs_done         = 0
          @last_heartbeat_at = nil
        end
      end

      # ── :store backend (heartbeat + sampled current job) ───────────────────
      private

      def hb_mutex
        @hb_mutex ||= Mutex.new
      end

      def set_current_job(job_id:, batch_id:, worker_class:, topic:, partition:)
        hb_mutex.synchronize do
          @current_job = {
            "current_job_id"    => job_id,
            "current_batch_id"  => batch_id,
            "current_worker"    => worker_class,
            "current_topic"     => topic,
            "current_partition" => partition
          }
        end
      end

      def clear_current_job(job_id)
        hb_mutex.synchronize do
          @jobs_done = (@jobs_done || 0) + 1
          @current_job = nil if @current_job && @current_job["current_job_id"] == job_id
        end
      end

      # Throttled upsert: at most one DB write per liveness_heartbeat_interval
      # per process, regardless of job throughput.
      #
      # Moderate bug fix: the check + update of @last_heartbeat_at was previously
      # done outside hb_mutex, causing a TOCTOU race under JRuby / TruffleRuby.
      # Now the throttle guard and the current_job snapshot are both taken under
      # hb_mutex so no intermediate state is observable.
      def store_heartbeat(topic: nil)
        now      = monotonic
        interval = KafkaBatch.config.liveness_heartbeat_interval.to_i

        snapshot = hb_mutex.synchronize do
          if @last_heartbeat_at && (now - @last_heartbeat_at) < interval
            nil  # sentinel: skip write
          else
            @last_heartbeat_at = now
            (@current_job || {}).dup
          end
        end
        return if snapshot.nil?  # throttled
        data = {
          hostname:          Socket.gethostname,
          pid:               Process.pid,
          topic:             topic,
          jobs_done:         (@jobs_done || 0),
          current_job_id:    snapshot["current_job_id"],
          current_worker:    snapshot["current_worker"],
          current_batch_id:  snapshot["current_batch_id"],
          current_topic:     snapshot["current_topic"],
          current_partition: snapshot["current_partition"]
        }
        KafkaBatch.store.record_heartbeat(consumer_id, data)
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch::Liveness] store heartbeat failed: #{e.message}")
      end

      def store_consumers(limit)
        active_heartbeats.first(limit).map do |h|
          {
            "consumer_id" => h[:consumer_id], "hostname" => h[:hostname],
            "pid" => h[:pid], "topic" => h[:topic], "last_seen" => h[:last_seen].to_s
          }
        end
      end

      def store_running_jobs(limit)
        active_heartbeats.select { |h| h[:current_job_id] }.first(limit).map do |h|
          {
            "job_id" => h[:current_job_id], "batch_id" => h[:current_batch_id],
            "worker_class" => h[:current_worker], "consumer_id" => h[:consumer_id],
            "topic" => h[:current_topic], "partition" => h[:current_partition],
            "started_at" => h[:last_seen].to_s
          }
        end
      end

      def active_heartbeats
        KafkaBatch.store.list_heartbeats(Time.now - ttl)
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch::Liveness] list_heartbeats failed: #{e.message}")
        []
      end

      def monotonic
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      # ── :redis backend ─────────────────────────────────────────────────────

      def redis_enabled?
        KafkaBatch.config.track_running_jobs
      end

      def redis_available?
        !redis_with { |r| r.ping }.nil?
      end

      def redis_job_started(job_id:, batch_id:, worker_class:, topic:, partition:)
        return unless redis_enabled?
        payload = dump(
          "job_id" => job_id, "batch_id" => batch_id, "worker_class" => worker_class.to_s,
          "consumer_id" => consumer_id, "topic" => topic, "partition" => partition,
          "started_at" => Time.now.iso8601
        )
        redis_with { |r| r.set("#{JOB_PREFIX}#{consumer_id}:#{job_id}", payload, ex: ttl) }
      end

      def redis_job_finished(job_id)
        return unless redis_enabled?
        redis_with { |r| r.del("#{JOB_PREFIX}#{consumer_id}:#{job_id}") }
      end

      def redis_heartbeat(topic:)
        return unless redis_enabled?
        payload = dump(
          "consumer_id" => consumer_id, "hostname" => Socket.gethostname,
          "pid" => Process.pid, "topic" => topic, "last_seen" => Time.now.iso8601
        )
        redis_with { |r| r.set("#{CONSUMER_PREFIX}#{consumer_id}", payload, ex: ttl) }
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
          Redis.new(url: KafkaBatch.config.redis_url, timeout: 1, reconnect_attempts: 0)
        end
      end

      def redis_circuit_closed?
        url = KafkaBatch.config.redis_url
        return false if url.nil? || url.empty?
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
