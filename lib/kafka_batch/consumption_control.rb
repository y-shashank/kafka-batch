require "redis"
require "connection_pool"

module KafkaBatch
  # Cross-process pause/resume for Karafka consumers, driven from the Web UI.
  #
  # Backends (first available wins):
  #   :redis – config.redis_url (SET members)
  #   :mysql – kafka_batch_consumption_pauses when config.store = :mysql
  #
  # Karafka consumers refresh pause state at most once per
  # config.consumption_control_refresh_interval (default 60s). The /lag UI
  # always reads fresh state.
  module ConsumptionControl
    TOPICS_KEY     = "kafka_batch:consumption:topics"
    PARTITIONS_KEY = "kafka_batch:consumption:partitions"
    SEP            = "\x1f"

    class << self
      def available?
        !backend.nil?
      end

      # @return [Symbol, nil] :redis or :mysql
      #
      # Bug #16 fix: cache the backend probe for 30 seconds. Previously every
      # call to backend (from pause_topic, load_snapshot, etc.) performed a live
      # Redis PING, turning every ConsumptionControl call into a network round-trip.
      #
      # IMPORTANT: uses a dedicated backend_mutex — NOT cache_mutex — because
      # cached_snapshot holds cache_mutex while calling load_snapshot → backend.
      # Ruby mutexes are non-reentrant; locking cache_mutex here would deadlock.
      def backend
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        backend_mutex.synchronize do
          @backend_cache ||= { value: nil, at: -Float::INFINITY }
          if (now - @backend_cache[:at]) >= 30
            @backend_cache[:value] = detect_backend
            @backend_cache[:at]    = now
          end
          @backend_cache[:value]
        end
      end

      def pause_topic(group:, topic:)
        case backend
        when :redis then redis_pause_topic(group, topic)
        when :mysql then mysql_store.pause_consumption_topic(group: group, topic: topic)
        end
        invalidate_snapshot_cache!
      end

      def resume_topic(group:, topic:)
        case backend
        when :redis then redis_resume_topic(group, topic)
        when :mysql then mysql_store.resume_consumption_topic(group: group, topic: topic)
        end
        invalidate_snapshot_cache!
      end

      def pause_partition(group:, topic:, partition:)
        case backend
        when :redis then redis_pause_partition(group, topic, partition)
        when :mysql then mysql_store.pause_consumption_partition(group: group, topic: topic, partition: partition)
        end
        invalidate_snapshot_cache!
      end

      def resume_partition(group:, topic:, partition:)
        case backend
        when :redis then redis_resume_partition(group, topic, partition)
        when :mysql then mysql_store.resume_consumption_partition(group: group, topic: topic, partition: partition)
        end
        invalidate_snapshot_cache!
      end

      # @return [Boolean]
      def paused?(group:, topic:, partition:)
        snap = snapshot(refresh: false)
        topic_paused?(snap, group, topic) || partition_paused?(snap, group, topic, partition)
      end

      # @param refresh [Boolean] true for the Web UI; false uses the consumer cache
      # @return [Hash] { topics: Set<String>, partitions: Set<String> }
      def snapshot(refresh: false)
        refresh ? load_snapshot : cached_snapshot
      end

      def topic_paused?(snap, group, topic)
        snap[:topics].include?(topic_key(group, topic))
      end

      def partition_only_paused?(snap, group, topic, partition)
        snap[:partitions].include?(partition_key(group, topic, partition))
      end

      def partition_paused?(snap, group, topic, partition)
        topic_paused?(snap, group, topic) || partition_only_paused?(snap, group, topic, partition)
      end

      def topic_key(group, topic)
        "#{group}#{SEP}#{topic}"
      end

      def partition_key(group, topic, partition)
        "#{group}#{SEP}#{topic}#{SEP}#{partition}"
      end

      def reset!
        @pool = nil
        cache_mutex.synchronize do
          @snapshot_cache = { at: -Float::INFINITY, snap: empty_snapshot }
        end
        backend_mutex.synchronize do
          @backend_cache = { value: nil, at: -Float::INFINITY }
        end
      end

      private

      def backend_mutex
        @backend_mutex ||= Mutex.new
      end

      def detect_backend
        return :redis if redis_available?
        return :mysql if mysql_available?
        nil
      end

      def empty_snapshot
        { topics: Set.new, partitions: Set.new }
      end

      def cached_snapshot
        interval = KafkaBatch.config.consumption_control_refresh_interval.to_i
        interval = 60 if interval <= 0
        now      = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        cache_mutex.synchronize do
          @snapshot_cache ||= { at: -Float::INFINITY, snap: empty_snapshot }
          if now - @snapshot_cache[:at] >= interval
            @snapshot_cache[:snap] = load_snapshot
            @snapshot_cache[:at]   = now
          end
          @snapshot_cache[:snap]
        end
      end

      def cache_mutex
        @cache_mutex ||= Mutex.new
      end

      def invalidate_snapshot_cache!
        cache_mutex.synchronize do
          @snapshot_cache = { at: -Float::INFINITY, snap: empty_snapshot }
        end
      end

      def load_snapshot
        case backend
        when :redis then redis_snapshot
        when :mysql then mysql_store.consumption_pause_snapshot
        else empty_snapshot
        end
      end

      def redis_available?
        !redis_with { |r| r.ping }.nil?
      end

      def mysql_available?
        return false unless KafkaBatch.config.store == :mysql

        mysql_store.consumption_pauses_enabled?
      rescue StandardError
        false
      end

      def mysql_store
        store = KafkaBatch.store
        raise TypeError, "expected MysqlStore" unless store.is_a?(KafkaBatch::Stores::MysqlStore)

        store
      end

      def redis_pause_topic(group, topic)
        redis_with { |r| r.sadd(TOPICS_KEY, topic_key(group, topic)) }
      end

      def redis_resume_topic(group, topic)
        redis_with { |r| r.srem(TOPICS_KEY, topic_key(group, topic)) }
      end

      def redis_pause_partition(group, topic, partition)
        redis_with { |r| r.sadd(PARTITIONS_KEY, partition_key(group, topic, partition)) }
      end

      def redis_resume_partition(group, topic, partition)
        redis_with { |r| r.srem(PARTITIONS_KEY, partition_key(group, topic, partition)) }
      end

      def redis_snapshot
        redis_with do |r|
          {
            topics:     r.smembers(TOPICS_KEY).to_set,
            partitions: r.smembers(PARTITIONS_KEY).to_set
          }
        end || empty_snapshot
      end

      def redis_with
        return nil unless KafkaBatch.config.redis_configured?

        redis_pool.with { |r| yield r }
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch::ConsumptionControl] Redis error: #{e.message}")
        nil
      end

      def redis_pool
        @pool ||= ConnectionPool.new(size: 3, timeout: 1) do
          KafkaBatch::RedisClient.new(KafkaBatch.config, timeout: 1, reconnect_attempts: 0) ||
            raise(ConfigurationError, "Redis is not configured")
        end
      end
    end
  end
end
