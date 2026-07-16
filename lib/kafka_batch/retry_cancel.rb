# frozen_string_literal: true

require "connection_pool"
require "oj"

module KafkaBatch
  # Control plane for operator-deleted retries.
  #
  #   Cancel set  – job_ids pending skip (single / multi delete). Removed via
  #                 {#acknowledge!} once a consumer skips and commits the message.
  #   Skip map    – topic/partition → max offset to skip (delete-all watermark).
  #                 Cleared cancel set on delete-all; watermarks remain until TTL.
  module RetryCancel
    CANCEL_KEY = "kafka_batch:retry:cancel"
    SKIP_KEY   = "kafka_batch:retry:skip" # hash field "#{topic}:#{partition}"

    DEFAULT_TTL = 7 * 24 * 3600

    class << self
      # @param job_ids [Array<String>]
      # @return [Integer] number of ids added
      def cancel!(job_ids)
        ids = Array(job_ids).map { |j| j.to_s.strip }.reject(&:empty?).uniq
        return 0 if ids.empty?

        with_redis do |r|
          n = r.sadd(CANCEL_KEY, ids)
          r.expire(CANCEL_KEY, ttl)
          n.to_i
        end || 0
      end

      def cancelled?(job_id)
        return false if job_id.nil? || job_id.to_s.empty?

        with_redis { |r| r.sismember(CANCEL_KEY, job_id.to_s) } || false
      end

      # Remove job_id after it was skipped and the Kafka offset was committed.
      def acknowledge!(job_id)
        return if job_id.nil? || job_id.to_s.empty?

        with_redis { |r| r.srem(CANCEL_KEY, job_id.to_s) }
      end

      def clear_cancel_set!
        with_redis { |r| r.del(CANCEL_KEY) }
      end

      # @param watermarks [Hash] { "topic" => { partition_int => offset_int } }
      def set_skip_watermarks!(watermarks)
        flat = {}
        Array(watermarks).each do |topic, parts|
          next if topic.to_s.empty?

          Hash(parts).each do |partition, offset|
            next if offset.nil?

            flat["#{topic}:#{partition.to_i}"] = offset.to_i
          end
        end
        return if flat.empty?

        with_redis do |r|
          r.hset(SKIP_KEY, flat)
          r.expire(SKIP_KEY, ttl)
        end
      end

      # @return [Integer, nil]
      def skip_until(topic, partition)
        with_redis { |r| r.hget(SKIP_KEY, "#{topic}:#{partition.to_i}") }&.to_i
      rescue StandardError
        nil
      end

      # Full skip map for UI readers: { "topic:partition" => offset }
      def skip_map
        with_redis { |r| r.hgetall(SKIP_KEY) } || {}
      rescue StandardError
        {}
      end

      def should_skip?(topic:, partition:, offset:, job_id: nil)
        lim = skip_until(topic, partition)
        return true if lim && offset.to_i <= lim

        cancelled?(job_id)
      end

      def available?
        KafkaBatch.config.redis_configured?
      end

      def reset!
        @pool = nil
      end

      private

      def ttl
        n = KafkaBatch.config.respond_to?(:retry_cancel_ttl) ? KafkaBatch.config.retry_cancel_ttl.to_i : 0
        n = DEFAULT_TTL if n <= 0
        n
      end

      def with_redis
        return nil unless available?

        redis_pool.with { |r| yield r }
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch::RetryCancel] Redis error: #{e.message}")
        nil
      end

      def redis_pool
        @pool ||= ConnectionPool.new(size: 3, timeout: 2) do
          KafkaBatch::RedisClient.new(KafkaBatch.config, timeout: 2) ||
            raise(ConfigurationError, "Redis is not configured")
        end
      end
    end
  end
end
