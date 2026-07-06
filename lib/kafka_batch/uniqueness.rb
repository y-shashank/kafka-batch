# frozen_string_literal: true

require "xxhash"
require "redis"
require "connection_pool"
require "oj"

module KafkaBatch
  # Per-worker job uniqueness backed by Redis.
  #
  # Workers opt in with `uniq true`. A 64-bit XXHash64 digest of
  # worker_class + canonical payload is stored as an 8-byte *binary* Redis key
  # suffix (not hex) to minimise RAM. The value is the owning job_id so release
  # is compare-and-delete safe after TTL races.
  #
  # A lock is claimed at enqueue (immediate or scheduled) and released when the
  # job reaches a terminal state (success, DLT, cancelled skip, scheduled drop).
  # Retries keep the lock — the logical job is still in flight.
  module Uniqueness
    KEY_PREFIX = "kafka_batch:uniq:"

    RELEASE_LUA = <<~LUA
      if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
      end
      return 0
    LUA

    class << self
      # @return [Boolean] true when the lock was acquired
      def claim(worker_class, payload, job_id:)
        return true unless applies_to?(worker_class)

        result = redis_with do |r|
          r.set(redis_key(worker_class, payload), job_id.to_s, nx: true, ex: ttl)
        end
        return true if result.nil? # Redis unavailable — fail open, skip dedup

        !!result
      end

      # Release the lock for a completed / dropped job. No-op when the worker is
      # not uniq-enabled or the key is owned by another job_id.
      def release(worker_class, payload, job_id:)
        return unless applies_to?(worker_class)

        safe_release(redis_key(worker_class, payload), job_id)
      end

      # Release by worker class name (consumer paths that may not resolve the Class).
      def release_by_name(worker_class_name, payload, job_id:)
        return unless KafkaBatch.config.uniq_enabled
        return if worker_class_name.nil? || worker_class_name.to_s.empty?

        safe_release(redis_key_for_name(worker_class_name.to_s, payload), job_id)
      end

      # @return [String] 8-byte binary digest (for tests / debugging)
      def digest(worker_class, payload)
        fingerprint(worker_class.name, payload)
      end

      def reset!
        @pool&.shutdown(&:close) rescue nil
        @pool = nil
      end

      # @return [String] 16-char hex digest (for tests / debugging / worker #uniq_hex)
      def digest_hex(worker_class, payload)
        digest(worker_class, payload).unpack1("H*")
      end

      def digest_hex_for_name(worker_class_name, payload)
        fingerprint(worker_class_name.to_s, payload).unpack1("H*")
      end

      private

      def applies_to?(worker_class)
        KafkaBatch.config.uniq_enabled &&
          worker_class.is_a?(Class) &&
          worker_class.include?(Worker) &&
          worker_class.uniq?
      end

      def ttl
        KafkaBatch.config.uniq_lock_ttl.to_i
      end

      def redis_key(worker_class, payload)
        "#{KEY_PREFIX}#{fingerprint(worker_class.name, payload)}"
      end

      def redis_key_for_name(name, payload)
        "#{KEY_PREFIX}#{fingerprint(name, payload)}"
      end

      # Canonical material → 64-bit XXHash64 → 8-byte little-endian binary.
      def fingerprint(worker_class_name, payload)
        material = "#{worker_class_name}\0#{canonical_payload(payload)}"
        [XXhash.xxh64(material) & 0xFFFF_FFFF_FFFF_FFFF].pack("Q")
      end

      def canonical_payload(payload)
        Oj.dump(deep_sort_keys(payload || {}), mode: :compat)
      end

      def deep_sort_keys(obj)
        case obj
        when Hash
          obj.sort_by { |k, _| k.to_s }.each_with_object({}) do |(k, v), h|
            h[k] = deep_sort_keys(v)
          end
        when Array
          obj.map { |e| deep_sort_keys(e) }
        else
          obj
        end
      end

      def safe_release(key, job_id)
        redis_with do |r|
          r.eval(RELEASE_LUA, keys: [key], argv: [job_id.to_s])
        end
      end

      def redis_with
        return nil unless KafkaBatch.config.redis_configured?

        redis_pool.with { |conn| yield conn }
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch::Uniqueness] Redis error: #{e.message}")
        nil
      end

      def redis_pool
        @pool ||= ConnectionPool.new(size: KafkaBatch.config.redis_pool_size, timeout: 1) do
          KafkaBatch::RedisClient.new(KafkaBatch.config, timeout: 1, reconnect_attempts: 0) ||
            raise(ConfigurationError, "Redis is not configured")
        end
      end
    end
  end
end
