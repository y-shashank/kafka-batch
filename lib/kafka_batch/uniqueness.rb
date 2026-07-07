# frozen_string_literal: true

require "xxhash"
require "redis"
require "connection_pool"
require "oj"

module KafkaBatch
  # Per-worker job uniqueness backed by Redis.
  #
  # Workers opt in with `uniq true`. A 128-bit digest (dual XXHash64) of
  # worker_class + canonical payload is stored as a 16-byte *binary* Redis key
  # suffix (not hex) to minimise RAM. The value is the owning job_id so release
  # is compare-and-delete safe after TTL races.
  #
  # Jobs carry `_uniq_fp` (hex) on the wire so release uses the same material
  # as claim even after JSON round-trip.
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
      # Pass +fp+ (_uniq_fp from the job message) when available.
      def release_by_name(worker_class_name, payload, job_id:, fp: nil)
        return unless KafkaBatch.config.uniq_enabled

        name = worker_class_name.to_s
        return if name.empty?

        # Fast path: new messages carry the fingerprint on the wire. Reconstruct
        # the exact key and release in a single round-trip — no class resolution,
        # no hashing. This is the steady-state path for every uniq job.
        if (key = redis_key_from_fp(fp))
          safe_release(key, job_id)
          return
        end

        # No fp on the message → either a non-uniq worker (nothing was ever
        # locked, so skip Redis entirely — an efficiency win over the old
        # unconditional release) or a job enqueued before _uniq_fp existed. Only
        # touch Redis when the worker actually opts into uniqueness.
        return unless uniq_worker?(name)

        safe_release(redis_key_for_name(name, payload), job_id)
        # Rolling-upgrade safety: also clear the legacy 64-bit (8-byte) key so a
        # lock claimed by a pre-128-bit version is never orphaned until its TTL.
        if (legacy = legacy_redis_key_for_name(name, payload))
          safe_release(legacy, job_id)
        end
      end

      # @return [String] 16 raw bytes (128-bit digest, not hex)
      def digest(worker_class, payload)
        fingerprint(worker_class.name, payload)
      end

      def reset!
        @pool&.shutdown(&:close) rescue nil
        @pool = nil
        @uniq_worker_cache = nil
      end

      # @return [String] 32-char hex digest (for _uniq_fp on job messages)
      def digest_hex(worker_class, payload)
        fingerprint(worker_class.name, payload).unpack1("H*")
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

      # Legacy pre-v0.2.2 lock key: single 64-bit XXHash64 packed as 8 raw bytes.
      # Only used on the fp-less release path to reclaim locks written by an older
      # version during a rolling upgrade. Returns nil on any hashing error.
      def legacy_redis_key_for_name(name, payload)
        material = "#{name}\0#{canonical_payload(payload)}"
        bin = [XXhash.xxh64(material) & 0xFFFF_FFFF_FFFF_FFFF].pack("Q")
        "#{KEY_PREFIX}#{bin}"
      rescue StandardError
        nil
      end

      # Whether a worker class opts into uniqueness, memoized per class name so the
      # fp-less release path never repeats a const lookup. Lock-free on hit (a
      # bounded set of worker classes); only synchronizes to fill a cold entry.
      def uniq_worker?(name)
        cache  = uniq_worker_cache
        cached = cache[name]
        return cached unless cached.nil? # false is a valid cached value

        klass = begin
          Object.const_get(name)
        rescue NameError
          nil
        end
        result = !!(klass.is_a?(Class) && klass.include?(Worker) &&
                    klass.respond_to?(:uniq?) && klass.uniq?)
        uniq_worker_cache_mutex.synchronize { cache[name] = result }
        result
      end

      def uniq_worker_cache
        @uniq_worker_cache ||= {}
      end

      def uniq_worker_cache_mutex
        @uniq_worker_cache_mutex ||= Mutex.new
      end

      def redis_key_from_fp(fp_hex)
        hex = fp_hex.to_s.strip
        return nil if hex.empty?

        bin = [hex].pack("H*")
        return nil unless bin.bytesize == 16

        "#{KEY_PREFIX}#{bin}"
      rescue ArgumentError
        nil
      end

      # Canonical material → dual XXHash64 → 16-byte little-endian binary (128-bit).
      def fingerprint(worker_class_name, payload)
        material = "#{worker_class_name}\0#{canonical_payload(payload)}"
        h1 = XXhash.xxh64(material) & 0xFFFF_FFFF_FFFF_FFFF
        h2 = XXhash.xxh64("#{material}\0uniq_salt_v1") & 0xFFFF_FFFF_FFFF_FFFF
        [h1, h2].pack("QQ")
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
