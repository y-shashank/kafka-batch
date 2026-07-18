# frozen_string_literal: true

require "securerandom"
require_relative "../redis_client"

module KafkaBatch
  module Recurring
    # Best-effort distributed lease gating recurring ticks. Uses the SAME Redis
    # key and SET NX EX + token-checked release as the Go daemon
    # (pkg/cron/lock.go), so a Ruby ticker and a Go ticker coordinate: only one
    # holds the lease per window. It is an optimization only — correctness comes
    # from the (schedule_id, fire_at) ledger key — so a brief split-brain window
    # is harmless.
    class Lock
      KEY = "kafka_batch:cron:leader_lock"
      RELEASE_LUA = <<~LUA
        if redis.call('GET', KEYS[1]) == ARGV[1] then
          return redis.call('DEL', KEYS[1])
        end
        return 0
      LUA

      def initialize(ttl_seconds:, redis: nil)
        @ttl = ttl_seconds.to_i
        @ttl = 60 if @ttl < 1
        @redis = redis
      end

      # acquire returns a token on success, or nil when another node holds it.
      def acquire
        token = SecureRandom.hex(16)
        ok = redis.set(KEY, token, nx: true, ex: @ttl)
        ok ? token : nil
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Recurring] leader lock acquire failed: #{e.message}")
        nil
      end

      # release drops the lease only if this token still owns it.
      def release(token)
        return if token.nil?

        redis.eval(RELEASE_LUA, keys: [KEY], argv: [token])
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Recurring] leader lock release failed: #{e.message}")
      end

      private

      def redis
        @redis ||= (KafkaBatch::RedisClient.new(KafkaBatch.config) ||
          raise(ConfigurationError, "Redis is not configured (required for the recurring leader lock)"))
      end
    end
  end
end
