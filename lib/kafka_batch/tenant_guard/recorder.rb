# frozen_string_literal: true

require "connection_pool"
require "time"
require_relative "../redis_client"

module KafkaBatch
  module TenantGuard
    # Sliding per-tenant error-rate window. Increments minute-bucket counters in
    # Redis (fed from job.processed / job.failed / job.retried, fairness jobs
    # only) and reads them back as a windowed {ok, fail, retry} total for the
    # tenant_error_rate rule. Mirrors the alerts DLT-per-minute counter pattern
    # and the shared Redis contract documented in the README ("Tenant guard").
    #
    # Race-safe across Ruby + Go workers: HINCRBY on a per-(tenant, minute) hash
    # is atomic, so concurrent recorders in either runtime simply add up. This is
    # fire-and-forget — any Redis error is swallowed so the job hot path is never
    # affected.
    module Recorder
      ERROR_PREFIX = "kafka_batch:tenant_errors:"
      # ZSET of tenants seen recently, scored by last-seen epoch. Lets the
      # evaluator enumerate candidate tenants cheaply instead of SCANning.
      ACTIVE_KEY   = "kafka_batch:tenant_errors:active"
      # Extra TTL past the window so a bucket survives long enough to be summed
      # on the tick that reads it (clock skew + evaluator interval slack).
      SKEW_SECONDS = 120

      FIELDS = { ok: "ok", fail: "fail", retry: "retry" }.freeze

      class << self
        # outcome: :ok | :fail | :retry
        def record(tenant_id, outcome, at: Time.now, window_seconds: nil)
          tid = tenant_id.to_s
          return if tid.empty?

          field = FIELDS[outcome]
          return unless field

          ttl = window_ttl(window_seconds)
          key = bucket_key(tid, at)
          redis_with do |r|
            r.hincrby(key, field, 1)
            r.expire(key, ttl)
            r.zadd(ACTIVE_KEY, at.to_i, tid)
          end
          nil
        rescue StandardError => e
          safe_debug("record failed: #{e.class}: #{e.message}")
          nil
        end

        def record_ok(tenant_id, **kw);    record(tenant_id, :ok, **kw);    end
        def record_fail(tenant_id, **kw);  record(tenant_id, :fail, **kw);  end
        def record_retry(tenant_id, **kw); record(tenant_id, :retry, **kw); end

        # Summed counters across every minute bucket overlapping the window that
        # ends at `at`. Returns { ok:, fail:, retry: } (integers).
        def window_counts(tenant_id, window_seconds: nil, at: Time.now)
          tid = tenant_id.to_s
          return zero_counts if tid.empty?

          win = normalized_window(window_seconds)
          start_bucket = bucket_epoch(at.to_i - win)
          end_bucket   = bucket_epoch(at.to_i)

          totals = { "ok" => 0, "fail" => 0, "retry" => 0 }
          redis_with do |r|
            b = start_bucket
            while b <= end_bucket
              h = r.hgetall("#{ERROR_PREFIX}#{tid}:#{minute_stamp(b)}")
              if h && !h.empty?
                totals["ok"]    += h["ok"].to_i
                totals["fail"]  += h["fail"].to_i
                totals["retry"] += h["retry"].to_i
              end
              b += 60
            end
          end
          { ok: totals["ok"], fail: totals["fail"], retry: totals["retry"] }
        rescue StandardError => e
          safe_debug("window_counts failed: #{e.class}: #{e.message}")
          zero_counts
        end

        # Tenants with any recorded activity within `within_seconds`. Prunes
        # stale members while it reads so the index stays bounded.
        def active_tenants(within_seconds: nil, at: Time.now)
          win = normalized_window(within_seconds)
          floor = at.to_i - win
          redis_with do |r|
            r.zremrangebyscore(ACTIVE_KEY, 0, floor - SKEW_SECONDS)
            r.zrangebyscore(ACTIVE_KEY, floor, "+inf")
          end || []
        rescue StandardError => e
          safe_debug("active_tenants failed: #{e.class}: #{e.message}")
          []
        end

        # Error rate as a percentage: fail / (ok + fail) * 100, with retries
        # optionally folded into the failure count. Returns nil when there are
        # not enough samples to be meaningful.
        def error_rate(tenant_id, window_seconds: nil, min_samples: 0, include_retries: false, at: Time.now)
          c = window_counts(tenant_id, window_seconds: window_seconds, at: at)
          fails = c[:fail] + (include_retries ? c[:retry] : 0)
          denom = c[:ok] + fails
          return nil if denom < min_samples.to_i || denom.zero?

          {
            rate: (fails.to_f / denom * 100.0),
            ok: c[:ok], fail: c[:fail], retry: c[:retry],
            samples: denom
          }
        end

        def reset!
          @pool = nil
        end

        private

        def bucket_key(tenant_id, at)
          "#{ERROR_PREFIX}#{tenant_id}:#{minute_stamp(bucket_epoch(at.to_i))}"
        end

        def bucket_epoch(epoch)
          (epoch / 60) * 60
        end

        # yyyymmddHHmm in UTC — MUST match the Go recorder's bucket suffix.
        def minute_stamp(epoch)
          Time.at(epoch).utc.strftime("%Y%m%d%H%M")
        end

        def window_ttl(window_seconds)
          normalized_window(window_seconds) + SKEW_SECONDS
        end

        def normalized_window(window_seconds)
          win = (window_seconds || KafkaBatch.config.tenant_guard_window_seconds).to_i
          win < 60 ? 60 : win
        end

        def zero_counts
          { ok: 0, fail: 0, retry: 0 }
        end

        def safe_debug(msg)
          KafkaBatch.logger.debug("[KafkaBatch][TenantGuard::Recorder] #{msg}")
        rescue StandardError
          nil
        end

        def redis_with
          return nil unless KafkaBatch.config.redis_configured?

          pool.with { |r| yield r }
        end

        def pool
          @pool ||= ConnectionPool.new(size: 2, timeout: 3) do
            client = RedisClient.new(KafkaBatch.config)
            raise "Redis not configured" unless client

            client
          end
        end
      end
    end
  end
end
