# frozen_string_literal: true

require "connection_pool"
require "time"
require_relative "../redis_client"

module KafkaBatch
  module TenantGuard
    # Runtime-editable guard settings, layered over the static config defaults —
    # the same pattern as Alerts::Settings. Stored in a versioned Redis hash so a
    # change on the dashboard page takes effect everywhere without a redeploy.
    # `effective` is cached per-process (short TTL + version check) so the hot
    # `enabled?` path does not hit Redis on every job event.
    module Settings
      KEY         = "kafka_batch:tenant_guard:settings"
      VERSION_KEY = "kafka_batch:tenant_guard:settings:version"
      CACHE_TTL   = 5 # seconds

      BOOL_FIELDS  = %w[enabled include_retries dry_run].freeze
      INT_FIELDS   = %w[window_seconds min_samples grace_ticks reconcile_interval].freeze
      FLOAT_FIELDS = %w[error_rate_pct throttle_weight].freeze
      STR_FIELDS   = %w[mitigation].freeze
      # Nullable: an explicit empty value means "manual only" (nil), which is a
      # meaningful setting distinct from "unset / use default".
      NULLABLE_INT_FIELDS = %w[auto_release_seconds].freeze

      MITIGATIONS = %w[none throttle pause throttle_then_pause].freeze

      ALL_FIELDS = (BOOL_FIELDS + INT_FIELDS + FLOAT_FIELDS + STR_FIELDS + NULLABLE_INT_FIELDS).freeze

      class << self
        def available?
          KafkaBatch.config.redis_configured?
        end

        # Config-only defaults (no Redis read).
        def defaults
          c = KafkaBatch.config
          {
            "enabled"              => !!c.tenant_guard_enabled,
            "dry_run"              => !!c.tenant_guard_dry_run,
            "window_seconds"       => c.tenant_guard_window_seconds.to_i,
            "min_samples"          => c.tenant_guard_min_samples.to_i,
            "error_rate_pct"       => c.tenant_guard_error_rate_pct.to_f,
            "include_retries"      => !!c.tenant_guard_include_retries,
            "mitigation"           => c.tenant_guard_mitigation.to_s,
            "throttle_weight"      => c.tenant_guard_throttle_weight.to_f,
            "auto_release_seconds" => c.tenant_guard_auto_release_seconds, # Integer or nil
            "grace_ticks"          => c.tenant_guard_grace_ticks.to_i,
            "reconcile_interval"   => c.tenant_guard_reconcile_interval.to_i
          }
        end

        # Effective values = Redis overrides layered over config defaults.
        # Cached for CACHE_TTL and invalidated when the version key changes.
        def effective(refresh: false)
          if !refresh && @cache && (Time.now.to_f - @cache_at.to_f) < CACHE_TTL
            return @cache
          end

          merged = defaults
          if available?
            raw, ver = read_raw
            merged = apply_overrides(defaults, raw)
            @cache_version = ver
          end
          @cache = merged.freeze
          @cache_at = Time.now.to_f
          @cache
        rescue StandardError => e
          KafkaBatch.logger.debug("[KafkaBatch][TenantGuard::Settings] effective failed: #{e.message}") rescue nil
          @cache || defaults
        end

        # Partial update from the page. Validates + coerces, writes only the
        # provided keys, bumps the version, busts the cache. Returns effective.
        def update(partial)
          raise ArgumentError, "settings unavailable (no Redis)" unless available?

          flat = coerce_for_write(partial)
          redis_with do |r|
            r.mapped_hmset(KEY, flat) unless flat.empty?
            r.incr(VERSION_KEY)
          end
          @cache = nil
          effective(refresh: true)
        end

        def reset!
          @cache = nil
          @cache_at = nil
          @cache_version = nil
          @pool = nil
        end

        private

        def read_raw
          redis_with do |r|
            raw = r.hgetall(KEY) || {}
            ver = r.get(VERSION_KEY)
            [raw, ver]
          end || [{}, nil]
        end

        def apply_overrides(base, raw)
          out = base.dup
          raw.each do |k, v|
            key = k.to_s
            next unless ALL_FIELDS.include?(key)

            out[key] = coerce_read(key, v, base[key])
          end
          out
        end

        def coerce_read(key, value, default)
          case key
          when *BOOL_FIELDS then truthy?(value)
          when *INT_FIELDS then value.to_s.strip.empty? ? default : value.to_i
          when *FLOAT_FIELDS then value.to_s.strip.empty? ? default : value.to_f
          when *STR_FIELDS then normalize_mitigation(value, default)
          when *NULLABLE_INT_FIELDS then nullable_int(value)
          else default
          end
        end

        # Validate + coerce a partial update into string fields for HSET.
        def coerce_for_write(partial)
          flat = {}
          partial.each do |k, v|
            key = k.to_s
            next unless ALL_FIELDS.include?(key)

            flat[key] =
              case key
              when *BOOL_FIELDS then truthy?(v) ? "true" : "false"
              when *INT_FIELDS then Integer(v).to_s
              when *FLOAT_FIELDS
                f = Float(v)
                raise ArgumentError, "#{key} must be > 0" if key == "throttle_weight" && f <= 0

                f.to_s
              when *STR_FIELDS
                m = v.to_s.strip.downcase
                raise ArgumentError, "invalid mitigation: #{v}" unless MITIGATIONS.include?(m)

                m
              when *NULLABLE_INT_FIELDS
                # "" / nil / negative ⇒ manual only (stored as empty string).
                s = v.to_s.strip
                if s.empty?
                  ""
                else
                  n = Integer(s)
                  n.negative? ? "" : n.to_s
                end
              end
          end
          flat
        rescue ArgumentError, TypeError => e
          raise ArgumentError, "invalid tenant_guard setting: #{e.message}"
        end

        def normalize_mitigation(value, default)
          m = value.to_s.strip.downcase
          MITIGATIONS.include?(m) ? m : default.to_s
        end

        def nullable_int(value)
          s = value.to_s.strip
          return nil if s.empty?

          n = s.to_i
          n.negative? ? nil : n
        end

        def truthy?(v)
          %w[1 true yes on].include?(v.to_s.strip.downcase)
        end

        def redis_with
          return nil unless available?

          pool.with { |r| yield r }
        end

        def pool
          @pool ||= ConnectionPool.new(size: 1, timeout: 3) do
            client = RedisClient.new(KafkaBatch.config)
            raise "Redis not configured" unless client

            client
          end
        end
      end
    end
  end
end
