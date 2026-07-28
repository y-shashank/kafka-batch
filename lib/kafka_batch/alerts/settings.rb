# frozen_string_literal: true

require "connection_pool"
require "oj"
require "time"
require_relative "../redis_client"
require_relative "../ai/crypto"

module KafkaBatch
  module Alerts
    # Operator settings in Redis. Secrets encrypted with Ai::Crypto (ai_encryption_salt).
    # Library/env defaults apply until Redis fields are set; Redis wins when present.
    module Settings
      KEY = "kafka_batch:alerts:settings"
      VERSION_KEY = "kafka_batch:alerts:settings:version"

      SECRET_FIELDS = %w[
        slack_webhook_url
        webhook_urls
        email_smtp_password
      ].freeze

      BOOL_FIELDS = %w[
        enabled
        channel_slack
        channel_webhook
        channel_email
        channel_metrics
      ].freeze

      INT_FIELDS = %w[
        interval for_ticks resolve_ticks cooldown_seconds
        lag_threshold lag_growth_min reconciler_max_age
        schedule_pending_max dlt_per_minute
        fairness_ingest_lag fairness_ready_max_when_stuck
      ].freeze

      FLOAT_FIELDS = %w[
        rtt_avg_ms rtt_max_ms rtt_error_rate
      ].freeze

      RULE_IDS = %w[
        lag_stuck_growing redis_rtt_high no_live_consumers reconciler_stale
        fairness_ingest_backed_up dlt_rate_high schedule_depth_high cron_stale
        tenant_error_rate_high
      ].freeze

      class << self
        def version
          redis_with { |r| r.get(VERSION_KEY) }.to_s
        end

        def bump_version!
          redis_with { |r| r.incr(VERSION_KEY) }
        end

        # Masked view for API/UI.
        def show
          raw = load_raw
          secrets = SECRET_FIELDS.each_with_object({}) do |f, h|
            set = !raw["#{f}_ciphertext"].to_s.empty?
            h[f] = {
              "set" => set,
              "masked" => set ? mask_preview(raw["#{f}_preview"]) : nil
            }
          end
          {
            "encryption_configured" => Ai::Crypto.configured?,
            "version" => version,
            "enabled" => bool_field(raw, "enabled", KafkaBatch.config.alerts_enabled),
            "interval" => int_field(raw, "interval", KafkaBatch.config.alerts_interval),
            "for_ticks" => int_field(raw, "for_ticks", KafkaBatch.config.alerts_for_ticks),
            "resolve_ticks" => int_field(raw, "resolve_ticks", KafkaBatch.config.alerts_resolve_ticks),
            "cooldown_seconds" => int_field(raw, "cooldown_seconds", KafkaBatch.config.alerts_cooldown_seconds),
            "lag_threshold" => int_field(raw, "lag_threshold", KafkaBatch.config.alerts_lag_threshold),
            "lag_growth_min" => int_field(raw, "lag_growth_min", KafkaBatch.config.alerts_lag_growth_min),
            "rtt_avg_ms" => float_field(raw, "rtt_avg_ms", KafkaBatch.config.alerts_rtt_avg_ms),
            "rtt_max_ms" => float_field(raw, "rtt_max_ms", KafkaBatch.config.alerts_rtt_max_ms),
            "rtt_error_rate" => float_field(raw, "rtt_error_rate", KafkaBatch.config.alerts_rtt_error_rate),
            "reconciler_max_age" => int_field(raw, "reconciler_max_age", KafkaBatch.config.alerts_reconciler_max_age),
            "schedule_pending_max" => int_field(raw, "schedule_pending_max", KafkaBatch.config.alerts_schedule_pending_max),
            "dlt_per_minute" => int_field(raw, "dlt_per_minute", KafkaBatch.config.alerts_dlt_per_minute),
            "fairness_ingest_lag" => int_field(raw, "fairness_ingest_lag", KafkaBatch.config.alerts_fairness_ingest_lag),
            "fairness_ready_max_when_stuck" => int_field(raw, "fairness_ready_max_when_stuck", KafkaBatch.config.alerts_fairness_ready_max_when_stuck),
            "channel_slack" => bool_field(raw, "channel_slack", false),
            "channel_webhook" => bool_field(raw, "channel_webhook", false),
            "channel_email" => bool_field(raw, "channel_email", false),
            "channel_metrics" => bool_field(raw, "channel_metrics", true),
            "email_to" => raw["email_to"].to_s,
            "email_from" => raw["email_from"].to_s,
            "email_smtp_address" => raw["email_smtp_address"].to_s,
            "email_smtp_port" => raw["email_smtp_port"].to_s.empty? ? 587 : raw["email_smtp_port"].to_i,
            "email_smtp_user" => raw["email_smtp_user"].to_s,
            "rules" => parse_rules(raw["rules_json"]),
            "secrets" => secrets,
            "updated_at" => raw["updated_at"]
          }
        end

        # Effective config for the evaluator (includes decrypted secrets).
        def effective
          raw = load_raw
          cfg = KafkaBatch.config
          {
            "enabled" => bool_field(raw, "enabled", cfg.alerts_enabled),
            "interval" => int_field(raw, "interval", cfg.alerts_interval),
            "for_ticks" => int_field(raw, "for_ticks", cfg.alerts_for_ticks),
            "resolve_ticks" => int_field(raw, "resolve_ticks", cfg.alerts_resolve_ticks),
            "cooldown_seconds" => int_field(raw, "cooldown_seconds", cfg.alerts_cooldown_seconds),
            "lag_threshold" => int_field(raw, "lag_threshold", cfg.alerts_lag_threshold),
            "lag_growth_min" => int_field(raw, "lag_growth_min", cfg.alerts_lag_growth_min),
            "rtt_avg_ms" => float_field(raw, "rtt_avg_ms", cfg.alerts_rtt_avg_ms),
            "rtt_max_ms" => float_field(raw, "rtt_max_ms", cfg.alerts_rtt_max_ms),
            "rtt_error_rate" => float_field(raw, "rtt_error_rate", cfg.alerts_rtt_error_rate),
            "reconciler_max_age" => int_field(raw, "reconciler_max_age", cfg.alerts_reconciler_max_age),
            "schedule_pending_max" => int_field(raw, "schedule_pending_max", cfg.alerts_schedule_pending_max),
            "dlt_per_minute" => int_field(raw, "dlt_per_minute", cfg.alerts_dlt_per_minute),
            "fairness_ingest_lag" => int_field(raw, "fairness_ingest_lag", cfg.alerts_fairness_ingest_lag),
            "fairness_ready_max_when_stuck" => int_field(raw, "fairness_ready_max_when_stuck", cfg.alerts_fairness_ready_max_when_stuck),
            "channel_slack" => bool_field(raw, "channel_slack", false),
            "channel_webhook" => bool_field(raw, "channel_webhook", false),
            "channel_email" => bool_field(raw, "channel_email", false),
            "channel_metrics" => bool_field(raw, "channel_metrics", true),
            "slack_webhook_url" => decrypt_secret(raw, "slack_webhook_url"),
            "webhook_urls" => parse_webhook_urls(decrypt_secret(raw, "webhook_urls")),
            "email_to" => raw["email_to"].to_s,
            "email_from" => raw["email_from"].to_s,
            "email_smtp_address" => raw["email_smtp_address"].to_s,
            "email_smtp_port" => raw["email_smtp_port"].to_s.empty? ? 587 : raw["email_smtp_port"].to_i,
            "email_smtp_user" => raw["email_smtp_user"].to_s,
            "email_smtp_password" => decrypt_secret(raw, "email_smtp_password"),
            "rules" => parse_rules(raw["rules_json"])
          }
        end

        def update!(attrs)
          attrs = stringify_keys(attrs)
          fields = {}
          fields["updated_at"] = Time.now.utc.iso8601

          BOOL_FIELDS.each do |f|
            next unless attrs.key?(f)

            fields[f] = truthy?(attrs[f]) ? "1" : "0"
          end
          INT_FIELDS.each do |f|
            next unless attrs.key?(f)

            fields[f] = Integer(attrs[f]).to_s
          end
          FLOAT_FIELDS.each do |f|
            next unless attrs.key?(f)

            fields[f] = Float(attrs[f]).to_s
          end
          %w[email_to email_from email_smtp_address email_smtp_port email_smtp_user].each do |f|
            next unless attrs.key?(f)

            fields[f] = attrs[f].to_s
          end
          if attrs.key?("rules")
            fields["rules_json"] = Oj.dump(normalize_rules(attrs["rules"]), mode: :compat)
          end

          SECRET_FIELDS.each do |f|
            clear_key = "clear_#{f}"
            if truthy?(attrs[clear_key])
              fields["#{f}_ciphertext"] = ""
              fields["#{f}_preview"] = ""
            elsif attrs.key?(f) && !attrs[f].to_s.strip.empty?
              plain = attrs[f].to_s.strip
              fields["#{f}_ciphertext"] = Ai::Crypto.encrypt(plain)
              fields["#{f}_preview"] = plain[-4..] || plain
            end
          end

          redis_with do |r|
            r.hset(KEY, fields) unless fields.empty?
            r.incr(VERSION_KEY)
          end
          show
        end

        def clear_secret!(field)
          field = field.to_s
          raise ArgumentError, "unknown secret" unless SECRET_FIELDS.include?(field)

          redis_with do |r|
            r.hset(KEY, "#{field}_ciphertext" => "", "#{field}_preview" => "")
            r.incr(VERSION_KEY)
          end
          show
        end

        def reset_pool!
          @pool&.shutdown(&:close) rescue nil
          @pool = nil
        end

        private

        def load_raw
          redis_with { |r| r.hgetall(KEY) } || {}
        end

        def decrypt_secret(raw, field)
          blob = raw["#{field}_ciphertext"]
          return nil if blob.to_s.empty?
          return nil unless Ai::Crypto.configured?

          Ai::Crypto.decrypt(blob)
        end

        def parse_webhook_urls(raw)
          return [] if raw.nil? || raw.to_s.empty?

          if raw.start_with?("[")
            Array(Oj.load(raw)).map(&:to_s).reject(&:empty?)
          else
            raw.to_s.split(/[\n,]/).map(&:strip).reject(&:empty?)
          end
        rescue StandardError
          raw.to_s.split(/[\n,]/).map(&:strip).reject(&:empty?)
        end

        def parse_rules(json)
          return default_rules_hash if json.nil? || json.to_s.empty?

          h = Oj.load(json)
          h.is_a?(Hash) ? h : default_rules_hash
        rescue StandardError
          default_rules_hash
        end

        def default_rules_hash
          RULE_IDS.each_with_object({}) do |id, h|
            h[id] = { "enabled" => true, "severity" => "warning" }
          end
        end

        def normalize_rules(rules)
          base = default_rules_hash
          return base unless rules.is_a?(Hash)

          rules.each do |id, conf|
            next unless base.key?(id.to_s)

            c = conf.is_a?(Hash) ? conf : {}
            base[id.to_s] = {
              "enabled" => truthy?(c["enabled"].nil? ? c[:enabled] : c["enabled"]),
              "severity" => (c["severity"] || c[:severity] || "warning").to_s
            }
          end
          base
        end

        def bool_field(raw, key, default)
          return !!default unless raw.key?(key)

          truthy?(raw[key])
        end

        def int_field(raw, key, default)
          return default.to_i unless raw.key?(key) && !raw[key].to_s.empty?

          Integer(raw[key])
        rescue ArgumentError, TypeError
          default.to_i
        end

        def float_field(raw, key, default)
          return default.to_f unless raw.key?(key) && !raw[key].to_s.empty?

          Float(raw[key])
        rescue ArgumentError, TypeError
          default.to_f
        end

        def truthy?(v)
          v == true || %w[1 true yes on].include?(v.to_s.strip.downcase)
        end

        def mask_preview(last4)
          return nil if last4.nil? || last4.empty?

          "••••#{last4}"
        end

        def stringify_keys(obj)
          case obj
          when Hash
            obj.each_with_object({}) { |(k, v), h| h[k.to_s] = stringify_keys(v) }
          when Array
            obj.map { |v| stringify_keys(v) }
          else
            obj
          end
        end

        def redis_with
          return nil unless KafkaBatch.config.redis_configured?

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
