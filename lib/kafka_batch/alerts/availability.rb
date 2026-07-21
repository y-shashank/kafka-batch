# frozen_string_literal: true

require_relative "../ai/crypto"
require_relative "settings"
require_relative "rules"

module KafkaBatch
  module Alerts
    module Availability
      module_function

      def channels(effective = nil)
        eff = effective || Settings.effective
        enc = Ai::Crypto.configured?
        [
          channel(
            "slack",
            "Slack",
            eff["channel_slack"],
            enc && !eff["slack_webhook_url"].to_s.empty?,
            enc ? (eff["slack_webhook_url"].to_s.empty? ? "Add Slack webhook URL" : nil) : "Configure ai_encryption_salt to store secrets"
          ),
          channel(
            "webhook",
            "Webhook",
            eff["channel_webhook"],
            enc && Array(eff["webhook_urls"]).any?,
            enc ? (Array(eff["webhook_urls"]).empty? ? "Add at least one webhook URL" : nil) : "Configure ai_encryption_salt to store secrets"
          ),
          channel(
            "email",
            "Email",
            eff["channel_email"],
            enc && !eff["email_to"].to_s.empty? && !eff["email_smtp_address"].to_s.empty?,
            email_reason(enc, eff)
          ),
          channel(
            "metrics",
            "Metrics",
            eff["channel_metrics"],
            KafkaBatch.config.metrics_enabled,
            KafkaBatch.config.metrics_enabled ? nil : "Enable config.metrics_enabled"
          )
        ]
      end

      def rules(effective = nil)
        eff = effective || Settings.effective
        rule_conf = eff["rules"] || {}
        Rules.metadata.map do |meta|
          req = Array(meta["requires"])
          ok, reason = requirements_met?(req)
          conf = rule_conf[meta["id"]] || {}
          meta.merge(
            "enabled" => conf.key?("enabled") ? !!conf["enabled"] : true,
            "severity" => (conf["severity"] || meta["default_severity"]).to_s,
            "available" => ok,
            "unavailable_reason" => reason
          )
        end
      end

      def requirements_met?(requires)
        Array(requires).each do |req|
          case req.to_s
          when "performance_metrics"
            unless defined?(KafkaBatch::PerformanceMetrics) && KafkaBatch::PerformanceMetrics.enabled?
              return [false, "Enable performance_metrics_enabled"]
            end
          when "liveness"
            unless defined?(KafkaBatch::Liveness) && KafkaBatch::Liveness.available?
              return [false, "Liveness backend must be :redis"]
            end
          end
        end
        [true, nil]
      end

      def channel(id, label, enabled_flag, ready, reason)
        {
          "id" => id,
          "label" => label,
          "enabled" => !!enabled_flag,
          "available" => !!ready,
          "ready_to_send" => !!enabled_flag && !!ready,
          "unavailable_reason" => ready ? nil : reason
        }
      end

      def email_reason(enc, eff)
        return "Configure ai_encryption_salt to store secrets" unless enc
        return "Set email_to" if eff["email_to"].to_s.empty?
        return "Set SMTP address" if eff["email_smtp_address"].to_s.empty?

        nil
      end
    end
  end
end
