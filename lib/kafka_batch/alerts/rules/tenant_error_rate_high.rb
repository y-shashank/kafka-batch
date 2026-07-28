# frozen_string_literal: true

require_relative "base"

module KafkaBatch
  module Alerts
    module Rules
      # Fires one finding per tenant whose sliding-window error rate exceeds the
      # tenant-guard threshold. The per-tenant samples are collected by the
      # Sampler (from TenantGuard::Recorder) so this rule does no Redis I/O
      # itself. Fingerprint is per tenant (`tenant_error_rate:{id}`) so each
      # tenant opens/resolves independently under the existing hysteresis.
      #
      # Thresholds (window / min samples / rate / include-retries) are owned by
      # the Tenant guard settings page, NOT alerts settings — this rule only
      # controls whether a breach is *notified*. When the guard is disabled the
      # Sampler returns no rows, so the rule is silent regardless of its toggle.
      class TenantErrorRateHigh < Base
        self.id = "tenant_error_rate_high"
        self.title = "Tenant error rate high"
        self.description =
          "A tenant's sliding-window failure rate exceeds the tenant-guard threshold."
        self.detail =
          "Per-tenant ok/fail counts are recorded into kafka_batch:tenant_errors:* (fairness jobs " \
          "only) and summed over tenant_guard_window_seconds. Fires when fail/(ok+fail) ≥ " \
          "tenant_guard_error_rate_pct once at least tenant_guard_min_samples are seen. When the " \
          "guard's mitigation is enabled the tenant may also be throttled/paused automatically."
        self.remediation =
          "Open the Tenant guard page to see the tenant's rate and any auto action; investigate the " \
          "failing job_type for that tenant, then reset (resume/restore weight) once healthy."
        self.default_severity = "warning"
        self.requires = []
        self.link = "/tenant_guard"
        # Thresholds live on the Tenant guard page (kafka_batch:tenant_guard:settings),
        # not in alerts settings — so no alerts-managed settings here.
        self.settings = []

        def evaluate(sample)
          rows = Array(sample["tenant_error_rates"])
          return [] if rows.empty?

          threshold = tenant_error_rate_threshold
          rows.filter_map do |row|
            rate = row["rate"].to_f
            next if rate < threshold

            tid = row["tenant_id"].to_s
            next if tid.empty?

            finding(
              fingerprint: "tenant_error_rate:#{tid}",
              summary: "tenant=#{tid} error rate #{rate.round(1)}% over #{row['samples']} samples " \
                       "(threshold #{threshold}%).",
              sample: row.merge("threshold" => threshold),
              link: "/tenant_guard"
            )
          end
        end

        private

        def tenant_error_rate_threshold
          if defined?(KafkaBatch::TenantGuard) && KafkaBatch::TenantGuard.respond_to?(:effective_error_rate_pct)
            KafkaBatch::TenantGuard.effective_error_rate_pct
          else
            KafkaBatch.config.tenant_guard_error_rate_pct.to_f
          end
        end
      end
    end
  end
end
