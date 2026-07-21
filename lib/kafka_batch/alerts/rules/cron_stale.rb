# frozen_string_literal: true

require_relative "base"

module KafkaBatch
  module Alerts
    module Rules
      class CronStale < Base
        self.id = "cron_stale"
        self.title = "Recurring schedule stale"
        self.description =
          "An enabled cron schedule has been idle longer than stale_factor × its interval."
        self.detail =
          "The recurring ticker’s heartbeat marks enabled schedules stale when idle_seconds > " \
          "stale_threshold (default recurring_stale_factor=2.0 × cron interval) and emits cron.stale. " \
          "The alerts evaluator opens an incident per schedule name from Redis markers " \
          "(does not require recurring_scheduler_enabled on the UI/evaluator pod)."
        self.remediation =
          "Open /recurring; confirm ticker pods (rake kafka_batch:recurring:run or control with " \
          "recurring_scheduler_enabled); check MySQL ledger/leader lock and enqueue errors."
        self.default_severity = "warning"
        self.requires = []
        self.link = "/recurring"
        self.settings = []

        def evaluate(sample)
          Array(sample["cron_stale"]).map do |entry|
            schedule = entry["schedule"].to_s
            finding(
              fingerprint: "#{id}:#{schedule}",
              summary: "Recurring schedule #{schedule} stale for #{entry['stale_seconds']}s (job_type=#{entry['job_type']}).",
              sample: entry
            )
          end
        end
      end
    end
  end
end
