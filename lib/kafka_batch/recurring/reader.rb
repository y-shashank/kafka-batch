# frozen_string_literal: true

require "active_record"
require "time"
require_relative "../database_connection"

module KafkaBatch
  module Recurring
    # Named AR model for the recurring (cron) schedule definitions written by the
    # Go control plane (pkg/cron). Read-only from Ruby's perspective — the daemon
    # owns firing; the dashboard only surfaces state.
    class ScheduleRecord < ActiveRecord::Base
      self.table_name         = "kafka_batch_recurring_schedules"
      self.inheritance_column = nil
    end

    # Read side for the /kafka_batch dashboard. Lists schedules and computes a
    # health verdict mirroring the daemon's heartbeat: a schedule is "stale" when
    # it has been idle for longer than stale_factor × its own cron interval.
    module Reader
      module_function

      DEFAULT_STALE_FACTOR = 2.0

      # available? is false when the recurring table is absent (feature not
      # enabled / not migrated) so the UI can render an empty state gracefully.
      def available?
        model.table_exists?
      rescue StandardError
        false
      end

      def list(stale_factor: DEFAULT_STALE_FACTOR, now: Time.now.utc)
        model.order(:name).map { |r| serialize(r, stale_factor: stale_factor, now: now) }
      end

      def summary(stale_factor: DEFAULT_STALE_FACTOR, now: Time.now.utc)
        rows = list(stale_factor: stale_factor, now: now)
        {
          total: rows.size,
          enabled: rows.count { |r| r[:enabled] },
          stale: rows.count { |r| r[:stale] }
        }
      end

      def serialize(r, stale_factor:, now:)
        interval = interval_seconds(r.cron_expr, r.timezone)
        last_fire = r.last_fire_at&.utc
        reference = last_fire || r.next_run_at&.utc
        idle = reference ? (now - reference).to_f : nil
        threshold = interval ? interval * stale_factor : nil
        stale = r.enabled && threshold && idle && idle > threshold

        {
          id: r.id,
          name: r.name,
          cron: r.cron_expr,
          timezone: r.timezone,
          job_type: r.job_type,
          tenant_id: r.tenant_id,
          args: parse_args(r.args_json),
          enabled: !!r.enabled,
          misfire_policy: r.misfire_policy,
          next_run_at: r.next_run_at&.utc&.iso8601,
          last_fire_at: last_fire&.iso8601,
          interval_seconds: interval,
          idle_seconds: idle&.round,
          stale_threshold_seconds: threshold&.round,
          stale: !!stale,
          health: health_label(r.enabled, stale)
        }
      end

      def health_label(enabled, stale)
        return "paused" unless enabled
        stale ? "stale" : "ok"
      end

      # interval_seconds derives the schedule cadence from the gap between the
      # next two cron instants. Uses the fugit gem when present (Rails apps that
      # already depend on it); otherwise returns nil and staleness is not
      # computed (health falls back to "ok"/"paused").
      def interval_seconds(cron_expr, timezone)
        return nil if cron_expr.to_s.strip.empty?

        parser = cron_parser(cron_expr, timezone)
        return nil unless parser

        base = Time.now.utc
        a = parser.call(base)
        return nil unless a

        b = parser.call(a)
        return nil unless b

        secs = (b - a).to_i
        secs.positive? ? secs : nil
      rescue StandardError
        nil
      end

      # next_run_at computes the first fire strictly after `from` (UTC). Raises
      # ArgumentError on an unparseable/never-firing cron so register can 400.
      def next_run_at(cron_expr, timezone, from: Time.now.utc)
        parser = cron_parser(cron_expr, timezone)
        raise ArgumentError, "invalid cron expression #{cron_expr.inspect}" unless parser

        nt = parser.call(from)
        raise ArgumentError, "cron #{cron_expr.inspect} never fires" unless nt

        nt
      end

      # cron_parser returns a callable next(after)->Time, backed by fugit if it
      # is loadable. The schedule timezone is honoured by appending it to the
      # expression (fugit's trailing-tz form). Returns nil when unavailable.
      def cron_parser(cron_expr, timezone)
        require "fugit" unless defined?(Fugit)
        expr = cron_expr.to_s.strip
        return nil if expr.empty?

        tz = timezone.to_s.strip
        expr = "#{expr} #{tz}" unless tz.empty? || tz == "UTC"
        cron = Fugit::Cron.parse(expr)
        return nil unless cron

        lambda do |after|
          nt = cron.next_time(after.getutc)
          nt&.to_t&.utc
        end
      rescue LoadError, StandardError
        nil
      end

      def parse_args(raw)
        return {} if raw.nil?
        return raw if raw.is_a?(Hash)

        parsed = JSON.parse(raw)
        parsed.is_a?(Hash) ? parsed : {}
      rescue StandardError
        {}
      end

      def model
        @model ||= DatabaseConnection.bind(
          ScheduleRecord,
          connection: KafkaBatch.config.schedule_store_database_connection
        )
      end

      # reset! drops the memoized bound model (tests / reconfiguration).
      def reset!
        @model = nil
      end
    end
  end
end
