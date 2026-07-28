# frozen_string_literal: true

require "oj"
require_relative "state"

module KafkaBatch
  module Alerts
    # Collects one evaluation sample from existing Lag / RTT / liveness APIs.
    module Sampler
      module_function

      def collect(config)
        {
          "lag_topics" => lag_topics,
          "lag_baseline" => State.load_baseline,
          "paused_keys" => paused_topic_keys,
          "pending_total" => safe_pending_total,
          "live_consumers" => live_consumer_count,
          "rtt" => rtt_summary,
          "reconciler" => reconciler_summary,
          "fairness" => fairness_lanes,
          "schedule_pending" => schedule_zcard("kafka_batch:sched:pending"),
          "schedule_inflight" => schedule_zcard("kafka_batch:sched:inflight"),
          "dlt_per_minute" => State.dlt_count_last_minute,
          "cron_stale" => State.cron_stale_entries,
          "tenant_error_rates" => tenant_error_rates,
          "sampled_at" => Time.now.utc.iso8601
        }
      end

      def persist_baseline!(sample)
        baseline = {}
        Array(sample["lag_topics"]).each do |row|
          key = "#{row['group']}|#{row['topic']}"
          baseline[key] = {
            "committed" => row["committed_sum"],
            "end_sum" => row["end_sum"],
            "lag" => row["lag"]
          }
        end
        State.save_baseline!(baseline)
      end

      def lag_topics
        return [] unless defined?(KafkaBatch::Lag) && KafkaBatch::Lag.available?

        rows = KafkaBatch::Lag.partitions.reject { |r| r[:log_archive] }
        rows.group_by { |r| [r[:group], r[:topic]] }.map do |(group, topic), parts|
          committed_vals = parts.map { |p| p[:committed] }.compact
          end_vals = parts.map { |p| p[:end_offset] }.compact
          {
            "group" => group,
            "topic" => topic,
            "lag" => parts.sum { |p| p[:lag].to_i },
            "partitions" => parts.size,
            "committed_sum" => committed_vals.empty? ? nil : committed_vals.sum,
            "end_sum" => end_vals.empty? ? nil : end_vals.sum
          }
        end
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch][Alerts::Sampler] lag failed: #{e.message}")
        []
      end

      def safe_pending_total
        return 0 unless defined?(KafkaBatch::Lag) && KafkaBatch::Lag.available?

        KafkaBatch::Lag.pending_total.to_i
      rescue StandardError
        0
      end

      # Per-tenant error-rate rows for the tenant_error_rate_high rule. Empty
      # unless the guard is enabled; each row already meets min_samples so the
      # rule only compares against the rate threshold. One Redis read per active
      # tenant per tick.
      def tenant_error_rates
        return [] unless defined?(KafkaBatch::TenantGuard)

        KafkaBatch::TenantGuard.error_rate_samples
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch][Alerts::Sampler] tenant_error_rates failed: #{e.message}")
        []
      end

      def paused_topic_keys
        return [] unless defined?(KafkaBatch::ConsumptionControl)

        snap = KafkaBatch::ConsumptionControl.snapshot(refresh: true)
        topics = snap[:topics] || snap["topics"] || []
        topics.respond_to?(:to_a) ? topics.to_a.map(&:to_s) : Array(topics).map(&:to_s)
      rescue StandardError
        []
      end

      def live_consumer_count
        return 0 unless defined?(KafkaBatch::Liveness) && KafkaBatch::Liveness.available?

        KafkaBatch::Liveness.consumers.size
      rescue StandardError
        0
      end

      def rtt_summary
        return nil unless defined?(KafkaBatch::PerformanceMetrics) && KafkaBatch::PerformanceMetrics.enabled?

        data = KafkaBatch::PerformanceMetrics::Reader.new.fetch(range: "5m")
        rtt = data[:rtt] || data["rtt"]
        return nil unless rtt

        h = rtt.transform_keys(&:to_s)
        points = Array(data[:points] || data["points"])
        latest = points.last
        if latest
          h["latest_avg_ms"] = latest[:rtt_avg_ms] || latest["rtt_avg_ms"]
          h["latest_max_ms"] = latest[:rtt_max_ms] || latest["rtt_max_ms"]
        end
        errors = points.sum { |p| (p[:rtt_errors] || p["rtt_errors"]).to_i }
        h["errors"] = errors
        # Each non-empty RTT bucket implies ≥1 successful or failed probe.
        h["probes"] = [points.count { |p| (p[:rtt_avg_ms] || p["rtt_avg_ms"]).to_f.positive? || (p[:rtt_errors] || p["rtt_errors"]).to_i.positive? }, errors].max
        h["probes"] = 1 if h["probes"].to_i <= 0 && h["avg_ms"]
        h
      rescue StandardError => e
        KafkaBatch.logger.debug("[KafkaBatch][Alerts::Sampler] rtt failed: #{e.message}")
        nil
      end

      def reconciler_summary
        return nil unless defined?(KafkaBatch::Reconciler::RunSummary)

        last = KafkaBatch::Reconciler::RunSummary.load_last
        return nil unless last

        last.transform_keys(&:to_s)
      rescue StandardError
        nil
      end

      def fairness_lanes
        %w[time throughput].map do |lane|
          ingest = topic_lag_for(/fair_#{lane}_ingest/)
          ready = topic_lag_for(/fair_#{lane}_ready/)
          {
            "lane" => lane,
            "ingest_lag" => ingest,
            "ready_lag" => ready
          }
        end
      end

      def topic_lag_for(pattern)
        lag_topics.select { |t| t["topic"].to_s.match?(pattern) }.sum { |t| t["lag"].to_i }
      end

      def schedule_zcard(key)
        return 0 unless KafkaBatch.config.redis_configured?

        client = KafkaBatch::RedisClient.new(KafkaBatch.config)
        return 0 unless client

        client.zcard(key).to_i
      rescue StandardError
        0
      ensure
        client&.close rescue nil
      end
    end
  end
end
