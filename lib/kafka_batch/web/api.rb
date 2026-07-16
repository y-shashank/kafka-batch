# frozen_string_literal: true

require "cgi"
require "time"
require_relative "../system_info"
require_relative "../weight_shares"
require_relative "../reconciler/run_summary"
require_relative "../dlt/stats"
require_relative "../dlt/reader"

module KafkaBatch
  class Web
    # JSON API backing the React dashboard. Instantiated per request with shared
    # Web helpers (csrf, params, body parsing).
    class Api
      def initialize(web)
        @web = web
      end

      def call(method, path, params, env)
        if method == "GET" && path == "/api/bootstrap"
          bootstrap
        elsif method == "GET" && path == "/api/dashboard"
          dashboard
        elsif method == "GET" && path == "/api/batches"
          batches_index(params)
        elsif method == "GET" && (m = path.match(%r{\A/api/batches/([^/]+)\z}))
          batches_show(m[1], params)
        elsif method == "POST" && (m = path.match(%r{\A/api/batches/([^/]+)/cancel\z}))
          batches_cancel(m[1])
        elsif method == "DELETE" && (m = path.match(%r{\A/api/batches/([^/]+)\z}))
          batches_delete(m[1])
        elsif method == "POST" && path == "/api/batches/bulk"
          batches_bulk(env, params)
        elsif method == "GET" && path == "/api/failures"
          failures_index(params)
        elsif method == "GET" && path == "/api/live"
          live
        elsif method == "GET" && path == "/api/lag"
          lag(params)
        elsif method == "POST" && path == "/api/lag/pause"
          lag_control(:pause, params, env)
        elsif method == "POST" && path == "/api/lag/resume"
          lag_control(:resume, params, env)
        elsif method == "GET" && (m = path.match(%r{\A/api/fairness/(time|throughput)\z}))
          fairness(m[1].to_sym, params)
        elsif method == "GET" && (m = path.match(%r{\A/api/weights/(time|throughput)\z}))
          weights(m[1].to_sym)
        elsif %w[PUT POST].include?(method) && (m = path.match(%r{\A/api/weights/(time|throughput)\z}))
          weights_set(m[1].to_sym, params, env)
        elsif method == "DELETE" && (m = path.match(%r{\A/api/weights/(time|throughput)/([^/]+)\z}))
          weights_reset(m[1].to_sym, CGI.unescape(m[2]))
        elsif method == "GET" && path == "/api/scheduled"
          scheduled(params)
        elsif method == "GET" && path == "/api/system"
          system_info
        elsif method == "GET" && path == "/api/reconciler"
          reconciler
        elsif method == "GET" && path == "/api/dead_letter"
          dead_letter(params)
        elsif method == "GET" && path == "/api/audit"
          audit(params)
        else
          Json.error(404, "Not found")
        end
      end

      private

      def bootstrap
        fairness_types =
          if KafkaBatch.respond_to?(:active_fairness_types)
            Array(KafkaBatch.active_fairness_types).map(&:to_s)
          else
            %w[time throughput]
          end
        Json.ok(
          ok: true,
          csrf_token: @web.csrf_token,
          mount: @web.script_name,
          audit_enabled: defined?(KafkaBatch::AuditLog) && KafkaBatch::AuditLog.enabled?,
          fairness_types: fairness_types,
          version: KafkaBatch::VERSION
        )
      end

      def dashboard
        counts = @web.safe_counts
        Json.ok(
          ok: true,
          counts: counts,
          total: counts.values.sum,
          pending_jobs: @web.safe_pending_jobs,
          liveness: @web.safe_liveness_snapshot
        )
      end

      def batches_index(params)
        status = @web.valid_status(params["status"])
        search = @web.non_empty(params["q"])
        page   = [params["page"].to_i, 1].max
        offset = (page - 1) * Web::PER_PAGE
        batches = KafkaBatch.store.list_batches(status: status, limit: Web::PER_PAGE + 1, offset: offset, search: search)
        has_next = batches.size > Web::PER_PAGE
        batches = batches.first(Web::PER_PAGE)
        Json.ok(
          ok: true,
          page: page,
          has_next: has_next,
          status: status,
          q: search,
          batches: batches.map { |b| serialize_batch(b) }
        )
      end

      def batches_show(id, params)
        batch = KafkaBatch.store.find_batch(id)
        return Json.error(404, "Batch not found") unless batch

        f_page = [params["fp"].to_i, 1].max
        f_offset = (f_page - 1) * Web::PER_PAGE
        failures =
          begin
            KafkaBatch.store.list_failures(batch[:id], limit: Web::PER_PAGE + 1, offset: f_offset)
          rescue StandardError => e
            KafkaBatch.logger.warn("[KafkaBatch::Web] list_failures failed: #{e.message}")
            []
          end
        has_next = failures.size > Web::PER_PAGE
        failures = failures.first(Web::PER_PAGE)
        Json.ok(
          ok: true,
          batch: serialize_batch(batch, detail: true),
          failures: failures.map { |f| serialize_failure(f) },
          failures_page: f_page,
          failures_has_next: has_next
        )
      end

      def batches_cancel(id)
        batch = KafkaBatch.store.find_batch(id)
        return Json.error(404, "Batch not found") unless batch

        if batch[:status] == "running"
          KafkaBatch.store.update_batch_status(id, "cancelled")
          KafkaBatch::CancellationCache.add(id) if defined?(KafkaBatch::CancellationCache)
        end
        Json.ok(ok: true, id: id, status: KafkaBatch.store.find_batch(id)&.dig(:status) || "cancelled")
      end

      def batches_delete(id)
        return Json.error(404, "Batch not found") unless KafkaBatch.store.find_batch(id)

        KafkaBatch.store.delete_batch(id)
        Json.ok(ok: true, id: id, deleted: true)
      end

      def batches_bulk(env, query_params)
        body = @web.scalarize_params(@web.body_params_multi(env).merge(json_body(env)))
        ids = @web.bulk_batch_ids(body)
        action = @web.form_param(body, "bulk_action").to_s
        note = nil

        case action
        when "cancel"
          ids.each do |bid|
            KafkaBatch.store.update_batch_status(bid, "cancelled")
            KafkaBatch::CancellationCache.add(bid) if defined?(KafkaBatch::CancellationCache)
          end
        when "delete"
          ids.each { |bid| KafkaBatch.store.delete_batch(bid) }
        when "cancel_all"
          note = @web.bulk_cancel_all(
            status: @web.valid_status(@web.form_param(body, "scope_status")),
            search: @web.form_param(body, "scope_search")
          )
        when "delete_all"
          note = @web.bulk_delete_all(
            status: @web.valid_status(@web.form_param(body, "scope_status")),
            search: @web.form_param(body, "scope_search")
          )
        else
          return Json.error(400, "Unknown bulk_action")
        end

        Json.ok(ok: true, action: action, processed: ids.size, bulk_note: note)
      end

      def failures_index(params)
        status = @web.non_empty(params["status"])
        page = [params["page"].to_i, 1].max
        offset = (page - 1) * Web::PER_PAGE
        failures = KafkaBatch.store.list_all_failures(limit: Web::PER_PAGE + 1, offset: offset, status: status)
        has_next = failures.size > Web::PER_PAGE
        failures = failures.first(Web::PER_PAGE)
        Json.ok(
          ok: true,
          page: page,
          has_next: has_next,
          status: status,
          failures: failures.map { |f| serialize_failure(f) },
          retry_lag_by_tier: @web.retry_lag_by_tier,
          retry_lag_total: @web.retry_lag
        )
      end

      def live
        unless KafkaBatch::Liveness.available?
          msg =
            if KafkaBatch::Liveness.backend == :off
              "Live activity is disabled (config.liveness_backend = :off)."
            else
              "This feature requires Redis (config.redis_url) and it is not currently reachable."
            end
          return Json.ok(ok: true, available: false, backend: KafkaBatch::Liveness.backend, message: msg,
                         consumers: [], running_jobs: [])
        end

        consumers = KafkaBatch::Liveness.consumers
        jobs = KafkaBatch::Liveness.running_jobs
        Json.ok(
          ok: true,
          available: true,
          backend: KafkaBatch::Liveness.backend,
          stats_interval: KafkaBatch.config.liveness_stats_interval,
          consumers: consumers.map { |c| serialize_consumer(c) },
          running_jobs: jobs.map { |j| serialize_running_job(j) }
        )
      end

      def lag(params)
        tenant_q = @web.non_empty(params["tenant_id"])
        lookup = ingest_lookup(tenant_q, lag_rows: nil)

        unless KafkaBatch::Lag.available?
          return Json.ok(
            ok: true, available: false,
            message: "Lag requires Karafka's admin API (Karafka::Admin), which isn't available in this process.",
            tenant_lookup: lookup, topics: [], partitions: [], total: 0, groups: 0,
            control_available: KafkaBatch::ConsumptionControl.available?,
            pause_tooltip: @web.lag_pause_tooltip(KafkaBatch.config.consumption_control_refresh_interval)
          )
        end

        begin
          rows = KafkaBatch::Lag.partitions
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch::Web] lag fetch failed: #{e.message}")
          return Json.ok(
            ok: true, available: false,
            message: "Could not read lag from Kafka (see server logs for details).",
            tenant_lookup: ingest_lookup(tenant_q, lag_rows: nil), topics: [], partitions: [],
            total: 0, groups: 0,
            control_available: KafkaBatch::ConsumptionControl.available?,
            pause_tooltip: @web.lag_pause_tooltip(KafkaBatch.config.consumption_control_refresh_interval)
          )
        end

        paused = KafkaBatch::ConsumptionControl.available? ? KafkaBatch::ConsumptionControl.snapshot(refresh: true) : nil
        topics = KafkaBatch::Lag.topics(rows)
        Json.ok(
          ok: true,
          available: true,
          total: KafkaBatch::Lag.total(rows),
          groups: rows.map { |r| r[:group] }.uniq.size,
          topics: topics.map { |t| serialize_lag_topic(t, paused) },
          partitions: rows.map { |r| serialize_lag_partition(r, paused) },
          tenant_lookup: ingest_lookup(tenant_q, lag_rows: rows),
          control_available: !paused.nil?,
          control_backend: KafkaBatch::ConsumptionControl.available? ? KafkaBatch::ConsumptionControl.backend : nil,
          control_redis_url: KafkaBatch::ConsumptionControl.available? && KafkaBatch::ConsumptionControl.backend == :redis ?
            KafkaBatch::SystemInfo.mask_redis_url(KafkaBatch.config.redis_url) : nil,
          refresh_interval: KafkaBatch.config.consumption_control_refresh_interval,
          pause_tooltip: @web.lag_pause_tooltip(KafkaBatch.config.consumption_control_refresh_interval),
          schedule_consumer_group: "#{KafkaBatch.config.consumer_group}-schedule"
        )
      end

      def lag_control(action, params, env)
        body = json_body(env).merge(@web.body_params(env))
        merged = params.merge(body)
        unless KafkaBatch::ConsumptionControl.available?
          return Json.error(503, "Consumption control unavailable")
        end

        scope = merged["scope"].to_s
        group = merged["group"].to_s
        topic = merged["topic"].to_s
        part  = merged["partition"]

        ok =
          case [action, scope]
          when [:pause, "topic"]
            KafkaBatch::ConsumptionControl.pause_topic(group: group, topic: topic)
          when [:resume, "topic"]
            KafkaBatch::ConsumptionControl.resume_topic(group: group, topic: topic)
          when [:pause, "partition"]
            KafkaBatch::ConsumptionControl.pause_partition(group: group, topic: topic, partition: part.to_i)
          when [:resume, "partition"]
            KafkaBatch::ConsumptionControl.resume_partition(group: group, topic: topic, partition: part.to_i)
          else
            true
          end

        return Json.error(500, "control_write_failed") unless ok

        Json.ok(ok: true, action: action.to_s, scope: scope, group: group, topic: topic, partition: part)
      end

      def fairness(type, params)
        tenant_q = @web.non_empty(params["tenant_id"])
        active = KafkaBatch.respond_to?(:active_fairness_types) ? Array(KafkaBatch.active_fairness_types).map(&:to_sym) : []
        lane_active = active.empty? || active.include?(type)

        unless KafkaBatch::Lag.available?
          return Json.ok(
            ok: true, available: false, type: type.to_s, lane_active: lane_active,
            message: "This view needs Karafka's admin API (Karafka::Admin), which isn't available in this process.",
            tenant_lookup: ingest_lookup(tenant_q, lag_rows: nil, type: type),
            ingest: [], ready: [], ingest_total: 0, ready_total: 0, active_lanes: 0,
            throttled: false, global_concurrency: KafkaBatch.config.fairness_global_concurrency
          )
        end

        begin
          ingest_topic = KafkaBatch.config.fairness_ingest_topic(type)
          ingest_group = KafkaBatch.dispatch_consumer_group(type)
          ingest = @web.lag_partitions(ingest_group, ingest_topic)
          ready = @web.fairness_ready_lag_partitions(type)
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch::Web] fairness lag failed: #{e.message}")
          return Json.ok(
            ok: true, available: false, type: type.to_s, lane_active: lane_active,
            message: "Could not read lag from Kafka (see server logs for details).",
            tenant_lookup: ingest_lookup(tenant_q, lag_rows: nil, type: type),
            ingest: [], ready: [], ingest_total: 0, ready_total: 0, active_lanes: 0,
            throttled: false, global_concurrency: KafkaBatch.config.fairness_global_concurrency
          )
        end

        full_rows =
          begin
            KafkaBatch::Lag.partitions
          rescue StandardError
            nil
          end

        ingest_total = ingest.sum { |p| p[:lag].to_i }
        ready_total = ready.sum { |p| p[:lag].to_i }
        active_lanes = ingest.count { |p| p[:lag].to_i.positive? }
        concurrency = KafkaBatch.config.fairness_global_concurrency.to_i

        Json.ok(
          ok: true,
          available: true,
          type: type.to_s,
          lane_active: lane_active,
          ingest_topic: ingest_topic,
          ready_topics_description: @web.fairness_ready_topics_description_text(type),
          ingest: ingest,
          ready: ready,
          ingest_total: ingest_total,
          ready_total: ready_total,
          active_lanes: active_lanes,
          throttled: ready_total >= concurrency && concurrency.positive?,
          global_concurrency: concurrency,
          tenant_lookup: ingest_lookup(tenant_q, lag_rows: full_rows, type: type)
        )
      end

      def weights(type)
        sched = KafkaBatch.scheduler(type)
        unless sched
          return Json.ok(
            ok: true, available: false, type: type.to_s,
            message: "The Redis-backed Scheduler is not available in this process.",
            tenants: [], shares: [], default_weight: nil
          )
        end

        tenants =
          begin
            sched.all_tenants
          rescue StandardError => e
            KafkaBatch.logger.warn("[KafkaBatch::Web] weights: all_tenants failed: #{e.message}")
            []
          end
        total_tenants = tenants.size
        truncated = total_tenants > Web::WEIGHTS_MAX
        tenants = tenants.first(Web::WEIGHTS_MAX) if truncated
        shares = KafkaBatch::WeightShares.compute(tenants)
        cfg = KafkaBatch.config

        Json.ok(
          ok: true,
          available: true,
          type: type.to_s,
          default_weight: sched.default_weight,
          total_tenants: total_tenants,
          truncated: truncated,
          weights_max: Web::WEIGHTS_MAX,
          weighted_concurrency: cfg.fairness_weighted_concurrency,
          max_inflight_per_tenant: cfg.fairness_max_inflight_per_tenant,
          weight_cache_ttl: cfg.fairness_weight_cache_ttl,
          tenants: tenants,
          shares: shares.map { |s|
            fg, bg = @web.tenant_colors(s.tenant_id)
            {
              tenant_id: s.tenant_id,
              weight: s.weight,
              share_pct: s.share_pct,
              share_pct_label: KafkaBatch::WeightShares.format_pct(s.share_pct),
              color_fg: fg,
              color_bg: bg
            }
          }
        )
      end

      def weights_set(type, params, env)
        body = json_body(env).merge(@web.body_params(env))
        merged = params.merge(body)
        tid = @web.non_empty(merged["tenant_id"])
        return Json.error(400, "tenant_id required") if tid.nil?

        weight = merged["weight"].to_f
        weight = KafkaBatch.config.fairness_default_weight if weight <= 0
        begin
          KafkaBatch.scheduler(type)&.set_weight(tid, weight)
        rescue StandardError => e
          KafkaBatch.logger.error("[KafkaBatch::Web] weights_set(#{type}) failed: #{e.message}")
          return Json.error(500, e.message)
        end
        Json.ok(ok: true, tenant_id: tid, weight: weight, type: type.to_s)
      end

      def weights_reset(type, tenant_id)
        tid = @web.non_empty(tenant_id)
        return Json.error(400, "tenant_id required") if tid.nil?

        begin
          KafkaBatch.scheduler(type)&.delete_weight(tid)
        rescue StandardError => e
          KafkaBatch.logger.error("[KafkaBatch::Web] weights_reset(#{type}) failed: #{e.message}")
          return Json.error(500, e.message)
        end
        Json.ok(ok: true, tenant_id: tid, reset: true, type: type.to_s)
      end

      def scheduled(params)
        store = KafkaBatch.schedule_store
        unless store
          return Json.ok(ok: true, available: false, message: "The schedule store is not available in this process.",
                         jobs: [], size: 0)
        end

        query = @web.non_empty(params["q"])
        total = (store.size rescue 0)
        jobs =
          if query
            found = store.find(query)
            found ? [found] : []
          else
            store.list(limit: Web::PER_PAGE)
          end
        Json.ok(
          ok: true,
          available: true,
          size: total,
          backend: KafkaBatch.config.schedule_store,
          q: query,
          jobs: Array(jobs).map { |j| serialize_scheduled(j) }
        )
      end

      def system_info
        last = KafkaBatch::Reconciler::RunSummary.load_last
        sections = KafkaBatch::SystemInfo.sections.map do |section|
          {
            id: section.id,
            title: section.title,
            icon: section.icon,
            accent: section.accent,
            wide: section.wide,
            rows: section.rows.map { |r| { label: r.label, value: r.value, masked: r.masked } }
          }
        end
        Json.ok(
          ok: true,
          sections: sections,
          reconciler_last_ran_at: last && last[:ran_at],
          reconciler_age: last && @web.reconciler_age_label(last[:ran_at])
        )
      end

      def reconciler
        last = KafkaBatch::Reconciler::RunSummary.load_last
        skip = KafkaBatch::Reconciler::RunSummary.load_skip
        cfg = KafkaBatch.config
        details = Array(last&.dig(:details)).map do |d|
          {
            batch_id: d["batch_id"] || d[:batch_id],
            action: d["action"] || d[:action],
            outcome: d["outcome"] || d[:outcome],
            total_jobs: d["total_jobs"] || d[:total_jobs],
            failed_count: d["failed_count"] || d[:failed_count]
          }
        end
        Json.ok(
          ok: true,
          last: last,
          skip: skip,
          details: details,
          reconciliation_interval: cfg.reconciliation_interval,
          max_reconcile_per_run: cfg.max_reconcile_per_run,
          max_details: KafkaBatch::Reconciler::RunSummary::MAX_DETAILS
        )
      end

      def dead_letter(params)
        type = @web.non_empty(params["type"])
        before = @web.non_empty(params["before"])
        stats = @web.safe_dlt_stats
        page = @web.safe_dlt_page(type: type, before: before)
        unless stats
          return Json.ok(
            ok: true, available: false,
            message: "Could not read #{KafkaBatch.config.dead_letter_topic}. Ensure Kafka brokers are configured.",
            topic: KafkaBatch.config.dead_letter_topic,
            messages: [], has_older: false
          )
        end

        Json.ok(
          ok: true,
          available: true,
          stats: stats,
          type: type,
          messages: Array(page[:messages]).map { |m| serialize_dlt(m) },
          has_older: page[:has_older],
          cursor_older: page[:cursor_older]
        )
      end

      def audit(params)
        unless defined?(KafkaBatch::AuditLog) && KafkaBatch::AuditLog.enabled?
          return Json.ok(
            ok: true, enabled: false,
            message: "The Web UI audit log is disabled. Enable with config.audit_enabled = true.",
            entries: [], actions: []
          )
        end

        action = @web.non_empty(params["action"])
        page = [params["page"].to_i, 1].max
        offset = (page - 1) * Web::PER_PAGE
        rows = KafkaBatch::AuditLog.list(limit: Web::PER_PAGE + 1, offset: offset, action: action)
        has_next = rows.size > Web::PER_PAGE
        rows = rows.first(Web::PER_PAGE)
        Json.ok(
          ok: true,
          enabled: true,
          page: page,
          has_next: has_next,
          action: action,
          actions: KafkaBatch::AuditLog.actions,
          entries: rows.map { |r|
            r.merge(
              created_at_label: @web.fmt_time(r[:created_at]),
              metadata_preview: @web.audit_metadata_preview(r[:metadata])
            )
          }
        )
      end

      # ── serializers / helpers ─────────────────────────────────────────────

      def serialize_batch(b, detail: false)
        h = {
          id: b[:id],
          short_id: b[:id].to_s[0, 8],
          description: b[:description],
          status: b[:status],
          tenant_id: b[:tenant_id],
          total_jobs: b[:total_jobs].to_i,
          completed_count: b[:completed_count].to_i,
          failed_count: b[:failed_count].to_i,
          touched_count: b[:touched_count].to_i,
          pending: @web.pending(b),
          created_at: b[:created_at],
          created_at_label: @web.fmt_time(b[:created_at]),
          progress: progress_pct(b)
        }
        return h unless detail

        h.merge(
          on_success: b[:on_success],
          on_complete: b[:on_complete],
          finished_at: b[:finished_at],
          finished_at_label: @web.fmt_time(b[:finished_at]),
          callback_dispatched_at: b[:callback_dispatched_at],
          callback_dispatched_at_label: b[:callback_dispatched_at].to_s.empty? ? nil : @web.fmt_time(b[:callback_dispatched_at]),
          callback_dispatched_by: b[:callback_dispatched_by],
          meta: b[:meta]
        )
      end

      def progress_pct(b)
        total = b[:total_jobs].to_i
        return { done_pct: 0, fail_pct: 0 } if total.zero?

        {
          done_pct: (b[:completed_count].to_i * 100.0 / total).round(1),
          fail_pct: (b[:failed_count].to_i * 100.0 / total).round(1)
        }
      end

      def serialize_failure(f)
        {
          batch_id: f[:batch_id],
          job_id: f[:job_id],
          worker_class: f[:worker_class],
          status: f[:status],
          attempt: f[:attempt].to_i + 1,
          next_retry_at: f[:next_retry_at],
          next_retry_eta: (f[:status] == "retrying" && f[:next_retry_at]) ? @web.fmt_eta(f[:next_retry_at]) : nil,
          next_retry_at_label: f[:next_retry_at] ? @web.fmt_time(f[:next_retry_at]) : nil,
          error_class: f[:error_class],
          error_message: f[:error_message],
          failed_at: f[:failed_at],
          failed_at_label: @web.fmt_time(f[:failed_at])
        }
      end

      def serialize_consumer(c)
        {
          consumer_id: c[:consumer_id] || c["consumer_id"],
          hostname: c[:hostname] || c["hostname"],
          pid: c[:pid] || c["pid"],
          rss_bytes: c[:rss_bytes] || c["rss_bytes"],
          rss_label: @web.fmt_mem_text(c[:rss_bytes] || c["rss_bytes"]),
          cpu_pct: c[:cpu_pct] || c["cpu_pct"],
          topic: c[:topic] || c["topic"],
          last_seen: c[:last_seen] || c["last_seen"],
          last_seen_label: @web.fmt_time(c[:last_seen] || c["last_seen"])
        }
      end

      def serialize_running_job(j)
        {
          job_id: j[:job_id] || j["job_id"],
          batch_id: j[:batch_id] || j["batch_id"],
          worker_class: j[:worker_class] || j["worker_class"],
          consumer_id: j[:consumer_id] || j["consumer_id"],
          topic: j[:topic] || j["topic"],
          partition: j[:partition] || j["partition"],
          started_at: j[:started_at] || j["started_at"],
          started_at_label: @web.fmt_time(j[:started_at] || j["started_at"])
        }
      end

      def serialize_lag_topic(t, paused)
        archive = @web.schedule_log_group?(t[:group])
        {
          group: t[:group],
          topic: t[:topic],
          partitions: t[:partitions],
          lag: t[:lag].to_i,
          archive: archive,
          status: lag_topic_status_key(t[:group], t[:topic], paused, archive),
          can_control: !archive && !paused.nil?
        }
      end

      def serialize_lag_partition(r, paused)
        archive = r[:log_archive] || @web.schedule_log_group?(r[:group])
        {
          group: r[:group],
          topic: r[:topic],
          partition: r[:partition],
          committed: r[:never_consumed] ? nil : r[:committed],
          never_consumed: !!r[:never_consumed],
          end_offset: r[:end_offset],
          lag: r[:lag].to_i,
          archive: archive,
          status: lag_partition_status_key(r[:group], r[:topic], r[:partition], paused, archive),
          can_control: !archive && !paused.nil? && !KafkaBatch::ConsumptionControl.topic_paused?(paused, r[:group], r[:topic])
        }
      end

      def lag_topic_status_key(group, topic, paused, archive)
        return "payload_log" if archive
        return "unknown" unless paused
        KafkaBatch::ConsumptionControl.topic_paused?(paused, group, topic) ? "paused_topic" : "running"
      end

      def lag_partition_status_key(group, topic, partition, paused, archive)
        return "payload_log" if archive
        return "unknown" unless paused
        if KafkaBatch::ConsumptionControl.topic_paused?(paused, group, topic)
          "topic_paused"
        elsif KafkaBatch::ConsumptionControl.partition_only_paused?(paused, group, topic, partition)
          "paused"
        else
          "running"
        end
      end

      def serialize_scheduled(j)
        j = j.transform_keys(&:to_sym) if j.is_a?(Hash)
        {
          job_id: j[:job_id],
          partition: j[:partition],
          offset: j[:offset],
          run_at: j[:run_at],
          run_at_eta: @web.fmt_eta(j[:run_at]),
          run_at_label: @web.fmt_time(j[:run_at]),
          batch_id: j[:batch_id],
          pointer: "#{j[:job_id]}:#{j[:partition]}:#{j[:offset]}",
          state: j[:in_flight] || j[:inflight] ? "in-flight" : "pending"
        }
      end

      def serialize_dlt(m)
        err = [m[:error_class], m[:error_message]].compact.join(": ")
        err = "#{err[0, 120]}…" if err.length > 120
        {
          dlt_at: m[:dlt_at] || m[:timestamp],
          dlt_at_label: @web.fmt_time(m[:dlt_at] || m[:timestamp]),
          dlt_type: m[:dlt_type],
          worker_class: m[:worker_class] || m[:callback_class],
          batch_id: m[:batch_id],
          job_id: m[:job_id],
          source_topic: m[:source_topic],
          error: err,
          partition: m[:partition],
          offset: m[:offset]
        }
      end

      def ingest_lookup(tenant_id, lag_rows:, type: :time)
        topic = KafkaBatch.config.fairness_ingest_topic(type)
        return { tenant_id: nil, topic: topic } if tenant_id.nil?

        count = KafkaBatch.fairness_ingest_partition_count(type)
        unless count
          return { tenant_id: tenant_id, topic: topic, error: "Could not read partition count" }
        end

        configured = (KafkaBatch.config.fairness_tenant_partitions || {})[tenant_id.to_s]
        dynamic = KafkaBatch.config.fairness_dynamic_tenant_partitions
        resolved = KafkaBatch.tenant_ingest_partition(tenant_id, type)
        source = nil
        partition =
          if resolved
            source =
              if configured && configured.to_i == resolved
                "configured"
              elsif dynamic
                "dynamic"
              else
                "resolved"
              end
            resolved
          elsif configured && configured.to_i >= count
            source = "out_of_range"
            KafkaBatch::Partition.for_key(tenant_id, count)
          else
            source = dynamic ? "hash_fallback" : "hash"
            KafkaBatch::Partition.for_key(tenant_id, count)
          end

        lag_row = lag_rows&.find do |r|
          r[:topic] == topic && r[:partition].to_i == partition &&
            r[:group] == KafkaBatch.dispatch_consumer_group(type)
        end

        {
          tenant_id: tenant_id,
          topic: topic,
          partition: partition,
          partition_count: count,
          source: source,
          configured_partition: configured,
          lag: lag_row&.dig(:lag),
          never_consumed: lag_row&.dig(:never_consumed)
        }
      end

      def json_body(env)
        @web.json_body_params(env)
      end
    end
  end
end
