require "erb"
require "cgi"
require "securerandom"
require "time"
require_relative "system_info"
require_relative "weight_shares"
require_relative "reconciler/run_summary"
require_relative "dlt/stats"
require_relative "dlt/reader"

module KafkaBatch
  # A minimal, dependency-free Rack application for inspecting batches –
  # think "Sidekiq Web" but tiny. Mount it in your routes:
  #
  #   # config/routes.rb
  #   mount KafkaBatch::Web => "/kafka_batch"
  #
  # It works with whichever store is configured (MySQL or Redis). Because it
  # exposes destructive actions (cancel / delete), mount it behind your own
  # authentication (e.g. `authenticate :admin do ... end` or HTTP basic auth).
  class Web
    PER_PAGE = 25
    BULK_MAX = 100  # safety cap on batch_ids per bulk request
    BULK_ALL_MAX = 1000  # max batches per cancel_all / delete_all request
    MAX_BODY_BYTES = 1_048_576  # 1 MiB cap on POST body reads
    WEIGHTS_MAX = 500  # max tenant rows rendered on the /weights page
    # Bound the id-enumeration scan behind cancel_all / delete_all so a store with
    # millions of matching batches can't be fully loaded into memory. We only need
    # enough ids to fill one BULK_ALL_MAX page (+1 to detect that more remain).
    FILTER_SCAN_MAX = BULK_ALL_MAX + 1
    # Statuses the index/bulk filters accept; anything else is treated as "no filter".
    VALID_STATUSES = %w[running success complete cancelled pending].freeze

    # ── CSRF (double-submit cookie pattern) ───────────────────────────────────
    # A per-session token is set as a SameSite=Strict cookie on every response and
    # embedded in every mutating form (cancel, delete, lag/pause, lag/resume).
    # On POST requests the cookie must be present and the submitted token (from
    # the action query string) must match it. Cross-origin forged POSTs omit the
    # cookie (SameSite=Strict), so they are rejected even when the attacker
    # guesses a token in the URL. API clients must GET a page first to obtain
    # the cookie, then replay the token on mutating POSTs.
    CSRF_COOKIE = "_kb_csrf"
    CSRF_FIELD  = "_csrf"

    STATUS_COLORS = {
      "running"      => "#3b82f6",
      "success"      => "#10b981",
      "complete"     => "#f59e0b",
      "cancelled"    => "#6b7280",
      "pending"      => "#8b5cf6",
      "consumers"    => "#0ea5e9",
      "running_jobs" => "#6366f1"
    }.freeze

    # Deterministic colour pairs [text, background] for tenant_id chips.
    # The index is derived from the tenant_id string so the same tenant always
    # gets the same colour across page loads and between list/show pages.
    TENANT_COLORS = [
      ["#1d4ed8", "#dbeafe"],  # blue
      ["#6d28d9", "#ede9fe"],  # violet
      ["#be185d", "#fce7f3"],  # pink
      ["#b45309", "#fef3c7"],  # amber
      ["#047857", "#d1fae5"],  # emerald
      ["#0f766e", "#ccfbf1"],  # teal
      ["#4338ca", "#e0e7ff"],  # indigo
      ["#b91c1c", "#fee2e2"],  # red
      ["#7c3aed", "#f3e8ff"],  # purple
      ["#0369a1", "#e0f2fe"],  # sky
    ].freeze

    def self.call(env)
      new.call(env)
    end

    def call(env)
      @script_name = env["SCRIPT_NAME"].to_s
      method       = env["REQUEST_METHOD"]
      path         = env["PATH_INFO"].to_s
      path         = "/" if path.empty?
      @path        = path   # remembered so layout can tailor per-page live/reload behaviour
      @secure      = request_secure?(env)
      params       = parse_query(env["QUERY_STRING"])

      # ── Optional built-in authentication ──────────────────────────────────
      # The dashboard exposes destructive actions and configuration/dead-letter
      # payloads, so it MUST sit behind authentication. Best practice is still to
      # wrap the mount in the host app (`authenticate :admin do … end`). As a
      # defence-in-depth backstop the host may also set config.web_authenticator
      # to a callable(env) -> truthy/false; a falsey result is rejected here.
      unless web_authenticated?(env)
        return inject_csrf_cookie(unauthorized)
      end

      # ── CSRF: resolve token from cookie (or generate a fresh one) ──────────
      request_cookies = parse_cookies(env)
      @csrf_token     = request_cookies[CSRF_COOKIE] || SecureRandom.hex(16)

      # Validate CSRF on every mutating POST: cookie and submitted token must
      # both be present and equal. Absent cookie rejects cross-site forgeries
      # (the cookie is SameSite=Strict, so a cross-origin POST never carries it).
      #
      # The submitted token is read from the POST body's hidden _csrf field
      # first (so the secret never rides the URL and leaks via Referer/logs), and
      # falls back to the QUERY_STRING for API clients / older forms. Body reads
      # are rewind-safe (Rails wraps rack.input in RewindableInput) and rescue to
      # nil, so a non-rewindable environment simply relies on the query token.
      # The comparison is constant-time to avoid leaking the token via timing.
      if method == "POST"
        cookie_token = request_cookies[CSRF_COOKIE]
        submitted    = body_csrf_token(env) || params[CSRF_FIELD]
        unless cookie_token && submitted && secure_compare(submitted, cookie_token)
          return inject_csrf_cookie(csrf_forbidden)
        end
        @csrf_token = cookie_token
      end

      response =
        if method == "GET" && path == "/"
          html(render_index(params))
        elsif method == "GET" && path == "/failures"
          html(render_failures(params), title: "Failures")
        elsif method == "GET" && path == "/live"
          html(render_live, title: "Consumer Process")
        elsif method == "GET" && path == "/lag"
          html(render_lag(params), title: "Kafka Lag")
        elsif method == "POST" && path == "/lag/pause"
          lag_consumption_control(:pause, params)
        elsif method == "POST" && path == "/lag/resume"
          lag_consumption_control(:resume, params)
        elsif method == "GET" && (path == "/fairness" || path == "/fairness/time")
          html(render_fairness(params, type: :time), title: "Time Fairness")
        elsif method == "GET" && path == "/fairness/throughput"
          html(render_fairness(params, type: :throughput), title: "Throughput Fairness")
        elsif method == "GET" && path == "/scheduled"
          html(render_scheduled(params), title: "Scheduled")
        elsif method == "GET" && path == "/system"
          html(render_system, title: "System")
        elsif method == "GET" && path == "/reconciler"
          html(render_reconciler, title: "Reconciler")
        elsif method == "GET" && path == "/dead_letter"
          html(render_dead_letter(params), title: "Dead letter")
        elsif method == "GET" && path == "/audit"
          html(render_audit(params), title: "Audit log")
        elsif method == "GET" && (path == "/weights" || path == "/weights/time")
          html(render_weights(params, type: :time), title: "Time Fairness Weights")
        elsif method == "GET" && path == "/weights/throughput"
          html(render_weights(params, type: :throughput), title: "Throughput Fairness Weights")
        elsif method == "POST" && (path == "/weights" || path == "/weights/time")
          weights_set(params.merge(body_params(env)), type: :time)
        elsif method == "POST" && path == "/weights/throughput"
          weights_set(params.merge(body_params(env)), type: :throughput)
        elsif method == "POST" && (path == "/weights/reset" || path == "/weights/time/reset")
          weights_reset(params.merge(body_params(env)), type: :time)
        elsif method == "POST" && path == "/weights/throughput/reset"
          weights_reset(params.merge(body_params(env)), type: :throughput)
        elsif method == "GET" && (m = path.match(%r{\A/batches/([^/]+)\z}))
          batch = KafkaBatch.store.find_batch(m[1])
          batch ? html(render_show(batch, params)) : not_found
        elsif method == "POST" && path == "/batches/bulk"
          bulk_batches(env, params)
        elsif method == "POST" && (m = path.match(%r{\A/batches/([^/]+)/cancel\z}))
          # Inline cancel so web.rb works in UI-only mode (no Batch class loaded).
          # Mirrors KafkaBatch::Batch.cancel — but only for a batch that exists and
          # is still running, so a stray POST can't flip a completed/succeeded
          # batch to "cancelled" (matches the bulk cancel_all guard).
          batch = KafkaBatch.store.find_batch(m[1])
          if batch.nil?
            not_found
          else
            if batch[:status] == "running"
              KafkaBatch.store.update_batch_status(m[1], "cancelled")
              KafkaBatch::CancellationCache.add(m[1]) if defined?(KafkaBatch::CancellationCache)
            end
            redirect_to_index(params)
          end
        elsif method == "POST" && (m = path.match(%r{\A/batches/([^/]+)/delete\z}))
          if KafkaBatch.store.find_batch(m[1])
            KafkaBatch.store.delete_batch(m[1])
            redirect_to_index(params)
          else
            not_found
          end
        else
          not_found
        end

      audit_web_action!(env, path, params, response) if method == "POST"
      inject_csrf_cookie(response)
    rescue StandardError => e
      audit_web_action!(env, path, params, nil, error: e.message) if method == "POST"
      KafkaBatch.logger.error(
        "[KafkaBatch][Web] #{e.class}: #{e.message}\n#{e.backtrace&.first(8)&.join("\n")}"
      )
      [500, html_headers,
       [layout("Error", "<div class='card'><h2>500</h2><p>An internal error occurred. Check server logs.</p></div>")]]
    end

    private

    # ── CSRF helpers ───────────────────────────────────────────────────────

    def csrf_token
      @csrf_token
    end

    # Hidden form field carrying the CSRF token in the POST body (preferred over
    # the URL query string, which leaks the token via Referer headers and logs).
    def csrf_field
      %(<input type="hidden" name="#{CSRF_FIELD}" value="#{h(csrf_token)}">)
    end

    # Stamp the CSRF cookie onto any Rack response triple.
    #
    # Flags:
    #   SameSite=Strict — the browser never sends this cookie on a cross-origin
    #     request; this is the core protection of the double-submit pattern.
    #   HttpOnly — the token is server-rendered into forms, never read by JS, so
    #     the cookie can be HttpOnly to keep it out of reach of injected scripts.
    #   Secure — set on HTTPS requests so the token is never sent in clear text.
    def inject_csrf_cookie(response)
      status, headers, body = response
      headers = headers.dup
      path_scope = @script_name.empty? ? "/" : "#{@script_name}/"
      attrs = ["#{CSRF_COOKIE}=#{csrf_token}", "Path=#{path_scope}", "SameSite=Strict", "HttpOnly"]
      attrs << "Secure" if @secure
      cookie = attrs.join("; ")
      # Rack ≥ 2 accepts an Array of cookie strings; joining with "\n" is the
      # widely-supported fallback for a single header value.
      existing = headers["set-cookie"]
      headers["set-cookie"] =
        case existing
        when nil, "" then cookie
        when Array   then existing + [cookie]
        else "#{existing}\n#{cookie}"
        end
      [status, headers, body]
    end

    # True when the request arrived over TLS (directly or via a terminating
    # proxy that set X-Forwarded-Proto / X-Forwarded-Ssl).
    def request_secure?(env)
      return true if env["HTTPS"].to_s.casecmp?("on")
      return true if env["rack.url_scheme"].to_s.casecmp?("https")
      return true if env["HTTP_X_FORWARDED_PROTO"].to_s.split(",").first.to_s.strip.casecmp?("https")
      return true if env["HTTP_X_FORWARDED_SSL"].to_s.casecmp?("on")

      false
    end

    # Constant-time string comparison (Rack's if available, else a manual XOR
    # fold) so a mismatched CSRF token can't be recovered via response timing.
    def secure_compare(a, b)
      a = a.to_s
      b = b.to_s
      if defined?(Rack::Utils) && Rack::Utils.respond_to?(:secure_compare)
        return Rack::Utils.secure_compare(a, b)
      end

      return false unless a.bytesize == b.bytesize

      l = a.unpack("C*")
      r = 0
      b.each_byte.with_index { |byte, i| r |= byte ^ l[i] }
      r.zero?
    end

    # Read the CSRF token from the POST body's hidden _csrf field. Rewind-safe
    # and capped; any error (non-rewindable input, oversized body) yields nil so
    # the caller falls back to the query-string token.
    def body_csrf_token(env)
      input = env["rack.input"]
      return nil unless input

      input.rewind
      raw = input.read(MAX_BODY_BYTES + 1).to_s
      input.rewind
      return nil if raw.bytesize > MAX_BODY_BYTES

      parsed = CGI.parse(raw)[CSRF_FIELD]
      parsed.is_a?(Array) ? parsed.first : parsed
    rescue StandardError
      nil
    end

    # ── Optional authentication hook ──────────────────────────────────────────

    # Evaluate config.web_authenticator (if any). A nil authenticator means the
    # host is responsible for protecting the mount (documented behaviour). A
    # configured callable gates every request; a truthy return authorizes it.
    def web_authenticated?(env)
      auth = KafkaBatch.config.respond_to?(:web_authenticator) ? KafkaBatch.config.web_authenticator : nil
      return true if auth.nil?

      !!auth.call(env)
    rescue StandardError => e
      KafkaBatch.logger.error("[KafkaBatch][Web] web_authenticator raised #{e.class}: #{e.message} — denying request")
      false
    end

    def unauthorized
      [401,
       html_headers.merge("www-authenticate" => 'Basic realm="kafka-batch"'),
       [layout("Unauthorized",
         "<div class='card'><h2>401 Unauthorized</h2>" \
         "<p>Access to this dashboard is denied.</p></div>")]]
    end

    def csrf_forbidden
      [403, html_headers,
       [layout("Forbidden",
         "<div class='card'><h2>403 Forbidden</h2>" \
         "<p>CSRF check failed. Please go back, refresh the page, and try again.</p></div>")]]
    end

    # Parse cookies from the HTTP_COOKIE header into a plain Hash.
    def parse_cookies(env)
      env["HTTP_COOKIE"].to_s.split(";").each_with_object({}) do |pair, h|
        k, v = pair.strip.split("=", 2)
        h[k.strip] = v.to_s if k && !k.strip.empty?
      end
    end

    def audit_web_action!(env, path, params, response, error: nil)
      status =
        if error
          "error"
        elsif response && response[0].to_i >= 400
          "error"
        else
          "ok"
        end

      action = KafkaBatch::AuditLog.action_name_for(path)
      KafkaBatch::Instrumentation.web_action(
        action: action,
        path:   path,
        status: status,
        actor:  KafkaBatch::AuditLog.resolve_actor(env),
        error:  error
      )

      return unless defined?(KafkaBatch::AuditLog) && KafkaBatch::AuditLog.enabled?

      KafkaBatch::AuditLog.record_web_action(
        env:    env,
        path:   path,
        params: params,
        status: status,
        error:  error
      )
    end

    # ── Responses ──────────────────────────────────────────────────────────

    # Dashboard data is always live; never let a browser/proxy cache it (also
    # prevents Rails' Rack::ETag from issuing 304s that mask counter updates).
    def html_headers
      { "content-type" => "text/html; charset=utf-8", "cache-control" => "no-store" }
    end

    # Wrap an HTML body string in the layout; pass through ready-made responses.
    def html(body_or_response, title: "Batches")
      return body_or_response if body_or_response.is_a?(Array)
      [200, html_headers, [layout(title, body_or_response)]]
    end

    def not_found
      [404, html_headers, [layout("Not found", "<div class='card'><h2>404</h2><p>Not found.</p></div>")]]
    end

    def redirect_to_lag(tenant_id = nil, error: nil)
      qs = []
      qs << "tenant_id=#{url_encode(tenant_id)}" if tenant_id && !tenant_id.to_s.empty?
      qs << "error=#{url_encode(error)}" if error && !error.to_s.empty?
      suffix = qs.empty? ? "" : "?#{qs.join("&")}"
      [302, { "location" => "#{lag_path}#{suffix}", "cache-control" => "no-store", "content-type" => "text/html" }, []]
    end

    def redirect_to_index(params = {})
      flat = scalarize_params(params)
      qs = []
      if (s = form_param(flat, "return_status") || form_param(flat, "status"))
        qs << "status=#{url_encode(s)}"
      end
      if (q = form_param(flat, "return_q") || form_param(flat, "q"))
        qs << "q=#{url_encode(q)}"
      end
      if (note = form_param(flat, "bulk_note"))
        qs << "bulk_note=#{url_encode(note)}"
      end
      page = (flat["return_page"] || flat["page"]).to_i
      qs << "page=#{page}" if page > 1
      loc = qs.empty? ? index_path : "#{index_path}?#{qs.join("&")}"
      [302, { "location" => loc, "cache-control" => "no-store", "content-type" => "text/html" }, []]
    end

    # POST /batches/bulk — cancel or delete many batches selected on the index page.
    def bulk_batches(env, query_params)
      body   = scalarize_params(body_params_multi(env))
      ids    = bulk_batch_ids(body)
      action = form_param(body, "bulk_action").to_s

      case action
      when "cancel"
        ids.each do |id|
          KafkaBatch.store.update_batch_status(id, "cancelled")
          KafkaBatch::CancellationCache.add(id) if defined?(KafkaBatch::CancellationCache)
        end
      when "delete"
        ids.each { |id| KafkaBatch.store.delete_batch(id) }
      when "cancel_all"
        note = bulk_cancel_all(
          status: valid_status(form_param(body, "scope_status")),
          search: form_param(body, "scope_search")
        )
        body["bulk_note"] = note if note
      when "delete_all"
        note = bulk_delete_all(
          status: valid_status(form_param(body, "scope_status")),
          search: form_param(body, "scope_search")
        )
        body["bulk_note"] = note if note
      end

      redirect_to_index(query_params.merge(body))
    end

    def lag_consumption_control(action, params)
      unless KafkaBatch::ConsumptionControl.available?
        return redirect_to_lag(non_empty(params["tenant_id"]))
      end

      scope = params["scope"].to_s
      group = params["group"].to_s
      topic = params["topic"].to_s
      part  = params["partition"]

      case [action, scope]
      when [:pause, "topic"]
        ok = KafkaBatch::ConsumptionControl.pause_topic(group: group, topic: topic)
      when [:resume, "topic"]
        ok = KafkaBatch::ConsumptionControl.resume_topic(group: group, topic: topic)
      when [:pause, "partition"]
        ok = KafkaBatch::ConsumptionControl.pause_partition(group: group, topic: topic, partition: part.to_i)
      when [:resume, "partition"]
        ok = KafkaBatch::ConsumptionControl.resume_partition(group: group, topic: topic, partition: part.to_i)
      else
        ok = true
      end

      return redirect_to_lag(non_empty(params["tenant_id"]), error: "control_write_failed") unless ok

      redirect_to_lag(non_empty(params["tenant_id"]))
    end

    # ── Pages ──────────────────────────────────────────────────────────────

    # Delayed jobs (perform_in / perform_at) waiting in the schedule index.
    # Top box shows the pending counter; a search finds a job by id; the table
    # lists the next few "job_id:partition:offset" pointers to be processed.
    def render_scheduled(params)
      store = KafkaBatch.schedule_store
      unless store
        return <<~HTML
          <div class="card"><h2>Scheduled jobs</h2>
          <p class="muted">The schedule store is not available in this process.</p></div>
        HTML
      end

      query = non_empty(params["q"])
      total = (store.size rescue 0)

      # Top box: pending counter.
      metrics = <<~HTML
        <div class="metrics">
          <div class="metric"><div class="metric-value">#{total}</div><div class="metric-label">Pending scheduled</div></div>
          <div class="metric"><div class="metric-value">#{h(KafkaBatch.config.schedule_store)}</div><div class="metric-label">Backend</div></div>
        </div>
      HTML

      # Search by job_id.
      clear    = query ? "<a class='btn' href='#{scheduled_path}'>Clear</a>" : ""
      search   = <<~HTML
        <form class="search" method="get" action="#{scheduled_path}">
          <input type="text" name="q" value="#{h(query)}" placeholder="Search by job ID…" autocomplete="off">
          <button type="submit" class="btn">Search</button>#{clear}
        </form>
      HTML

      if query
        hit  = (store.find(query) rescue nil)
        rows = hit ? scheduled_row(hit) : "<tr><td colspan='4' class='empty'>No scheduled job matches #{h(query)}.</td></tr>"
        title = "Search result"
      else
        entries = (store.list(limit: PER_PAGE) rescue [])
        rows = entries.map { |e| scheduled_row(e.merge(state: :pending)) }.join
        rows = "<tr><td colspan='4' class='empty'>No scheduled jobs pending.</td></tr>" if entries.empty?
        title = "Next #{PER_PAGE} to be processed"
      end

      <<~HTML
        #{metrics}
        <div class="filterbar">#{search}</div>
        <div class="card">
          <h2>#{title}</h2>
          <table>
            <thead><tr><th>Pointer (job_id:partition:offset)</th><th>Location</th><th>Runs</th><th>State</th></tr></thead>
            <tbody>#{rows}</tbody>
          </table>
          <p class="muted">Payloads live in <code>#{h(KafkaBatch.config.scheduled_topic)}</code>; the poller re-produces each job onto its real topic when due.</p>
        </div>
      HTML
    end

    def scheduled_row(e)
      member = "#{e[:job_id]}:#{e[:partition]}:#{e[:offset]}"
      state  = e[:state] == :leased ? "<span class='badge' style='background:#f59e0b'>in-flight</span>" \
                                    : "<span class='badge' style='background:#3b82f6'>pending</span>"
      <<~ROW.gsub(/\n\s*/, "")
        <tr>
          <td class="mono">#{h(member)}</td>
          <td class="mono">#{h(KafkaBatch.config.scheduled_topic)}/#{h(e[:partition])}:#{h(e[:offset])}</td>
          <td>#{fmt_eta(e[:run_at])}</td>
          <td>#{state}</td>
        </tr>
      ROW
    end

    def render_system
      recon = reconciler_status_banner
      cards = KafkaBatch::SystemInfo.sections.map { |section| system_card(section) }.join

      <<~HTML
        #{recon}
        <p class="muted sys-lead">Read-only view of the active KafkaBatch configuration. Passwords and secrets are masked.</p>
        <div class="sys-grid">#{cards}</div>
      HTML
    end

    def render_reconciler
      last  = KafkaBatch::Reconciler::RunSummary.load_last
      skip  = KafkaBatch::Reconciler::RunSummary.load_skip
      cfg   = KafkaBatch.config

      metrics = if last
        <<~HTML
          <div class="metrics">
            <div class="metric"><div class="metric-value">#{h(fmt_time(last[:ran_at]))}</div><div class="metric-label">Last run</div></div>
            <div class="metric"><div class="metric-value">#{h(last[:triggered_by].to_s)}</div><div class="metric-label">Triggered by</div></div>
            <div class="metric"><div class="metric-value">#{h(last[:duration].to_s)}s</div><div class="metric-label">Duration</div></div>
            <div class="metric"><div class="metric-value">#{last[:recovered_stale].to_i}</div><div class="metric-label">Stuck batches recovered</div></div>
            <div class="metric"><div class="metric-value">#{last[:refired_lost].to_i}</div><div class="metric-label">Callbacks refired</div></div>
            <div class="metric"><div class="metric-value">#{last[:produce_failed].to_i}</div><div class="metric-label">Produce failures</div></div>
          </div>
        HTML
      else
        "<p class='muted'>No reconciler run recorded yet. Runs automatically from <code>EventConsumer</code> every " \
        "#{h(cfg.reconciliation_interval.to_s)}s or via <code>rake kafka_batch:reconcile</code>.</p>"
      end

      skip_note =
        if skip && skip[:at]
          "<p class='muted'>Last lock skip: #{h(fmt_time(skip[:at]))} (#{h(skip[:reason].to_s)}) — another process held the reconciler lock.</p>"
        else
          ""
        end

      summary_rows = ""
      if last
        summary_rows = <<~HTML
          <div class="card">
            <h2>Last sweep</h2>
            <table class="detail"><tbody>
              <tr><th>Stuck-running found / processed</th><td>#{last[:found_stale].to_i} / #{last[:processed_stale].to_i}#{last[:capped_stale].to_s == "1" ? " (capped)" : ""}</td></tr>
              <tr><th>Lost-callback found / processed</th><td>#{last[:found_lost].to_i} / #{last[:processed_lost].to_i}#{last[:capped_lost].to_s == "1" ? " (capped)" : ""}</td></tr>
              <tr><th>Skipped (open / in progress)</th><td>#{last[:skipped_stale].to_i}</td></tr>
              <tr><th>Interval</th><td>#{h(cfg.reconciliation_interval.to_s)}s</td></tr>
              <tr><th>Max per run</th><td>#{h(cfg.max_reconcile_per_run.to_s)}</td></tr>
            </tbody></table>
          </div>
        HTML
      end

      details = Array(last&.dig(:details))
      detail_rows = details.map do |d|
        bid = d["batch_id"] || d[:batch_id]
        action = d["action"] || d[:action]
        link = bid ? "<a href='#{show_path(bid)}'>#{h(bid[0, 8])}…</a>" : "—"
        <<~ROW.gsub(/\n\s*/, "")
          <tr>
            <td class="mono">#{link}</td>
            <td>#{h(action.to_s)}</td>
            <td>#{h((d["outcome"] || d[:outcome]).to_s)}</td>
            <td>#{h((d["total_jobs"] || d[:total_jobs]).to_s)}</td>
            <td>#{h((d["failed_count"] || d[:failed_count]).to_s)}</td>
          </tr>
        ROW
      end.join
      detail_rows = "<tr><td colspan='5' class='empty'>No per-batch actions on the last run.</td></tr>" if detail_rows.empty?

      <<~HTML
        #{metrics}
        #{skip_note}
        #{summary_rows}
        <div class="card">
          <h2>Last run detail</h2>
          <table>
            <thead><tr><th>Batch</th><th>Action</th><th>Outcome</th><th>Total</th><th>Failed</th></tr></thead>
            <tbody>#{detail_rows}</tbody>
          </table>
          <p class="muted">Shows up to #{KafkaBatch::Reconciler::RunSummary::MAX_DETAILS} batches from the most recent sweep. In-flight jobs are not preempted.</p>
        </div>
      HTML
    end

    def render_dead_letter(params)
      type   = non_empty(params["type"])
      before = non_empty(params["before"])
      stats  = safe_dlt_stats
      page   = safe_dlt_page(type: type, before: before)

      unless stats
        return <<~HTML
          <div class="card"><h2>Dead letter topic</h2>
          <p class="muted">Could not read <code>#{h(KafkaBatch.config.dead_letter_topic)}</code>. Ensure Kafka brokers are configured and reachable from this process.</p>
          <p class="muted">The <a href="#{failures_path}">Failures</a> page shows batch job failures from the store — not the same as the Kafka dead-letter topic.</p></div>
        HTML
      end

      type_note = stats[:sample_limited] ? " <span class='muted'>(type counts from recent sample of #{stats[:sample_size].to_i})</span>" : ""
      type_chips = dlt_type_chips(type, stats[:by_type] || {})

      metrics = <<~HTML
        <div class="metrics">
          <div class="metric"><div class="metric-value">#{stats[:total].to_i}</div><div class="metric-label">Messages in DLT</div></div>
          <div class="metric"><div class="metric-value">#{stats[:partitions].to_i}</div><div class="metric-label">Partitions</div></div>
          <div class="metric"><div class="metric-value mono" style="font-size:14px">#{h(stats[:topic].to_s)}</div><div class="metric-label">Topic</div></div>
        </div>
      HTML

      rows = page[:messages].map { |m| dlt_row(m) }.join
      rows = "<tr><td colspan='7' class='empty'>No dead-letter messages on this page.</td></tr>" if rows.empty?

      older_q = type ? "&type=#{CGI.escape(type)}" : ""
      older_link = page[:has_older] && page[:cursor_older] ?
        "<a class='btn' href='#{dead_letter_path}?before=#{CGI.escape(page[:cursor_older])}#{older_q}'>Older →</a>" : ""

      <<~HTML
        #{metrics}
        <p class="muted">Newest first. Type breakdown#{type_note}. See also <a href="#{failures_path}">Failures</a> (batch retry log in Redis/MySQL).</p>
        <div class="filterbar"><div class="chips">#{type_chips}</div></div>
        <div class="card">
          <h2>Dead letter messages</h2>
          <table>
            <thead><tr><th>When</th><th>Type</th><th>Worker / callback</th><th>Batch / job</th><th>Source</th><th>Error</th><th>Location</th></tr></thead>
            <tbody>#{rows}</tbody>
          </table>
          <div class="pager">#{older_link}</div>
        </div>
      HTML
    end

    def reconciler_status_banner
      last = KafkaBatch::Reconciler::RunSummary.load_last
      return "" unless last

      age = reconciler_age_label(last[:ran_at])
      <<~HTML
        <div class="card" style="margin-bottom:12px">
          <strong>Reconciler</strong> — last run #{h(age)} (#{h(last[:triggered_by].to_s)}):
          #{last[:recovered_stale].to_i} stuck recovered,
          #{last[:refired_lost].to_i} callbacks refired.
          <a href="#{reconciler_path}">Details →</a>
        </div>
      HTML
    rescue StandardError
      ""
    end

    def render_index(params)
      status   = valid_status(params["status"])
      search   = non_empty(params["q"])
      page     = [params["page"].to_i, 1].max
      offset   = (page - 1) * PER_PAGE
      counts   = safe_counts
      # Fetch one extra row to detect whether a next page exists.
      batches  = KafkaBatch.store.list_batches(status: status, limit: PER_PAGE + 1, offset: offset, search: search)
      has_next = batches.size > PER_PAGE
      batches  = batches.first(PER_PAGE)

      summary = summary_cards(counts, safe_pending_jobs, safe_liveness_snapshot)
      filters = status_filters(status, counts, search)
      rows    = batches.map { |b| batch_row(b, search: search) }.join
      empty   = search ? "“No batches match “#{h(search)}”.”" : "No batches found."
      rows    = "<tr><td colspan='11' class='empty'>#{empty}</td></tr>" if batches.empty?
      pager   = pagination(page, has_next, status, search)
      bulk    = bulk_batch_toolbar(status: status, search: search, page: page)
      bulk_all = bulk_all_toolbar(status: status, search: search, page: page)

      <<~HTML
        #{summary}
        <div class="filterbar">#{filters}#{search_box(search, status)}</div>
        <div class="card">
          <table>
            <thead>
              <tr>
                <th class="col-check"><input type="checkbox" id="kb-select-all" title="Select all on this page" aria-label="Select all on this page"></th>
                <th>Batch</th><th>Tenant</th><th>Status</th><th>Total</th><th>Done</th>
                <th>Failed</th><th>Pending</th><th>Progress</th><th>Created</th><th>Actions</th>
              </tr>
            </thead>
            <tbody>#{rows}</tbody>
          </table>
        </div>
        <div class="index-bottom">
          #{bulk}
          #{pager}
          #{bulk_all}
        </div>
        #{bulk_batch_script}
      HTML
    end

    # Client-side "Live" toggle: when on, reloads the current page every 5s (full
    # server render). The choice is persisted in localStorage so it survives
    # reloads and navigation, and a small countdown is shown on the button.
    # Reload keeps the current URL, so filters / search / pagination are preserved.
    def live_toggle_script
      <<~'HTML'
        <script>
        (function () {
          var KEY = "kafka_batch_live";
          var INTERVAL = 5;
          var btn = document.getElementById("kb-live-toggle");
          if (!btn) return;
          var timer = null, secs = INTERVAL;
          function isOn() { return localStorage.getItem(KEY) === "1"; }
          function paint() {
            if (isOn()) { btn.textContent = "● Live: " + secs + "s"; btn.classList.add("live-on"); }
            else { btn.textContent = "○ Live"; btn.classList.remove("live-on"); }
          }
          function stop() { if (timer) { clearInterval(timer); timer = null; } }
          function start() {
            stop(); secs = INTERVAL; paint();
            timer = setInterval(function () {
              secs -= 1;
              if (secs <= 0) { location.reload(); return; }
              paint();
            }, 1000);
          }
          btn.addEventListener("click", function () {
            if (isOn()) { localStorage.setItem(KEY, "0"); stop(); paint(); }
            else { localStorage.setItem(KEY, "1"); start(); }
          });
          paint();
          if (isOn()) start();
        })();
        </script>
      HTML
    end

    def render_failures(params)
      status   = non_empty(params["status"])
      page     = [params["page"].to_i, 1].max
      offset   = (page - 1) * PER_PAGE
      failures = KafkaBatch.store.list_all_failures(limit: PER_PAGE + 1, offset: offset, status: status)
      has_next = failures.size > PER_PAGE
      failures = failures.first(PER_PAGE)

      filter_links = [["Retrying", "retrying"], ["Failed", "failed"]].map do |label, s|
        cls  = status == s ? "chip active" : "chip"
        "<a class='#{cls}' href='#{failures_path}?status=#{s}'>#{label}</a>"
      end.join

      rows = failures.map do |f|
        color = f[:status] == "retrying" ? "#f59e0b" : "#ef4444"
        <<~ROW.gsub(/\n\s*/, "")
          <tr>
            <td><a class="mono" href="#{show_path(f[:batch_id])}">#{h(short_id(f[:batch_id]))}</a></td>
            <td class="mono">#{h(short_id(f[:job_id]))}</td>
            <td>#{h(f[:worker_class])}</td>
            <td><span class="badge" style="background:#{color}">#{h(f[:status])}</span></td>
            <td>#{f[:attempt].to_i + 1}</td>
            <td>#{next_retry_cell(f)}</td>
            <td class="danger">#{h(f[:error_class])}</td>
            <td>#{h(f[:error_message])}</td>
            <td>#{fmt_time(f[:failed_at])}</td>
          </tr>
        ROW
      end.join
      rows = "<tr><td colspan='9' class='empty'>No failures recorded.</td></tr>" if failures.empty?

      qs        = status ? "&status=#{status}" : ""
      prev_link = page > 1 ? "<a class='btn' href='#{failures_path}?page=#{page - 1}#{qs}'>← Prev</a>" : ""
      next_link = has_next ? "<a class='btn' href='#{failures_path}?page=#{page + 1}#{qs}'>Next →</a>" : ""
      pager     = (prev_link.empty? && next_link.empty?) ? "" : "<div class='pager'>#{prev_link}<span class='page'>Page #{page}</span>#{next_link}</div>"

      <<~HTML
        <p><a class="back" href="#{index_path}">← All batches</a></p>
        #{retry_lag_metric}
        <div class="chips">#{filter_links}</div>
        <div class="card">
          <h2>Failures across all batches</h2>
          <table>
            <thead><tr><th>Batch</th><th>Job</th><th>Worker</th><th>Status</th><th>Attempt</th><th>Next retry</th><th>Error</th><th>Message</th><th>Failed at</th></tr></thead>
            <tbody>#{rows}</tbody>
          </table>
        </div>
        #{pager}
      HTML
    end

    def render_audit(params)
      back = "<p><a class=\"back\" href=\"#{index_path}\">← All batches</a></p>"

      unless defined?(KafkaBatch::AuditLog) && KafkaBatch::AuditLog.enabled?
        return <<~HTML
          #{back}
          <div class="card">
            <h2>Audit log</h2>
            <p class="muted">The Web UI audit log is disabled. Enable it with
            <code>config.audit_enabled = true</code> (and run the audit migration —
            <code>rails generate kafka_batch:install --audit</code>).</p>
          </div>
        HTML
      end

      action = non_empty(params["action"])
      page   = [params["page"].to_i, 1].max
      offset = (page - 1) * PER_PAGE
      rows_data = KafkaBatch::AuditLog.list(limit: PER_PAGE + 1, offset: offset, action: action)
      has_next  = rows_data.size > PER_PAGE
      rows_data = rows_data.first(PER_PAGE)

      filter_links = ["<a class='#{action ? 'chip' : 'chip active'}' href='#{audit_path}'>All</a>"]
      KafkaBatch::AuditLog.actions.each do |a|
        cls = action == a ? "chip active" : "chip"
        filter_links << "<a class='#{cls}' href='#{audit_path}?action=#{url_encode(a)}'>#{h(a)}</a>"
      end

      rows = rows_data.map do |r|
        color = r[:status] == "ok" ? "#10b981" : "#ef4444"
        <<~ROW.gsub(/\n\s*/, "")
          <tr>
            <td>#{fmt_time(r[:created_at])}</td>
            <td>#{h(non_empty(r[:actor]) || "—")}</td>
            <td class="mono">#{h(r[:action])}</td>
            <td class="mono">#{h(r[:method])} #{h(r[:path])}</td>
            <td><span class="badge" style="background:#{color}">#{h(r[:status])}</span></td>
            <td class="mono">#{h(short_id(r[:node_id]))}</td>
            <td><code>#{h(audit_metadata_preview(r[:metadata]))}</code></td>
          </tr>
        ROW
      end.join
      rows = "<tr><td colspan='7' class='empty'>No audit entries recorded yet.</td></tr>" if rows_data.empty?

      qs        = action ? "&action=#{url_encode(action)}" : ""
      prev_link = page > 1 ? "<a class='btn' href='#{audit_path}?page=#{page - 1}#{qs}'>← Prev</a>" : ""
      next_link = has_next ? "<a class='btn' href='#{audit_path}?page=#{page + 1}#{qs}'>Next →</a>" : ""
      pager     = (prev_link.empty? && next_link.empty?) ? "" : "<div class='pager'>#{prev_link}<span class='page'>Page #{page}</span>#{next_link}</div>"

      <<~HTML
        #{back}
        <div class="chips">#{filter_links.join}</div>
        <div class="card">
          <h2>Web UI audit log</h2>
          <p class="muted">Mutating dashboard actions (cancel/delete, pause/resume, weight changes), newest first.</p>
          <table>
            <thead><tr><th>When (UTC)</th><th>Actor</th><th>Action</th><th>Request</th><th>Status</th><th>Node</th><th>Metadata</th></tr></thead>
            <tbody>#{rows}</tbody>
          </table>
        </div>
        #{pager}
      HTML
    end

    # Compact one-line preview of the metadata hash for the table cell.
    def audit_metadata_preview(metadata, limit: 160)
      return "—" if metadata.nil? || (metadata.respond_to?(:empty?) && metadata.empty?)

      str = metadata.is_a?(String) ? metadata : Oj.dump(metadata, mode: :compat)
      str.length > limit ? "#{str[0, limit]}…" : str
    rescue StandardError
      metadata.to_s
    end

    def render_live
      unless KafkaBatch::Liveness.available?
        msg = if KafkaBatch::Liveness.backend == :off
          "Live activity is disabled (<code>config.liveness_backend = :off</code>)."
        else
          "This feature requires Redis (<code>config.redis_url</code>) and it is not currently reachable, so running-job and consumer info is unavailable."
        end
        return <<~HTML
          <p><a class="back" href="#{index_path}">← All batches</a></p>

          <div class="card">
            <h2>Live activity</h2>
            <p class="muted">#{msg}</p>
          </div>
        HTML
      end

      consumers = KafkaBatch::Liveness.consumers
      jobs      = KafkaBatch::Liveness.running_jobs

      consumer_rows = consumers.map do |c|
        "<tr><td class='mono'>#{h(c['consumer_id'])}</td><td>#{h(c['hostname'])}</td>" \
        "<td>#{h(c['pid'])}</td><td>#{fmt_mem(c['rss_bytes'])}</td><td>#{fmt_cpu(c['cpu_pct'])}</td>" \
        "<td>#{h(c['topic'])}</td><td>#{fmt_time(c['last_seen'])}</td></tr>"
      end.join
      consumer_rows = "<tr><td colspan='7' class='empty'>No active consumers seen.</td></tr>" if consumers.empty?

      job_rows = jobs.map do |j|
        batch = j["batch_id"] ? "<a class='mono' href='#{show_path(j['batch_id'])}'>#{h(short_id(j['batch_id']))}</a>" : "<span class='muted'>—</span>"
        "<tr><td class='mono'>#{h(short_id(j['job_id']))}</td><td>#{batch}</td>" \
        "<td>#{h(j['worker_class'])}</td><td class='mono'>#{h(j['consumer_id'])}</td>" \
        "<td>#{h(j['topic'])}/#{h(j['partition'])}</td><td>#{fmt_time(j['started_at'])}</td></tr>"
      end.join
      job_rows = "<tr><td colspan='6' class='empty'>No jobs currently running.</td></tr>" if jobs.empty?

      <<~HTML
        <p><a class="back" href="#{index_path}">← All batches</a></p>

        <div class="metrics">
          <div class="metric"><div class="metric-value">#{consumers.size}</div><div class="metric-label">Consumers</div></div>
          <div class="metric"><div class="metric-value">#{jobs.size}</div><div class="metric-label">Running jobs</div></div>
        </div>
        <div class="card">
          <h3>Active consumers</h3>
          <table>
            <thead><tr><th>Consumer</th><th>Host</th><th>PID</th><th>RAM</th><th>CPU</th><th>Topic</th><th>Last seen</th></tr></thead>
            <tbody>#{consumer_rows}</tbody>
          </table>
        </div>
        <div class="card">
          <h3>Running jobs</h3>
          <p class="muted">Backend: <code>#{h(KafkaBatch::Liveness.backend)}</code>. RAM/CPU are sampled in each consumer process every <code>#{KafkaBatch.config.liveness_stats_interval}s</code> (process-level, approximate). Auto-refreshing every 5s.</p>
          <table>
            <thead><tr><th>Job</th><th>Batch</th><th>Worker</th><th>Consumer</th><th>Topic/Part</th><th>Started</th></tr></thead>
            <tbody>#{job_rows}</tbody>
          </table>
        </div>
      HTML
    end

    def render_lag(params = {})
      tenant_q = non_empty(params["tenant_id"])

      unless KafkaBatch::Lag.available?
        return <<~HTML
          <p><a class="back" href="#{index_path}">← All batches</a></p>

          #{ingest_partition_lookup_widget(tenant_q)}
          <div class="card">
            <h2>Topic lag</h2>
            <p class="muted">Lag requires Karafka's admin API (<code>Karafka::Admin</code>), which isn't available in this process.</p>
          </div>
        HTML
      end

      begin
        rows = KafkaBatch::Lag.partitions
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch::Web] lag fetch failed: #{e.message}")
        return <<~HTML
          <p><a class="back" href="#{index_path}">← All batches</a></p>

          #{ingest_partition_lookup_widget(tenant_q)}
          <div class="card">
            <h2>Topic lag</h2>
            <p class="muted">Could not read lag from Kafka (see server logs for details).</p>
          </div>
        HTML
      end

      lookup_html = ingest_partition_lookup_widget(tenant_q, lag_rows: rows)
      paused      = KafkaBatch::ConsumptionControl.available? ? KafkaBatch::ConsumptionControl.snapshot(refresh: true) : nil

      total   = KafkaBatch::Lag.total(rows)
      topics  = KafkaBatch::Lag.topics(rows)
      groups  = rows.map { |r| r[:group] }.uniq.size

      topic_rows = topics.map do |t|
        ctrl = lag_topic_control(t[:group], t[:topic], paused, tenant_q)
        status = lag_topic_status(t[:group], t[:topic], paused)
        "<tr><td class='mono'>#{h(t[:group])}</td><td class='mono'>#{h(t[:topic])}</td>" \
        "<td>#{t[:partitions]}</td><td>#{lag_badge(t[:lag])}</td><td>#{status}</td><td>#{ctrl}</td></tr>"
      end.join
      topic_rows = "<tr><td colspan='6' class='empty'>No consumed topics found.</td></tr>" if topics.empty?

      part_rows = rows.map do |r|
        committed = r[:never_consumed] ? "<span class='muted'>never consumed</span>" : r[:committed]
        endoff    = r[:end_offset].nil? ? "<span class='muted'>—</span>" : r[:end_offset]
        ctrl      = lag_partition_control(r[:group], r[:topic], r[:partition], paused, tenant_q)
        status    = lag_partition_status(r[:group], r[:topic], r[:partition], paused)
        "<tr><td class='mono'>#{h(r[:group])}</td><td class='mono'>#{h(r[:topic])}</td>" \
        "<td>#{r[:partition]}</td><td>#{committed}</td><td>#{endoff}</td><td>#{lag_badge(r[:lag])}</td>" \
        "<td>#{status}</td><td>#{ctrl}</td></tr>"
      end.join
      part_rows = "<tr><td colspan='8' class='empty'>No partitions found.</td></tr>" if rows.empty?

      pause_note =
        if paused
          refresh = KafkaBatch.config.consumption_control_refresh_interval
          tip = lag_pause_tooltip(refresh)
          case KafkaBatch::ConsumptionControl.backend
          when :redis
            "<p class='muted'>Pause/resume uses Redis (<code>#{h(KafkaBatch::SystemInfo.mask_redis_url(KafkaBatch.config.redis_url))}</code>). " \
            "<span title='#{h(tip)}' style='cursor:help;border-bottom:1px dotted #9ca3af'>Consumers pick up changes within ~#{refresh}s</span> " \
            "(<code>consumption_control_refresh_interval</code>).</p>"
          when :mysql
            "<p class='muted'>Pause/resume uses MySQL (<code>kafka_batch_consumption_pauses</code>). " \
            "<span title='#{h(tip)}' style='cursor:help;border-bottom:1px dotted #9ca3af'>Consumers pick up changes within ~#{refresh}s</span> " \
            "(<code>consumption_control_refresh_interval</code>).</p>"
          end
        else
          "<p class='muted'>Pause/resume requires Redis (<code>config.redis_url</code>) or MySQL " \
          "(<code>config.store = :mysql</code> plus the consumption_pauses migration).</p>"
        end

      <<~HTML
        <p><a class="back" href="#{index_path}">← All batches</a></p>

        #{lookup_html}
        <div class="metrics">
          <div class="metric"><div class="metric-value">#{total}</div><div class="metric-label">Total pending</div></div>
          <div class="metric"><div class="metric-value">#{topics.size}</div><div class="metric-label">Topics</div></div>
          <div class="metric"><div class="metric-value">#{groups}</div><div class="metric-label">Consumer groups</div></div>
        </div>
        <div class="card">
          <h3>Pending by topic</h3>
          <p class="muted">Lag = messages produced but not yet committed by the consumer group (i.e. pending work). Auto-refreshing every 5s.</p>
          #{pause_note}
          <table>
            <thead><tr><th>Group</th><th>Topic</th><th>Partitions</th><th>Pending (lag)</th><th>Status</th><th>Control</th></tr></thead>
            <tbody>#{topic_rows}</tbody>
          </table>
        </div>
        <div class="card">
          <h3>Pending by partition</h3>
          <table>
            <thead><tr><th>Group</th><th>Topic</th><th>Partition</th><th>Committed</th><th>End offset</th><th>Pending (lag)</th><th>Status</th><th>Control</th></tr></thead>
            <tbody>#{part_rows}</tbody>
          </table>
        </div>
      HTML
    end

    # Pending retries sitting on the retry topics (consumer lag), rendered on the
    # failures page as a total plus a per-tier breakdown (short/medium/large).
    # Returns "" if lag is unavailable.
    def retry_lag_metric
      by_tier = retry_lag_by_tier
      return "" if by_tier.nil?

      total = by_tier.values.sum
      cards = +%(<div class="metric"><div class="metric-value">#{lag_badge(total)}</div>) +
              %(<div class="metric-label">Pending retries (all tiers)</div></div>)
      by_tier.each do |tier, lag|
        cards << %(<div class="metric"><div class="metric-value">#{lag_badge(lag)}</div>) +
                 %(<div class="metric-label">#{tier} tier (#{tier_delay_label(tier)})</div></div>)
      end

      %(<div class="metrics">#{cards}</div>)
    end

    # Pending (lag) per retry tier topic: { short: N, medium: N, large: N }.
    # nil when lag introspection is unavailable.
    def retry_lag_by_tier
      return nil unless KafkaBatch::Lag.available?
      parts = KafkaBatch::Lag.partitions
      KafkaBatch.config.retry_tiers.keys.each_with_object({}) do |tier, h|
        topic  = KafkaBatch.config.retry_topic_for(tier)
        h[tier] = parts.select { |r| r[:topic] == topic }.sum { |r| r[:lag].to_i }
      end
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] retry_lag_by_tier failed: #{e.message}")
      nil
    end

    # Total lag across all retry tier topics (used by callers wanting one number).
    def retry_lag
      by_tier = retry_lag_by_tier
      by_tier&.values&.sum
    end

    # Human delay for a tier, e.g. "~30s" / "~7m".
    def tier_delay_label(tier)
      secs = KafkaBatch.config.retry_tiers[tier].to_i
      secs >= 60 ? "~#{secs / 60}m" : "~#{secs}s"
    end

    def render_fairness(params = {}, type: :time)
      lane_label = type == :throughput ? "Throughput Fairness" : "Time Fairness"
      tenant_q = non_empty(params["tenant_id"])
      back = "<p><a class=\"back\" href=\"#{index_path}\">← All batches</a></p>"

      inactive_notice =
        unless KafkaBatch.active_fairness_types.include?(type)
          "<div class='card'><p class='muted'>No registered workers use the <code>#{type}</code> fairness lane yet (set <code>fairness_type :#{type}</code> on a Worker class). Lag below reflects the lane's ingest/ready topics.</p></div>"
        end
      unless KafkaBatch::Lag.available?
        return "#{back}#{inactive_notice}#{ingest_partition_lookup_widget(tenant_q, action: fairness_path(type), type: type)}<div class='card'><h2>#{lane_label}</h2><p class='muted'>This view needs Karafka's admin API (<code>Karafka::Admin</code>), which isn't available in this process.</p></div>"
      end

      cfg            = KafkaBatch.config
      ingest_topic   = cfg.fairness_ingest_topic(type)
      ready_topic    = cfg.fairness_ready_topic(type)
      ingest, ready =
        begin
          [lag_partitions(KafkaBatch.dispatch_consumer_group(type), ingest_topic),
           lag_partitions(KafkaBatch.jobs_fair_consumer_group(type), ready_topic)]
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch::Web] fairness lag read failed: #{e.message}")
          return "#{back}#{inactive_notice}#{ingest_partition_lookup_widget(tenant_q, action: fairness_path(type), type: type)}<div class='card'><h2>#{lane_label}</h2><p class='muted'>Could not read lag from Kafka (see server logs for details).</p></div>"
        end

      # Augment ingest rows with :group and :topic so ingest_partition_lookup_result
      # can match against them (same shape as KafkaBatch::Lag.partitions rows).
      full_ingest_rows = ingest.map do |p|
        { group:     KafkaBatch.dispatch_consumer_group(type),
          topic:     ingest_topic,
          partition: p[:partition],
          lag:       p[:lag] }
      end
      lookup_html = ingest_partition_lookup_widget(tenant_q, lag_rows: full_ingest_rows, action: fairness_path(type), type: type)

      ingest_total = ingest.sum { |p| p[:lag] }
      ready_total  = ready.sum { |p| p[:lag] }
      lanes        = ingest.count { |p| p[:lag].positive? }
      # The Forwarder stops forwarding once the in-flight window is full, so the
      # ready buffer sitting at ~fairness_global_concurrency means it's throttling.
      window       = cfg.fairness_global_concurrency.to_i
      throttled    = window.positive? && ready_total >= window
      status_html  = throttled ? "<span class='badge' style='background:#ef4444'>Throttled</span>"
                               : "<span class='badge' style='background:#10b981'>Flowing</span>"

      ingest_rows = ingest.reject { |p| p[:lag].zero? }.map do |p|
        "<tr><td>#{p[:partition]}</td><td>#{lag_badge(p[:lag])}</td></tr>"
      end.join
      ingest_rows = "<tr><td colspan='2' class='empty'>No un-dispatched jobs — all lanes drained.</td></tr>" if ingest_rows.empty?

      ready_rows = ready.map do |p|
        "<tr><td>#{p[:partition]}</td><td>#{lag_badge(p[:lag])}</td></tr>"
      end.join
      ready_rows = "<tr><td colspan='2' class='empty'>Ready topic empty.</td></tr>" if ready_rows.empty?

      <<~HTML
        #{back}
        #{inactive_notice}
        #{lookup_html}
        <div class="metrics">
          <div class="metric"><div class="metric-value">#{lanes}</div><div class="metric-label">Active lanes (≈ tenants)</div></div>
          <div class="metric"><div class="metric-value">#{ingest_total}</div><div class="metric-label">Un-dispatched (ingest)</div></div>
          <div class="metric"><div class="metric-value">#{ready_total}</div><div class="metric-label">In buffer (ready)</div></div>
          <div class="metric"><div class="metric-value">#{status_html}</div><div class="metric-label">Dispatcher</div></div>
        </div>
        <div class="card">
          <h2>#{lane_label} <span class="muted" style="font-size:13px">(lane: <code>#{type}</code>)</span></h2>
          <p class="muted">Jobs land on <code>#{h(ingest_topic)}</code> (keyed one-tenant-per-partition), the Dispatcher stages them into the <code>#{type}</code> Redis WFQ scheduler, and the Forwarder checks out the fairest jobs onto <code>#{h(ready_topic)}</code> (bounded by the in-flight window <code>fairness_global_concurrency=#{window}</code>), which the JobConsumer swarm drains. Auto-refreshing every 5s.</p>
        </div>
        <div class="card">
          <h3>Ingest backlog by lane (un-dispatched, ≈ per tenant)</h3>
          <table>
            <thead><tr><th>Ingest partition</th><th>Waiting (lag)</th></tr></thead>
            <tbody>#{ingest_rows}</tbody>
          </table>
        </div>
        <div class="card">
          <h3>Ready buffer by partition (scheduled, awaiting execution)</h3>
          <table>
            <thead><tr><th>Ready partition</th><th>Depth (lag)</th></tr></thead>
            <tbody>#{ready_rows}</tbody>
          </table>
        </div>
      HTML
    end

    # ── Tenant weight management ───────────────────────────────────────────

    # GET /weights — shows all known tenants with their current weight and
    # runtime state (in-flight, queued, accumulated vtime). Tenants appear
    # automatically as soon as they enqueue their first job — no manual setup.
    def render_weights(_params = {}, type: :time)
      back = "<p><a class=\"back\" href=\"#{index_path}\">← All batches</a></p>"
      cfg  = KafkaBatch.config

      time_mode  = (type != :throughput)
      mode_label = time_mode ? "Time fairness" : "Throughput fairness"
      mode_badge_color = time_mode ? "#10b981" : "#3b82f6"
      mode_desc =
        if time_mode
          "Vtime advances at <strong>completion</strong> by <code>actual_seconds / weight</code>. " \
          "Each tenant receives proportional wall-clock time per hour (recommended for 20–60s jobs)."
        else
          "Vtime advances at <strong>dispatch</strong> by <code>1 / weight</code>. " \
          "Fair over job count, not duration. Correct when all tenants' jobs have similar runtimes."
        end

      sched = KafkaBatch.scheduler(type)

      unless sched
        return <<~HTML
          #{back}
          <div class="card">
            <h2>#{mode_label} — Tenant Weights</h2>
            <p class="muted">The Redis-backed Scheduler is not available in this process. Ensure
            <code>config.redis_url</code> is set and <code>kafka_batch/fairness/scheduler</code>
            is loaded.</p>
          </div>
        HTML
      end

      tenants = begin
        sched.all_tenants
      rescue => e
        KafkaBatch.logger.warn("[KafkaBatch::Web] weights: all_tenants failed: #{e.message}")
        []
      end

      default_w = sched.default_weight
      shares    = KafkaBatch::WeightShares.compute(tenants)
      share_by  = shares.to_h { |s| [s.tenant_id, s] }

      # Cap the number of tenant rows rendered so a lane with tens of thousands
      # of tenants can't produce an unbounded page (each row emits two forms).
      total_tenants = tenants.size
      truncated     = total_tenants > WEIGHTS_MAX
      tenants       = tenants.first(WEIGHTS_MAX) if truncated

      tenant_rows = tenants.map do |t|
        tid    = t[:tenant_id]
        w      = t[:weight]
        custom = t[:has_custom_weight]
        inf    = t[:inflight]
        queued = t[:queued]
        vtime  = t[:vtime]
        share  = share_by[tid]
        pct    = share ? KafkaBatch::WeightShares.format_pct(share.share_pct) : "—"
        fg, _bg = tenant_colors(tid)
        mini_bar = share ? weight_share_mini_bar(share.share_pct, fg) : ""

        custom_badge = custom \
          ? "<span class='badge' style='background:#6366f1'>custom</span>"
          : "<span class='muted'>default</span>"
        inf_cell   = inf.positive? ? "<strong>#{inf}</strong>" : inf.to_s
        queue_cell = queued \
          ? "<span class='badge' style='background:#10b981'>queued</span>"
          : "<span class='muted'>idle</span>"
        vtime_cell = vtime > 0 ? ("%.1f" % vtime) + "s" : "<span class='muted'>—</span>"

        set_form = <<~FORM.gsub(/\n\s*/, "")
          <form class="inline-form" method="POST" action="#{weights_path(type)}">
            #{csrf_field}
            <input type="hidden" name="tenant_id" value="#{h(tid)}">
            <input type="number" name="weight" value="#{w}" step="0.1" min="0.1" class="weight-input">
            <button type="submit" class="btn btn-sm">Set</button>
          </form>
        FORM

        reset_form =
          if custom
            <<~FORM.gsub(/\n\s*/, "")
              <form class="inline-form" method="POST" action="#{weights_reset_path(type)}">
                #{csrf_field}
                <input type="hidden" name="tenant_id" value="#{h(tid)}">
                <button type="submit" class="btn btn-sm">Reset</button>
              </form>
            FORM
          else
            ""
          end

        weight_display = "<strong>#{w}</strong>"
        share_cell = <<~CELL.gsub(/\n\s*/, "")
          <div class="weight-share-cell">
            <span class="weight-share-pct">#{pct}</span>
            #{mini_bar}
          </div>
        CELL

        <<~ROW.gsub(/\n\s*/, "")
          <tr>
            <td class="mono">#{h(tid)}</td>
            <td class="mono" style="text-align:right">#{weight_display}</td>
            <td style="text-align:right">#{share_cell}</td>
            <td>#{set_form}</td>
            <td>#{custom_badge}</td>
            <td>#{inf_cell}</td>
            <td>#{queue_cell}</td>
            <td class="mono">#{vtime_cell}</td>
            <td>#{reset_form}</td>
          </tr>
        ROW
      end.join

      tenant_rows = "<tr><td colspan='9' class='empty'>No active tenants yet. Tenants appear automatically as soon as they enqueue their first job.</td></tr>" if tenants.empty?

      add_form = <<~FORM
        <form class="weight-add-form" method="POST" action="#{weights_path(type)}">
          #{csrf_field}
          <input type="text" name="tenant_id" placeholder="Tenant ID" required class="weight-add-id">
          <input type="number" name="weight" value="#{default_w}" step="0.1" min="0.1" class="weight-input">
          <button type="submit" class="btn">Set weight</button>
        </form>
      FORM

      capacity_card = weight_share_distribution_card(shares, cfg)

      # Loud, actionable warning: without weighted concurrency, weights only bias
      # selection ORDER — under saturation every tenant gets an equal in-flight cap,
      # so throughput is ~equal regardless of weight. This is the #1 "my weights do
      # nothing" gotcha.
      weighted_warning =
        unless cfg.fairness_weighted_concurrency
          <<~HTML.gsub(/\n\s*/, " ")
            <div class="card" style="border-left:4px solid #f59e0b;background:#fffbeb">
              <strong>⚠ Weights only affect ordering right now.</strong>
              <code>config.fairness_weighted_concurrency</code> is <code>false</code>, so under load every
              active tenant gets an <em>equal</em> in-flight share (<code>ceil(global_concurrency / active)</code>)
              and throughput is roughly equal no matter the weight. Set
              <code>config.fairness_weighted_concurrency = true</code> to make weights control the
              throughput/concurrency split. (Weights still change selection order, most visibly in the
              throughput lane; they only change <em>totals</em> while tenants are contended, not once a
              finite backlog fully drains.)
            </div>
          HTML
        end

      <<~HTML
        #{back}
        #{weighted_warning}
        <div class="metrics">
          <div class="metric"><div class="metric-value">#{total_tenants}</div><div class="metric-label">Known tenants</div></div>
          <div class="metric"><div class="metric-value">#{tenants.count { |t| t[:has_custom_weight] }}</div><div class="metric-label">Custom weights</div></div>
          <div class="metric metric-info" title="Tenants with jobs currently checked out via Scheduler#checkout but not yet completed. Tracks active WFQ concurrency. Always 0 with the Kafka-native Dispatcher — only non-zero when driving the Scheduler directly with checkout/complete."><div class="metric-value">#{tenants.count { |t| t[:inflight].positive? }}</div><div class="metric-label">In-flight now ⓘ</div></div>
          <div class="metric metric-info" title="Tenants currently in the WFQ ring — they have jobs in their Scheduler ready queue awaiting checkout. Always 0 with the Kafka-native Dispatcher — only non-zero when using the Scheduler-based WFQ dispatch path."><div class="metric-value">#{tenants.count { |t| t[:queued] }}</div><div class="metric-label">Queued ⓘ</div></div>
        </div>
        #{capacity_card}
        <div class="card">
          <p>
            <span class="badge" style="background:#{mode_badge_color}">#{mode_label}</span>
            &nbsp;#{mode_desc}
            &nbsp;<span class="muted">Default weight: <code>#{default_w}</code> &nbsp;|&nbsp; Per-tenant cap: <code>#{cfg.fairness_max_inflight_per_tenant == 0 ? "none" : cfg.fairness_max_inflight_per_tenant}</code> &nbsp;|&nbsp; Cache TTL: <code>#{cfg.fairness_weight_cache_ttl}s</code></span>
          </p>
        </div>
        <div class="card">
          <h3>Tenant weights</h3>
          #{truncated ? "<p class='muted' style='color:#b45309'>Showing the first #{WEIGHTS_MAX} of #{total_tenants} tenants. Use the search on the fairness page to look up a specific tenant.</p>" : ""}
          <p class="muted">
            Higher weight = proportionally more throughput.
            <strong>Capacity share</strong> normalizes all tenant weights to 100% — raising one tenant's weight
            increases its share and reduces others proportionally.
            Weights are persisted in Redis and propagate to all dispatcher processes within the cache TTL
            (<code>#{cfg.fairness_weight_cache_ttl}s</code>). New tenants appear here automatically.
          </p>
          <table>
            <thead>
              <tr>
                <th>Tenant ID</th>
                <th style="text-align:right">Weight</th>
                <th style="text-align:right" title="Share of total configured weight (always sums to 100%)">Capacity share ⓘ</th>
                <th>Set Weight</th>
                <th>Override</th>
                <th title="Jobs currently checked out from the WFQ Scheduler but not yet completed. Always 0 with the Kafka-native Dispatcher.">In-flight ⓘ</th>
                <th title="Whether this tenant has jobs in its Scheduler ready queue (queued = in the WFQ ring). Always idle with the Kafka-native Dispatcher.">Status ⓘ</th>
                <th>Vtime (slot-s)</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>#{tenant_rows}</tbody>
          </table>
        </div>
        <div class="card">
          <h3>Add / pre-configure tenant weight</h3>
          <p class="muted">Pre-set a weight for a tenant before it appears, or override an existing one.</p>
          #{add_form}
        </div>
      HTML
    end

    # POST /weights/<type> — set a tenant weight for a lane, then redirect back.
    def weights_set(params, type: :time)
      tid    = non_empty(params["tenant_id"])
      weight = params["weight"].to_f
      loc    = weights_path(type)

      if tid.nil?
        return [302, { "location" => loc, "cache-control" => "no-store", "content-type" => "text/html" }, []]
      end

      if weight <= 0
        weight = KafkaBatch.config.fairness_default_weight
      end

      begin
        KafkaBatch.scheduler(type)&.set_weight(tid, weight)
      rescue => e
        KafkaBatch.logger.error("[KafkaBatch::Web] weights_set(#{type}) failed: #{e.message}")
      end

      [302, { "location" => loc, "cache-control" => "no-store", "content-type" => "text/html" }, []]
    end

    # POST /weights/<type>/reset — remove a custom weight override for a lane.
    def weights_reset(params, type: :time)
      tid = non_empty(params["tenant_id"])
      begin
        KafkaBatch.scheduler(type)&.delete_weight(tid) if tid
      rescue => e
        KafkaBatch.logger.error("[KafkaBatch::Web] weights_reset(#{type}) failed: #{e.message}")
      end
      [302, { "location" => weights_path(type), "cache-control" => "no-store", "content-type" => "text/html" }, []]
    end

    def lag_topic_status(group, topic, paused)
      return "<span class='muted'>—</span>" unless paused
      if KafkaBatch::ConsumptionControl.topic_paused?(paused, group, topic)
        "<span class='badge' style='background:#ef4444'>Paused (topic)</span>"
      else
        "<span class='badge' style='background:#10b981'>Running</span>"
      end
    end

    def lag_partition_status(group, topic, partition, paused)
      return "<span class='muted'>—</span>" unless paused
      if KafkaBatch::ConsumptionControl.topic_paused?(paused, group, topic)
        "<span class='badge' style='background:#f59e0b'>Topic paused</span>"
      elsif KafkaBatch::ConsumptionControl.partition_only_paused?(paused, group, topic, partition)
        "<span class='badge' style='background:#ef4444'>Paused</span>"
      else
        "<span class='badge' style='background:#10b981'>Running</span>"
      end
    end

    def lag_topic_control(group, topic, paused, tenant_q)
      return "<span class='muted'>—</span>" unless paused

      lag_control_form(
        action: KafkaBatch::ConsumptionControl.topic_paused?(paused, group, topic) ? :resume : :pause,
        scope:  "topic",
        group:  group,
        topic:  topic,
        tenant_q: tenant_q
      )
    end

    def lag_partition_control(group, topic, partition, paused, tenant_q)
      return "<span class='muted'>—</span>" unless paused
      return "<span class='muted'>topic</span>" if KafkaBatch::ConsumptionControl.topic_paused?(paused, group, topic)

      lag_control_form(
        action: KafkaBatch::ConsumptionControl.partition_only_paused?(paused, group, topic, partition) ? :resume : :pause,
        scope:  "partition",
        group:  group,
        topic:  topic,
        partition: partition,
        tenant_q: tenant_q
      )
    end

    def lag_control_form(action:, scope:, group:, topic:, tenant_q:, partition: nil)
      verb = action == :pause ? "Pause" : "Resume"
      qs   = lag_control_query(scope: scope, group: group, topic: topic, partition: partition, tenant_q: tenant_q)
      path = action == :pause ? "#{lag_path}/pause" : "#{lag_path}/resume"
      cls  = action == :pause ? "btn btn-sm danger-btn" : "btn btn-sm"
      tip  = h(lag_pause_tooltip(KafkaBatch.config.consumption_control_refresh_interval))
      # #23: lag forms POST operational params (group/topic/partition) via the
      # query string; the CSRF token rides a hidden body field so the secret never
      # appears in the URL (Referer/log leak). body_csrf_token reads it back.
      %(<form class="inline-form" method="post" action="#{path}?#{qs}">#{csrf_field}<button type="submit" class="#{cls}" title="#{tip}">#{verb}</button></form>)
    end

    def lag_pause_tooltip(refresh_seconds)
      secs = refresh_seconds.to_i
      secs = 30 if secs <= 0
      "Pause/resume is written immediately, but Karafka consumers cache pause state " \
        "for up to #{secs}s (consumption_control_refresh_interval). Processing may " \
        "continue briefly after you click Pause; lag can take up to #{secs}s to stop falling."
    end

    def lag_control_query(scope:, group:, topic:, tenant_q:, partition: nil)
      parts = [
        ["scope", scope],
        ["group", group],
        ["topic", topic],
        ["partition", partition.to_s],
        ["tenant_id", tenant_q]
      ].reject { |_, v| v.nil? || v.to_s.empty? }
      parts.map { |k, v| "#{url_encode(k)}=#{url_encode(v)}" }.join("&")
    end

    def url_encode(value)
      CGI.escape(value.to_s)
    end

    # Small lookup on /lag (or /fairness): given tenant_id, show which ingest partition Kafka assigns.
    def ingest_partition_lookup_widget(tenant_id, lag_rows: nil, action: nil, type: :time)
      action ||= lag_path
      topic = KafkaBatch.config.fairness_ingest_topic(type)
      result =
        if tenant_id.nil?
          ""
        else
          ingest_partition_lookup_result(tenant_id, topic, lag_rows, type)
        end

      <<~HTML
        <div class="card ingest-lookup">
          <h3>Ingest partition lookup</h3>
          <p class="muted">Fair jobs are keyed by <code>tenant_id</code> on <code>#{h(topic)}</code>. Uses the murmur2 partitioner (same as the producer — requires <code>partitioner: murmur2_random</code> in producer config). If the producer was previously using a different partitioner (e.g. librdkafka's default CRC32), in-flight jobs from that era landed on different partitions and will not appear here.</p>
          <form class="search" method="get" action="#{action}">
            <input type="text" name="tenant_id" value="#{h(tenant_id.to_s)}" placeholder="tenant_id (e.g. acme)" autocomplete="off">
            <button type="submit" class="btn">Lookup</button>
          </form>
          #{result}
        </div>
      HTML
    end

    def ingest_partition_lookup_result(tenant_id, topic, lag_rows, type = :time)
      count = KafkaBatch.fairness_ingest_partition_count(type)
      unless count
        return "<p class='muted'>Could not read partition count for <code>#{h(topic)}</code>.</p>"
      end

      # Resolve partition: configured map → dynamic Redis → murmur2 hash fallback.
      configured = (KafkaBatch.config.fairness_tenant_partitions || {})[tenant_id.to_s]
      dynamic    = KafkaBatch.config.fairness_dynamic_tenant_partitions
      resolved   = KafkaBatch.tenant_ingest_partition(tenant_id, type)

      if resolved
        partition = resolved
        source_badge =
          if configured && configured.to_i == partition
            "<span class='badge' style='background:#6366f1'>configured</span> "
          elsif dynamic
            "<span class='badge' style='background:#10b981'>dynamic</span> "
          else
            ""
          end
      elsif configured && configured.to_i >= count
        partition    = KafkaBatch::Partition.for_key(tenant_id, count)
        source_badge = "<span class='badge' style='background:#ef4444'>configured partition #{configured} out of range</span> "
      else
        partition    = KafkaBatch::Partition.for_key(tenant_id, count)
        source_badge = dynamic ? "<span class='badge' style='background:#f59e0b'>hash fallback</span> " : ""
      end

      lag_row = lag_rows&.find do |r|
        r[:topic] == topic && r[:partition].to_i == partition &&
          r[:group] == KafkaBatch.dispatch_consumer_group(type)
      end

      lag_note =
        if lag_row
          if lag_row[:never_consumed]
            " <span class='badge' style='background:#f59e0b'>Never consumed</span>" \
            " — Dispatcher has not yet committed to this partition." \
            " Jobs may be queued here waiting for the Dispatcher."
          elsif lag_row[:lag].to_i.zero?
            " Ingest drained (lag: 0). Jobs forwarded to ready topic — check ready-topic lag below."
          else
            " Pending on this partition (dispatch group): #{lag_badge(lag_row[:lag])}."
          end
        elsif lag_rows
          " <span class='badge' style='background:#f59e0b'>No dispatch-group data</span>" \
          " — Dispatcher has never consumed this topic."
        end

      <<~HTML
        <p>#{source_badge}<strong>Tenant</strong> <code>#{h(tenant_id)}</code> → <strong>partition #{partition}</strong> of #{count} on <code>#{h(topic)}</code>.#{lag_note}</p>
      HTML
    end

    # Per-partition lag rows for a (group, topic), sorted by lag desc.
    # Preserves :never_consumed so the ingest lookup can distinguish "drained"
    # from "group has never committed here".
    def lag_partitions(group, topic)
      data  = KafkaBatch::Lag.read_group(group, [topic])
      parts = (data[group] || {})[topic] || {}
      parts.map do |partition, info|
        never_consumed = info[:offset].to_i < 0
        lag            = info[:lag].to_i
        lag            = 0 if lag.negative?
        { partition: partition.to_i, lag: lag, never_consumed: never_consumed }
      end.sort_by { |p| -p[:lag] }
    end

    # Colour the lag number: grey at 0, amber when backed up.
    def lag_badge(lag)
      n = lag.to_i
      return "<span class='muted'>0</span>" if n.zero?
      color = n >= 1000 ? "#ef4444" : "#f59e0b"
      "<span class='badge' style='background:#{color}'>#{n}</span>"
    end

    def render_show(b, params = {})
      pend = pending(b)
      meta = b[:meta].nil? || b[:meta].empty? ? "—" : "<pre>#{h(b[:meta].inspect)}</pre>"

      tenant_row =
        if (tid = non_empty(b[:tenant_id]))
          { "Tenant" => tenant_chip(tid) }
        else
          {}
        end

      rows = {
        "ID"             => h(b[:id]),
        "Description"    => (non_empty(b[:description]) ? h(b[:description]) : "—"),
        "Status"         => status_badge(b[:status]),
      }.merge(tenant_row).merge(
        "Total jobs"     => b[:total_jobs],
        "Completed"      => b[:completed_count],
        "Failed"         => b[:failed_count],
        "Pending"        => pend,
        "on_success"     => h(b[:on_success] || "—"),
        "on_complete"    => h(b[:on_complete] || "—"),
        "Created at"     => fmt_time(b[:created_at]),
        "Finished at"    => fmt_time(b[:finished_at]),
        "Callback fired" => (b[:callback_dispatched_at].to_s.empty? ? "no" : fmt_time(b[:callback_dispatched_at])),
        "Callback ran on" => (non_empty(b[:callback_dispatched_by]) ? "<span class='mono'>#{h(b[:callback_dispatched_by])}</span>" : "—"),
        "Meta"           => meta
      ).map { |k, v| "<tr><th>#{k}</th><td>#{v}</td></tr>" }.join

      <<~HTML
        <p><a class="back" href="#{index_path}">← All batches</a></p>
        <div class="card">
          <h2>Batch #{h(short_id(b[:id]))}</h2>
          #{progress_bar(b)}
          <table class="detail"><tbody>#{rows}</tbody></table>
          <div class="actions">#{actions_for(b)}</div>
        </div>
        #{failures_section(b, params: params)}
      HTML
    end

    # #24: failures_section now supports pagination via `params["fp"]` (failures
    # page). Previously hardcoded to limit: 100 with a "Showing first 100" note.
    def failures_section(b, params: {})
      f_page   = [params["fp"].to_i, 1].max
      f_offset = (f_page - 1) * PER_PAGE
      failures = KafkaBatch.store.list_failures(b[:id], limit: PER_PAGE + 1, offset: f_offset)
      return "" if failures.empty? && f_page == 1

      has_next  = failures.size > PER_PAGE
      failures  = failures.first(PER_PAGE)

      rows = failures.map do |f|
        color = f[:status] == "retrying" ? "#f59e0b" : "#ef4444"
        <<~ROW.gsub(/\n\s*/, "")
          <tr>
            <td class="mono">#{h(short_id(f[:job_id]))}</td>
            <td>#{h(f[:worker_class])}</td>
            <td><span class="badge" style="background:#{color}">#{h(f[:status])}</span></td>
            <td>#{f[:attempt].to_i + 1}</td>
            <td>#{next_retry_cell(f)}</td>
            <td class="danger">#{h(f[:error_class])}</td>
            <td>#{h(f[:error_message])}</td>
            <td>#{fmt_time(f[:failed_at])}</td>
          </tr>
        ROW
      end.join

      batch_show = show_path(b[:id])
      prev_link  = f_page > 1 ? "<a class='btn' href='#{batch_show}?fp=#{f_page - 1}'>← Prev</a>" : ""
      next_link  = has_next   ? "<a class='btn' href='#{batch_show}?fp=#{f_page + 1}'>Next →</a>" : ""
      pager      = (prev_link.empty? && next_link.empty?) ? "" : \
                   "<div class='pager'>#{prev_link}<span class='page'>Page #{f_page}</span>#{next_link}</div>"

      empty_row = failures.empty? ? "<tr><td colspan='8' class='empty'>No failures on this page.</td></tr>" : ""

      <<~HTML
        <div class="card">
          <h3>Job failures</h3>
          <p class="muted">Recorded on the first failed attempt — <span class="badge" style="background:#f59e0b">retrying</span> while retries remain, <span class="badge" style="background:#ef4444">failed</span> once exhausted.</p>
          <table>
            <thead><tr><th>Job</th><th>Worker</th><th>Status</th><th>Attempt</th><th>Next retry</th><th>Error</th><th>Message</th><th>Failed at</th></tr></thead>
            <tbody>#{rows}#{empty_row}</tbody>
          </table>
          #{pager}
        </div>
      HTML
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] list_failures failed: #{e.message}")
      ""
    end

    # ── Partials ───────────────────────────────────────────────────────────

    def system_card(section)
      wide = section.wide ? " sys-card-wide" : ""
      rows = section.rows.map do |r|
        val_cls = r.masked ? "sys-value masked" : "sys-value"
        <<~ROW
          <div class="sys-row">
            <span class="sys-label">#{h(r.label)}</span>
            <span class="#{val_cls}">#{h(r.value)}</span>
          </div>
        ROW
      end.join

      <<~HTML
        <section class="sys-card#{wide}" style="--sys-accent:#{h(section.accent)}">
          <div class="sys-card-head">
            <span class="sys-card-icon" aria-hidden="true">#{section.icon}</span>
            <h2 class="sys-card-title">#{h(section.title)}</h2>
          </div>
          <div class="sys-card-body">#{rows}</div>
        </section>
      HTML
    end

    def summary_cards(counts, pending_jobs = nil, liveness = nil)
      total = counts.values.sum
      cards = [["Total", total, "#111827", nil]]
      %w[running success complete cancelled].each do |s|
        cards << [s.capitalize, counts[s].to_i, STATUS_COLORS[s], nil]
      end
      # System-wide backlog: jobs pushed but not yet completed/failed.
      cards << ["Pending jobs", pending_jobs, STATUS_COLORS["pending"], nil] unless pending_jobs.nil?
      if liveness
        cards << ["Consumers", liveness[:consumers], STATUS_COLORS["consumers"], live_path]
        cards << ["Running jobs", liveness[:running_jobs], STATUS_COLORS["running_jobs"], live_path]
      end
      inner = cards.map do |label, value, color, href|
        label_html = href ? "<a class='metric-link' href='#{href}'>#{h(label)}</a>" : h(label)
        "<div class='metric'><div class='metric-value' style='color:#{color}'>#{value}</div>" \
        "<div class='metric-label'>#{label_html}</div></div>"
      end.join
      "<div class='metrics'>#{inner}</div>"
    end

    def status_filters(active, counts, search = nil)
      qparam = search ? "&q=#{CGI.escape(search)}" : ""
      links  = [["All", nil]] + %w[running success complete cancelled].map { |s| [s.capitalize, s] }
      items  = links.map do |label, s|
        cls  = (active == s || (s.nil? && active.nil?)) ? "chip active" : "chip"
        href = s ? "#{index_path}?status=#{s}#{qparam}" : "#{index_path}#{search ? "?q=#{CGI.escape(search)}" : ''}"
        n    = s ? " (#{counts[s].to_i})" : ""
        "<a class='#{cls}' href='#{href}'>#{label}#{n}</a>"
      end.join
      "<div class='chips'>#{items}</div>"
    end

    # Search form (GET) filtering batches by id or description. Preserves the
    # active status filter via a hidden field.
    def search_box(search, status)
      hidden = status ? "<input type='hidden' name='status' value='#{h(status)}'>" : ""
      clear  = search ? "<a class='btn' href='#{index_path}#{status ? "?status=#{h(status)}" : ''}'>Clear</a>" : ""
      <<~HTML
        <form class="search" method="get" action="#{index_path}">
          #{hidden}
          <input type="text" name="q" value="#{h(search)}" placeholder="Search by batch ID or description…" autocomplete="off">
          <button type="submit" class="btn">Search</button>#{clear}
        </form>
      HTML
    end

    # #28: when a search query is active, show up to 10 words of the description
    # so the operator can see WHY the batch matched. Without a search (browse mode)
    # only the first 3 words are shown to keep the list compact.
    def batch_row(b, search: nil)
      pend   = pending(b)
      desc   = if (raw_desc = non_empty(b[:description]))
        words      = raw_desc.split
        limit      = search ? 10 : 3
        short      = words.first(limit).join(" ")
        short     += "…" if words.size > limit
        tooltip    = words.size > limit ? " title='#{h(words.first(20).join(" "))}'" : ""
        "<div class='desc'#{tooltip}>#{h(short)}</div>"
      end
      tid    = non_empty(b[:tenant_id])
      t_cell = if tid
        "<td>#{tenant_chip(tid)}</td>"
      else
        "<td class='muted'>—</td>"
      end
      # #26: show created_at so operators can sort/scan batch age at a glance.
      created_cell = "<td class='muted' style='font-size:12px;white-space:nowrap'>#{fmt_time(b[:created_at])}</td>"
      <<~HTML
        <tr>
          <td class="col-check"><input type="checkbox" form="kb-bulk-form" name="batch_ids" value="#{h(b[:id])}" class="kb-batch-check" aria-label="Select batch #{h(short_id(b[:id]))}"></td>
          <td><a href="#{show_path(b[:id])}" class="mono">#{h(short_id(b[:id]))}</a>#{desc}</td>
          #{t_cell}
          <td>#{status_badge(b[:status])}</td>
          <td>#{b[:total_jobs]}</td>
          <td>#{b[:completed_count]}</td>
          <td class="#{b[:failed_count].to_i.positive? ? 'danger' : ''}">#{b[:failed_count]}</td>
          <td>#{pend}</td>
          <td style="min-width:120px">#{progress_bar(b)}</td>
          #{created_cell}
          <td class="actions">#{actions_for(b)}</td>
        </tr>
      HTML
    end

    BULK_FORM_ID = "kb-bulk-form"

    def bulk_batch_toolbar(status:, search:, page:)
      hidden  = +""
      hidden << %(<input type="hidden" form="#{BULK_FORM_ID}" name="return_status" value="#{h(status)}">) if status
      hidden << %(<input type="hidden" form="#{BULK_FORM_ID}" name="return_q" value="#{h(search)}">) if search
      hidden << %(<input type="hidden" form="#{BULK_FORM_ID}" name="return_page" value="#{page}">) if page > 1
      <<~HTML
        #{hidden}
        <form id="#{BULK_FORM_ID}" class="bulk-toolbar" method="post" action="#{bulk_batches_path}">
          #{csrf_field}
          <button type="submit" name="bulk_action" value="cancel" class="btn warn">Cancel selected</button>
          <button type="submit" name="bulk_action" value="delete" class="btn danger-btn">Delete selected</button>
        </form>
      HTML
    end

    def bulk_all_toolbar(status:, search:, page:)
      scope     = bulk_scope_fields(status: status, search: search, page: page)
      label     = bulk_scope_label(status: status, search: search)
      cancel_q  = js_string("Cancel all#{label}? Remaining jobs will not run.")
      delete_q  = js_string("Delete all#{label} permanently? This cannot be undone.")
      <<~HTML
        <div class="bulk-all-actions">
          <form class="inline-form" method="post" action="#{bulk_batches_path}"
                onsubmit="return confirm('#{cancel_q}')">
            #{csrf_field}
            #{scope}
            <button type="submit" name="bulk_action" value="cancel_all" class="btn warn">Cancel all</button>
          </form>
          <form class="inline-form" method="post" action="#{bulk_batches_path}"
                onsubmit="return confirm('#{delete_q}')">
            #{csrf_field}
            #{scope}
            <button type="submit" name="bulk_action" value="delete_all" class="btn danger-btn">Delete all</button>
          </form>
        </div>
      HTML
    end

    def bulk_scope_fields(status:, search:, page:)
      hidden = +""
      hidden << %(<input type="hidden" name="scope_status" value="#{h(status)}">) if status
      hidden << %(<input type="hidden" name="scope_search" value="#{h(search)}">) if search
      hidden << %(<input type="hidden" name="return_status" value="#{h(status)}">) if status
      hidden << %(<input type="hidden" name="return_q" value="#{h(search)}">) if search
      hidden << %(<input type="hidden" name="return_page" value="#{page}">) if page > 1
      hidden
    end

    def bulk_scope_label(status:, search:)
      parts = []
      parts << " #{h(status)}" if status
      parts << " matching \"#{h(search)}\"" if search
      parts << " batches" if parts.any?
      parts.empty? ? " batches" : parts.join
    end

    def bulk_cancel_all(status:, search:)
      ids, note = limited_batch_ids(status: status, search: search)
      ids.each do |id|
        batch = KafkaBatch.store.find_batch(id)
        next unless batch && batch[:status] == "running"

        KafkaBatch.store.update_batch_status(id, "cancelled")
        KafkaBatch::CancellationCache.add(id) if defined?(KafkaBatch::CancellationCache)
      end
      note
    end

    def bulk_delete_all(status:, search:)
      ids, note = limited_batch_ids(status: status, search: search)
      ids.each { |id| KafkaBatch.store.delete_batch(id) }
      note
    end

    def limited_batch_ids(status:, search:)
      all = filtered_batch_ids(status: status, search: search)
      if all.size > BULK_ALL_MAX
        # We stopped scanning at FILTER_SCAN_MAX, so the true total is unknown —
        # report it as "N+" rather than a wrong exact count.
        total = "#{BULK_ALL_MAX}+"
        note  = "Processed first #{BULK_ALL_MAX} of #{total} matching batches; run again for the rest."
        [all.first(BULK_ALL_MAX), note]
      else
        [all, nil]
      end
    end

    # Batch ids matching the index filters, bounded to FILTER_SCAN_MAX so a huge
    # store can never load an unbounded id list into memory. The caller
    # (limited_batch_ids) trims to BULK_ALL_MAX and tells the user to run again
    # for the rest, so scanning past that cap would be wasted work anyway.
    def filtered_batch_ids(status: nil, search: nil)
      ids    = []
      offset = 0
      chunk  = 500
      loop do
        page = KafkaBatch.store.list_batches(status: status, limit: chunk, offset: offset, search: search)
        break if page.empty?

        ids.concat(page.map { |b| b[:id] })
        break if page.size < chunk || ids.size >= FILTER_SCAN_MAX

        offset += chunk
      end
      ids.first(FILTER_SCAN_MAX)
    end

    def js_string(text)
      text.to_s.gsub("\\", "\\\\").gsub("'", "\\'").gsub("\n", " ").gsub("\r", "")
    end

    def bulk_batch_script
      <<~'HTML'
        <script>
        (function () {
          var all = document.getElementById("kb-select-all");
          var form = document.getElementById("kb-bulk-form");
          if (!form) return;
          function boxes() { return Array.prototype.slice.call(document.querySelectorAll(".kb-batch-check")); }
          function syncAll() {
            if (!all) return;
            var cbs = boxes();
            if (!cbs.length) { all.checked = false; all.indeterminate = false; return; }
            var n = cbs.filter(function (cb) { return cb.checked; }).length;
            all.checked = n === cbs.length;
            all.indeterminate = n > 0 && n < cbs.length;
          }
          if (all) {
            all.addEventListener("change", function () {
              boxes().forEach(function (cb) { cb.checked = all.checked; });
              all.indeterminate = false;
            });
          }
          boxes().forEach(function (cb) { cb.addEventListener("change", syncAll); });
          form.addEventListener("submit", function (e) {
            var btn = e.submitter;
            if (!btn || !btn.name) return;
            var n = boxes().filter(function (cb) { return cb.checked; }).length;
            if (n === 0) { e.preventDefault(); alert("Select at least one batch."); return; }
            var msg = btn.value === "cancel"
              ? "Cancel " + n + " batch(es)? Remaining jobs will not run."
              : "Delete " + n + " batch record(s) permanently?";
            if (!confirm(msg)) e.preventDefault();
          });
        })();
        </script>
      HTML
    end

    def bulk_batches_path
      "#{@script_name}/batches/bulk"
    end

    def bulk_batch_ids(params)
      ids = params["batch_ids"]
      ids = ids.is_a?(Array) ? ids : Array(ids)
      ids.map(&:to_s).reject(&:empty?).uniq.first(BULK_MAX)
    end

    # CGI.parse returns arrays for every key; collapse single-value fields.
    def scalarize_params(hash)
      hash.transform_values { |v| v.is_a?(Array) ? (v.size == 1 ? v.first : v) : v }
    end

    # One scalar form/query value (never an Array — Array#to_s would break redirects).
    def form_param(params, key)
      v = params[key]
      v = v.first if v.is_a?(Array) && v.size == 1
      return nil if v.is_a?(Array)
      non_empty(v.to_s)
    end

    def actions_for(b)
      buttons = []
      if b[:status] == "running"
        buttons << form_button(cancel_path(b[:id]), "Cancel", "warn",
                               "Cancel this batch? Remaining jobs will not run.")
      end
      buttons << form_button(delete_path(b[:id]), "Delete", "danger-btn",
                             "Delete this batch record permanently?")
      buttons.join(" ")
    end

    def form_button(action, label, css, confirm)
      # The CSRF token rides a hidden body field (csrf_field), not the URL, so it
      # never leaks via Referer/logs. The main CSRF check reads it back with
      # body_csrf_token (rewind-safe) and falls back to the query token for API
      # clients. Rails wraps rack.input in RewindableInput, so the body read is
      # reliable when mounted in Rails.
      "<form method='post' action='#{action}' onsubmit=\"return confirm('#{h(confirm)}')\" style='display:inline'>" \
      "#{csrf_field}<button type='submit' class='btn #{css}'>#{label}</button></form>"
    end

    def progress_bar(b)
      total = b[:total_jobs].to_i
      done  = b[:completed_count].to_i
      fail  = b[:failed_count].to_i
      return "<span class='muted'>—</span>" if total.zero?

      dpct = (done * 100.0 / total).round(1)
      fpct = (fail * 100.0 / total).round(1)
      <<~HTML.gsub(/\n\s*/, "")
        <div class="bar" title="#{done}/#{total} done, #{fail} failed">
          <div class="bar-done" style="width:#{dpct}%"></div>
          <div class="bar-fail" style="width:#{fpct}%"></div>
        </div>
      HTML
    end

    def status_badge(status)
      color = STATUS_COLORS[status] || "#6b7280"
      "<span class='badge' style='background:#{color}'>#{h(status)}</span>"
    end

    # Render a coloured pill for a tenant_id. The colour is deterministic: the
    # same tenant always gets the same pair so operators can spot it visually
    # without needing a legend.
    def tenant_chip(tenant_id)
      return "" if tenant_id.nil? || tenant_id.empty?
      fg, bg = tenant_colors(tenant_id)
      "<span class='tenant-chip' style='color:#{fg};background:#{bg}'>#{h(tenant_id)}</span>"
    end

    def tenant_colors(tenant_id)
      idx = tenant_id.to_s.bytes.sum % TENANT_COLORS.size
      TENANT_COLORS[idx]
    end

    def weight_share_mini_bar(share_pct, color)
      width = [[share_pct.to_f, 0].max, 100].min
      return "" if width <= 0

      %(<div class="weight-share-mini" aria-hidden="true"><span style="width:#{width}%;background:#{color}"></span></div>)
    end

    def weight_share_distribution_card(shares, cfg)
      return "" if shares.empty?

      segments = shares.map do |s|
        fg, bg = tenant_colors(s.tenant_id)
        pct = s.share_pct.to_f
        label = "#{s.tenant_id}: #{KafkaBatch::WeightShares.format_pct(pct)}"
        <<~SEG.gsub(/\n\s*/, "")
          <div class="weight-share-segment" style="width:#{pct}%;background:#{bg};color:#{fg}"
               title="#{h(label)}" aria-label="#{h(label)}">
            <span class="weight-share-segment-label">#{h(KafkaBatch::WeightShares.format_pct(pct))}</span>
          </div>
        SEG
      end.join

      legend = shares.map do |s|
        fg, bg = tenant_colors(s.tenant_id)
        pct = KafkaBatch::WeightShares.format_pct(s.share_pct)
        <<~ITEM.gsub(/\n\s*/, "")
          <div class="weight-share-legend-item">
            <span class="weight-share-swatch" style="background:#{bg};color:#{fg}"></span>
            <span class="mono">#{h(s.tenant_id)}</span>
            <span class="weight-share-legend-pct">#{pct}</span>
            <span class="muted weight-share-legend-weight">(w=#{s.weight})</span>
          </div>
        ITEM
      end.join

      concurrency_note =
        if cfg.fairness_weighted_concurrency
          "<p class=\"muted\">With <code>fairness_weighted_concurrency</code> enabled, these shares drive per-tenant in-flight caps.</p>"
        else
          "<p class=\"muted\">Shares reflect relative weight ratios. Enable <code>fairness_weighted_concurrency</code> to enforce them as in-flight capacity caps; otherwise weights mainly affect selection order under saturation.</p>"
        end

      <<~HTML
        <div class="card">
          <h3>Capacity distribution</h3>
          <p class="muted">Overall capacity is normalized to <strong>100%</strong>. Each tenant's share is its weight divided by the sum of all tenant weights.</p>
          #{concurrency_note}
          <div class="weight-share-track" role="img" aria-label="Tenant capacity share distribution">
            #{segments}
          </div>
          <div class="weight-share-legend">#{legend}</div>
        </div>
      HTML
    end

    def pagination(page, has_next, status, search = nil)
      qs = ""
      qs += "&status=#{status}" if status
      qs += "&q=#{CGI.escape(search)}" if search
      prev_link = page > 1 ? "<a class='btn' href='#{index_path}?page=#{page - 1}#{qs}'>← Prev</a>" : ""
      next_link = has_next ? "<a class='btn' href='#{index_path}?page=#{page + 1}#{qs}'>Next →</a>" : ""
      return "" if prev_link.empty? && next_link.empty?
      "<div class='pager'>#{prev_link}<span class='page'>Page #{page}</span>#{next_link}</div>"
    end

    # ── Helpers ────────────────────────────────────────────────────────────

    def pending(b)
      return 0 if %w[success complete cancelled].include?(b[:status])
      [b[:total_jobs].to_i - b[:completed_count].to_i - b[:failed_count].to_i, 0].max
    end

    def safe_counts
      KafkaBatch.store.batch_counts || {}
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] batch_counts failed: #{e.message}")
      {}
    end

    def safe_pending_jobs
      KafkaBatch.store.pending_jobs_total
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] pending_jobs_total failed: #{e.message}")
      nil
    end

    def safe_liveness_snapshot
      return nil unless KafkaBatch::Liveness.available?

      {
        consumers:    KafkaBatch::Liveness.consumers.size,
        running_jobs: KafkaBatch::Liveness.running_jobs.size
      }
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] liveness snapshot failed: #{e.message}")
      nil
    end

    def safe_dlt_stats
      KafkaBatch::Dlt::Stats.fetch
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] DLT stats failed: #{e.message}")
      nil
    end

    def safe_dlt_page(type:, before:)
      reader = KafkaBatch::Dlt::Reader.new
      reader.fetch_page(type: type, before: before, limit: PER_PAGE)
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] DLT page failed: #{e.message}")
      { messages: [], has_older: false, cursor_older: nil }
    ensure
      reader&.close
    end

    def dlt_type_chips(active, by_type)
      chips = [["All", nil, by_type.values.sum]]
      KafkaBatch::Dlt::Reader::DLT_TYPES.each do |t|
        n = by_type[t].to_i
        chips << [t, t, n] if n.positive? || active == t
      end
      chips.map do |label, t, n|
        cls = (active == t || (t.nil? && active.nil?)) ? "chip active" : "chip"
        href = t ? "#{dead_letter_path}?type=#{CGI.escape(t)}" : dead_letter_path
        suffix = n.positive? ? " (#{n})" : ""
        "<a class='#{cls}' href='#{href}'>#{h(label)}#{suffix}</a>"
      end.join
    end

    def dlt_row(m)
      when_at = m[:dlt_at] || m[:timestamp]
      worker = m[:worker_class] || m[:callback_class]
      err    = [m[:error_class], m[:error_message]].compact.join(": ")
      err    = err[0, 120] + "…" if err.length > 120
      batch_link = m[:batch_id] ? "<a href='#{show_path(m[:batch_id])}'>#{h(short_id(m[:batch_id]))}</a>" : "—"
      job        = m[:job_id] ? h(short_id(m[:job_id])) : "—"
      <<~ROW.gsub(/\n\s*/, "")
        <tr>
          <td>#{h(fmt_time(when_at))}</td>
          <td><span class="badge">#{h(m[:dlt_type].to_s)}</span></td>
          <td class="mono">#{h(worker.to_s)}</td>
          <td>#{batch_link} / <span class="mono">#{job}</span></td>
          <td class="mono">#{h(m[:source_topic].to_s)}</td>
          <td class="mono">#{h(err)}</td>
          <td class="mono">#{m[:partition]}:#{m[:offset]}</td>
        </tr>
      ROW
    end

    def reconciler_age_label(ran_at)
      return "unknown" if ran_at.nil? || ran_at.to_s.empty?

      t = Time.parse(ran_at.to_s)
      secs = (Time.now - t).to_i
      return "just now" if secs < 60
      return "#{secs / 60}m ago" if secs < 3600
      return "#{secs / 3600}h ago" if secs < 86_400

      "#{secs / 86_400}d ago"
    rescue StandardError
      ran_at.to_s
    end

    def short_id(id)
      id.to_s[0, 8]
    end

    def index_path
      @script_name.empty? ? "/" : "#{@script_name}/"
    end

    def failures_path
      "#{@script_name}/failures"
    end

    def live_path
      "#{@script_name}/live"
    end

    def lag_path
      "#{@script_name}/lag"
    end

    def fairness_path(type = :time)
      "#{@script_name}/fairness/#{type}"
    end

    def weights_path(type = :time)
      "#{@script_name}/weights/#{type}"
    end

    def system_path
      "#{@script_name}/system"
    end

    def scheduled_path
      "#{@script_name}/scheduled"
    end

    def reconciler_path
      "#{@script_name}/reconciler"
    end

    def dead_letter_path
      "#{@script_name}/dead_letter"
    end

    def audit_path
      "#{@script_name}/audit"
    end

    def weights_reset_path(type = :time)
      "#{@script_name}/weights/#{type}/reset"
    end

    def show_path(id)
      "#{@script_name}/batches/#{CGI.escape(id.to_s)}"
    end

    def cancel_path(id)
      "#{show_path(id)}/cancel"
    end

    def delete_path(id)
      "#{show_path(id)}/delete"
    end

    def non_empty(v)
      v.nil? || v.empty? ? nil : v
    end

    # A status filter is honoured only if it is one of the known batch statuses;
    # anything else (typos, injection probes) is treated as "no filter".
    def valid_status(v)
      s = non_empty(v)
      s if s && VALID_STATUSES.include?(s)
    end

    def parse_query(qs)
      CGI.parse(qs.to_s).transform_values(&:first)
    end

    # Like body_params but keeps multi-value fields (e.g. batch_ids from checkboxes).
    def body_params_multi(env)
      input = env["rack.input"]
      return {} unless input

      input.rewind
      raw = input.read(MAX_BODY_BYTES + 1)
      return {} if raw.bytesize > MAX_BODY_BYTES

      CGI.parse(raw)
    rescue StandardError
      {}
    end

    # Safely read URL-encoded params from the request body (rack.input).
    # Used only for POST /weights and POST /weights/reset, where tenant_id
    # and weight arrive in the body (standard HTML form fields).
    #
    # The normal params hash is parsed from QUERY_STRING to avoid consuming
    # rack.input (which middleware like ActionDispatch::Request may exhaust).
    # However, Rails wraps rack.input in Rack::RewindableInput, so calling
    # rewind after Rails has read it is always safe. We rescue any error
    # (e.g. raw Rack without rewindable input) and fall back to {}.
    def body_params(env)
      input = env["rack.input"]
      return {} unless input

      input.rewind
      raw = input.read(MAX_BODY_BYTES + 1).to_s
      input.rewind
      return {} if raw.bytesize > MAX_BODY_BYTES

      parse_query(raw)
    rescue StandardError
      {}
    end

    def h(text)
      CGI.escapeHTML(text.to_s)
    end

    # Human "time until" a future timestamp, e.g. "in 23h 59m" / "in 5m 3s".
    def fmt_eta(value)
      return "—" if value.nil? || (value.respond_to?(:empty?) && value.empty?)
      t = value.respond_to?(:to_time) ? value.to_time : Time.parse(value.to_s)
      secs = (t - Time.now).round
      return "due now" if secs <= 0

      d = secs / 86_400; secs %= 86_400
      hh = secs / 3_600; secs %= 3_600
      mm = secs / 60;    ss = secs % 60
      parts = []
      parts << "#{d}d"  if d.positive?
      parts << "#{hh}h" if hh.positive?
      parts << "#{mm}m" if mm.positive?
      parts << "#{ss}s" if ss.positive?
      "in #{parts.first(2).join(' ')}"
    rescue StandardError
      "—"
    end

    # Table cell for a failure's next retry (ETA + absolute UTC), or "—".
    def next_retry_cell(failure)
      return "—" unless failure[:status] == "retrying" && failure[:next_retry_at]
      "#{fmt_eta(failure[:next_retry_at])}<br><span class='muted'>#{fmt_time(failure[:next_retry_at])}</span>"
    end

    # Render any timestamp (Time, ActiveRecord time, or ISO8601 string) as
    # UTC in 24-hour format with an explicit suffix: "2026-06-27 20:19:44 UTC".
    def fmt_time(value)
      return "—" if value.nil? || (value.respond_to?(:empty?) && value.empty?)

      t =
        if value.respond_to?(:to_time)
          value.to_time
        else
          Time.parse(value.to_s)
        end
      t.utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    rescue StandardError
      h(value.to_s)
    end

    def fmt_mem(bytes)
      b = bytes.to_i
      return "<span class='muted'>—</span>" if bytes.nil? || b <= 0
      if b >= 1_073_741_824
        "#{(b / 1_073_741_824.0).round(1)} GB"
      elsif b >= 1_048_576
        "#{(b / 1_048_576.0).round(1)} MB"
      else
        "#{(b / 1024.0).round(0)} KB"
      end
    end

    def fmt_cpu(pct)
      return "<span class='muted'>—</span>" if pct.nil? || pct.to_s.empty?
      n = pct.to_f
      return "<span class='muted'>—</span>" if n.negative?
      color = n >= 80.0 ? "#ef4444" : (n >= 50.0 ? "#f59e0b" : nil)
      return "#{n.round(1)}%" unless color
      "<span class='badge' style='background:#{color}'>#{n.round(1)}%</span>"
    end

    def layout(title, body)
      live_toggle_button = %(<button id="kb-live-toggle" type="button" class="btn">○ Live</button>)
      foot_script        = live_toggle_script

      <<~HTML
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>KafkaBatch — #{h(title)}</title>
          <style>#{CSS}</style>
        </head>
        <body>
          <header>
            <a href="#{index_path}" class="logo">KafkaBatch</a>
            <nav class="header-nav">
              #{nav_btn("/", "Batches")}
              #{nav_btn("/failures", "⚠ Failures")}
              #{nav_btn("/dead_letter", "☠ Dead letter")}
              #{nav_btn("/live", "▶ Consumer Process")}
              #{nav_btn("/lag", "▦ Kafka Lag")}
              #{nav_btn("/scheduled", "⏰ Scheduled")}
              #{nav_btn("/reconciler", "⟳ Reconciler")}
              #{audit_nav_btn}
              #{nav_btn("/fairness/time", "⏱ Time Fairness")}
              #{nav_btn("/weights/time", "⚖ Time Weights")}
              #{nav_btn("/fairness/throughput", "⚡ Throughput Fairness")}
              #{nav_btn("/weights/throughput", "⚖ Throughput Weights")}
              #{nav_btn("/system", "⚙ System")}
              #{live_toggle_button}
            </nav>
          </header>
          <main>#{body}</main>
          #{foot_script}
        </body>
        </html>
      HTML
    end

    def nav_btn(path_suffix, label)
      href = path_suffix == "/" ? index_path : "#{@script_name}#{path_suffix}"
      active = @path == path_suffix
      cls = active ? "btn nav-active" : "btn"
      %(<a class="#{cls}" href="#{href}">#{label}</a>)
    end

    # The audit link only appears when the audit log is enabled, so the nav
    # doesn't advertise a page that would just say "disabled".
    def audit_nav_btn
      return "" unless defined?(KafkaBatch::AuditLog) && KafkaBatch::AuditLog.enabled?

      nav_btn("/audit", "📝 Audit")
    end

    CSS = <<~CSS
      * { box-sizing: border-box; }
      body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
             background: #f3f4f6; color: #111827; }
      header { background: #111827; color: #fff; padding: 10px 24px; display: flex; align-items: center; gap: 10px; }
      header .logo { color: #fff; text-decoration: none; font-weight: 700; font-size: 18px; }
      header .tag { color: #9ca3af; font-size: 13px; }
      .header-nav { margin-left: auto; display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
      header .btn { background: transparent; border-color: #4b5563; color: #e5e7eb; font-size: 12px; padding: 4px 10px; }
      header .btn.nav-active { border-color: #e5e7eb; background: #374151; color: #fff; }
      header .btn.live-on { border-color: #10b981; background: #10b981; color: #fff; }
      main { max-width: 1100px; margin: 24px auto; padding: 0 16px; }
      .card { background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px; margin-bottom: 16px; }
      table { width: 100%; border-collapse: collapse; font-size: 14px; }
      th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid #f0f0f0; }
      thead th { color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      td.empty { text-align: center; color: #9ca3af; padding: 28px; }
      .mono, .detail th { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
      .metrics { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
      .metric { background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 14px 18px; min-width: 110px; }
      .metric-value { font-size: 26px; font-weight: 700; }
      .metric-label { color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
      .metric-link { color: inherit; text-decoration: none; }
      .metric-link:hover { color: #2563eb; text-decoration: underline; }
      .chips { margin-bottom: 12px; display: flex; gap: 8px; flex-wrap: wrap; }
      .chip { text-decoration: none; color: #374151; background: #fff; border: 1px solid #e5e7eb;
              padding: 5px 12px; border-radius: 999px; font-size: 13px; }
      .chip.active { background: #111827; color: #fff; border-color: #111827; }
      .badge { color: #fff; padding: 3px 9px; border-radius: 999px; font-size: 12px; text-transform: capitalize; }
      .bar { background: #eef0f3; border-radius: 999px; height: 8px; overflow: hidden; display: flex; }
      .bar-done { background: #10b981; height: 100%; }
      .bar-fail { background: #ef4444; height: 100%; }
      .btn { display: inline-block; text-decoration: none; border: 1px solid #d1d5db; background: #fff;
             color: #374151; padding: 5px 12px; border-radius: 7px; font-size: 13px; cursor: pointer; }
      .btn.warn { border-color: #f59e0b; color: #b45309; }
      .btn.danger-btn { border-color: #ef4444; color: #b91c1c; }
      .btn.btn-sm { padding: 2px 10px; font-size: 12px; }
      .inline-form { display: inline; margin: 0; }
      .btn.live-on { border-color: #10b981; background: #10b981; color: #fff; }
      .actions { white-space: nowrap; }
      td.danger { color: #b91c1c; font-weight: 600; }
      .pager { display: flex; gap: 12px; align-items: center; justify-content: center; margin: 8px 0 24px; }
      .page { color: #6b7280; font-size: 13px; }
      .back { color: #2563eb; text-decoration: none; }
      .muted { color: #9ca3af; }
      .desc { color: #6b7280; font-size: 12px; margin-top: 3px; max-width: 320px; }
      .filterbar { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; flex-wrap: wrap; }
      .filterbar .chips { margin-bottom: 0; }
      .filterbar .search { margin-bottom: 0; margin-left: auto; }
      .index-bottom { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px; margin: 12px 0 24px; }
      .index-bottom .bulk-toolbar,
      .index-bottom .bulk-all-actions { display: flex; align-items: center; gap: 8px; margin: 0; flex-wrap: wrap; }
      .index-bottom .inline-form { display: flex; align-items: center; margin: 0; }
      .index-bottom .pager { margin: 0; flex: 1; justify-content: center; min-height: 32px; }
      .index-bottom .btn { min-height: 32px; padding: 6px 12px; line-height: 1.25; box-sizing: border-box; }
      .bulk-hint { font-size: 13px; }
      .col-check { width: 32px; text-align: center; padding-left: 4px; padding-right: 4px; }
      .col-check input[type=checkbox] { cursor: pointer; width: 15px; height: 15px; }
      .search { display: flex; gap: 8px; margin-bottom: 12px; }
      .search input[type=text] { width: 280px; padding: 6px 12px; border: 1px solid #d1d5db;
                                 border-radius: 7px; font-size: 14px; }
      .detail th { width: 180px; color: #6b7280; vertical-align: top; }
      pre { white-space: pre-wrap; word-break: break-word; margin: 0; font-size: 12px; }
      h2 { margin-top: 0; }
      .tenant-chip { display:inline-block; font-size:11px; font-weight:600; padding:1px 8px;
                     border-radius:999px; white-space:nowrap; }
      .weight-input { width:72px; padding:4px 8px; border:1px solid #d1d5db; border-radius:6px;
                      font-size:13px; text-align:right; }
      .weight-add-form { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
      .weight-add-id { padding:6px 12px; border:1px solid #d1d5db; border-radius:7px;
                       font-size:14px; width:220px; }
      .weight-share-track {
        display: flex; width: 100%; height: 28px; border-radius: 999px; overflow: hidden;
        border: 1px solid #e5e7eb; background: #f9fafb; margin: 12px 0 14px;
      }
      .weight-share-segment {
        display: flex; align-items: center; justify-content: center; min-width: 0;
        overflow: hidden; transition: width .2s ease;
      }
      .weight-share-segment-label {
        font-size: 11px; font-weight: 700; padding: 0 6px; white-space: nowrap;
        overflow: hidden; text-overflow: ellipsis;
      }
      .weight-share-legend { display: flex; flex-wrap: wrap; gap: 10px 18px; }
      .weight-share-legend-item { display: flex; align-items: center; gap: 6px; font-size: 13px; min-width: 0; }
      .weight-share-swatch { width: 12px; height: 12px; border-radius: 3px; flex-shrink: 0; border: 1px solid rgba(0,0,0,.06); }
      .weight-share-legend-pct { font-weight: 700; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
      .weight-share-legend-weight { font-size: 12px; }
      .weight-share-cell { display: flex; flex-direction: column; align-items: flex-end; gap: 4px; min-width: 72px; }
      .weight-share-pct { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; font-weight: 700; }
      .weight-share-mini { width: 72px; height: 5px; border-radius: 999px; background: #eef0f3; overflow: hidden; }
      .weight-share-mini span { display: block; height: 100%; border-radius: 999px; }
      @media (max-width: 640px) {
        .weight-share-segment-label { display: none; }
        .weight-share-track { height: 22px; }
        .weight-share-legend { flex-direction: column; gap: 8px; }
      }
      .metric-info { cursor:help; }
      th[title] { cursor:help; }
      .sys-lead { margin: 0 0 16px; font-size: 14px; line-height: 1.5; }
      main:has(.sys-grid) { max-width: 1280px; }
      .sys-grid {
        display: grid;
        grid-template-columns: 1fr;
        gap: 12px;
        margin-bottom: 24px;
      }
      .sys-card {
        background: #fff;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        overflow: hidden;
        border-top: 3px solid var(--sys-accent, #111827);
        box-shadow: 0 1px 2px rgba(17,24,39,.04);
        min-width: 0;
      }
      .sys-card-wide { grid-column: 1 / -1; }
      .sys-card-head {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 12px 14px 10px;
        border-bottom: 1px solid #f3f4f6;
        background: linear-gradient(180deg, #fafafa 0%, #fff 100%);
      }
      .sys-card-icon { font-size: 18px; line-height: 1; opacity: .85; flex-shrink: 0; }
      .sys-card-title { margin: 0; font-size: 15px; font-weight: 700; color: #111827; line-height: 1.3; }
      .sys-card-body { padding: 4px 14px 12px; min-width: 0; }
      .sys-row {
        display: grid;
        grid-template-columns: minmax(0, 1fr) minmax(0, 1.2fr);
        align-items: start;
        gap: 8px 16px;
        padding: 8px 0;
        border-bottom: 1px solid #f9fafb;
        font-size: 13px;
      }
      .sys-row:last-child { border-bottom: none; }
      .sys-label { color: #6b7280; line-height: 1.4; min-width: 0; }
      .sys-value {
        color: #111827;
        text-align: right;
        word-break: break-word;
        overflow-wrap: anywhere;
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 12px;
        line-height: 1.45;
        min-width: 0;
      }
      .sys-value.masked { color: #9ca3af; font-style: italic; }
      @media (min-width: 480px) {
        .sys-grid { gap: 14px; }
        .sys-card-head { padding: 14px 16px 10px; }
        .sys-card-body { padding: 4px 16px 14px; }
      }
      @media (min-width: 640px) {
        .sys-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
      }
      @media (min-width: 1024px) {
        .sys-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
        .sys-card-wide .sys-card-body {
          display: grid;
          grid-template-columns: repeat(2, minmax(0, 1fr));
          column-gap: 28px;
        }
      }
      @media (max-width: 479px) {
        .sys-row { grid-template-columns: 1fr; gap: 2px; }
        .sys-value { text-align: left; }
        .sys-lead { font-size: 13px; margin-bottom: 12px; }
      }
    CSS
  end
end
