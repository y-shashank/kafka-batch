require "erb"
require "cgi"
require "securerandom"
require "time"

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

    # ── CSRF (double-submit cookie pattern) ───────────────────────────────────
    # A random token is generated once per process. It is set as a SameSite=Strict
    # cookie on every response and embedded in every destructive form (cancel,
    # delete, lag/pause, lag/resume). On POST requests the submitted token (from
    # the form field or action query string) must match the cookie value.
    # Cross-origin requests are blocked because browsers will not send the
    # SameSite=Strict cookie, so the attacker cannot obtain a valid token.
    # Non-browser clients (curl, scripts) that omit the cookie bypass the check,
    # which is intentional — those clients are not subject to CSRF.
    CSRF_COOKIE = "_kb_csrf"
    CSRF_FIELD  = "_csrf"

    STATUS_COLORS = {
      "running"   => "#3b82f6",
      "success"   => "#10b981",
      "complete"  => "#f59e0b",
      "cancelled" => "#6b7280",
      "pending"   => "#8b5cf6"
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
      params       = parse_query(env["QUERY_STRING"])

      # ── CSRF: resolve token from cookie (or generate a fresh one) ──────────
      request_cookies = parse_cookies(env)
      @csrf_token     = request_cookies[CSRF_COOKIE] || SecureRandom.hex(16)

      # Validate CSRF on mutating POSTs: submitted token must match the cookie.
      # Non-browser clients that send no cookie value bypass the check (no cookie
      # means no session, no cross-site vector).
      if method == "POST" && request_cookies.key?(CSRF_COOKIE)
        # Parse POST body for hidden-field submissions (cancel / delete forms).
        body        = env["rack.input"]&.read.to_s ; env["rack.input"]&.rewind
        body_params = parse_query(body)
        submitted   = params[CSRF_FIELD] || body_params[CSRF_FIELD]
        unless submitted && submitted == @csrf_token
          return inject_csrf_cookie(csrf_forbidden)
        end
      end

      response =
        if method == "GET" && path == "/"
          html(render_index(params))
        elsif method == "GET" && path == "/failures"
          html(render_failures(params))
        elsif method == "GET" && path == "/live"
          html(render_live)
        elsif method == "GET" && path == "/lag"
          html(render_lag(params))
        elsif method == "POST" && path == "/lag/pause"
          lag_consumption_control(:pause, params)
        elsif method == "POST" && path == "/lag/resume"
          lag_consumption_control(:resume, params)
        elsif method == "GET" && path == "/fairness"
          html(render_fairness(params))
        elsif method == "GET" && (m = path.match(%r{\A/batches/([^/]+)\z}))
          batch = KafkaBatch.store.find_batch(m[1])
          batch ? html(render_show(batch, params)) : not_found
        elsif method == "POST" && (m = path.match(%r{\A/batches/([^/]+)/cancel\z}))
          # Inline cancel so web.rb works in UI-only mode (no Batch class loaded).
          # Mirrors KafkaBatch::Batch.cancel exactly.
          KafkaBatch.store.update_batch_status(m[1], "cancelled")
          KafkaBatch::CancellationCache.add(m[1]) if defined?(KafkaBatch::CancellationCache)
          redirect_to_index
        elsif method == "POST" && (m = path.match(%r{\A/batches/([^/]+)/delete\z}))
          KafkaBatch.store.delete_batch(m[1])
          redirect_to_index
        else
          not_found
        end

      # Stamp the CSRF cookie on every response so it is always fresh.
      inject_csrf_cookie(response)
    rescue StandardError => e
      KafkaBatch.logger.error("[KafkaBatch::Web] #{e.class}: #{e.message}")
      [500, html_headers,
       [layout("Error", "<div class='card'><h2>500</h2><pre>#{h(e.message)}</pre></div>")]]
    end

    private

    # ── CSRF helpers ───────────────────────────────────────────────────────

    def csrf_token
      @csrf_token
    end

    # A hidden <input> that submits the CSRF token in POST bodies.
    def csrf_hidden
      "<input type='hidden' name='#{CSRF_FIELD}' value='#{h(csrf_token)}'>"
    end

    # Stamp the CSRF cookie onto any Rack response triple.
    def inject_csrf_cookie(response)
      status, headers, body = response
      headers = headers.dup
      # SameSite=Strict: browser will not send this cookie on cross-origin requests,
      # which is the key security property of the double-submit pattern.
      path_scope = @script_name.empty? ? "/" : "#{@script_name}/"
      cookie = "#{CSRF_COOKIE}=#{csrf_token}; Path=#{path_scope}; SameSite=Strict"
      existing = headers["set-cookie"].to_s
      headers["set-cookie"] = existing.empty? ? cookie : "#{existing}\n#{cookie}"
      [status, headers, body]
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

    # ── Responses ──────────────────────────────────────────────────────────

    # Dashboard data is always live; never let a browser/proxy cache it (also
    # prevents Rails' Rack::ETag from issuing 304s that mask counter updates).
    def html_headers
      { "content-type" => "text/html; charset=utf-8", "cache-control" => "no-store" }
    end

    # Wrap an HTML body string in the layout; pass through ready-made responses.
    def html(body_or_response)
      return body_or_response if body_or_response.is_a?(Array)
      [200, html_headers, [layout("Batches", body_or_response)]]
    end

    def not_found
      [404, html_headers, [layout("Not found", "<div class='card'><h2>404</h2><p>Not found.</p></div>")]]
    end

    def redirect_to_lag(tenant_id = nil)
      qs = tenant_id && !tenant_id.empty? ? "?tenant_id=#{url_encode(tenant_id)}" : ""
      [302, { "location" => "#{lag_path}#{qs}", "cache-control" => "no-store", "content-type" => "text/html" }, []]
    end

    def redirect_to_index
      [302, { "location" => index_path, "cache-control" => "no-store", "content-type" => "text/html" }, []]
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
        KafkaBatch::ConsumptionControl.pause_topic(group: group, topic: topic)
      when [:resume, "topic"]
        KafkaBatch::ConsumptionControl.resume_topic(group: group, topic: topic)
      when [:pause, "partition"]
        KafkaBatch::ConsumptionControl.pause_partition(group: group, topic: topic, partition: part.to_i)
      when [:resume, "partition"]
        KafkaBatch::ConsumptionControl.resume_partition(group: group, topic: topic, partition: part.to_i)
      end

      redirect_to_lag(non_empty(params["tenant_id"]))
    end

    # Full-page auto-reload used by the always-live pages (/live, /lag, /fairness).
    # Each response is a fresh server render (cache-control: no-store), so a reload
    # simply shows current data. Included on their transient "unavailable" states
    # too, so those pages self-recover once the backend comes back.
    def auto_reload_script
      '<script>setTimeout(function(){ location.reload(); }, 5000);</script>'
    end

    # ── Pages ──────────────────────────────────────────────────────────────

    def render_index(params)
      status   = non_empty(params["status"])
      search   = non_empty(params["q"])
      page     = [params["page"].to_i, 1].max
      offset   = (page - 1) * PER_PAGE
      counts   = safe_counts
      # Fetch one extra row to detect whether a next page exists.
      batches  = KafkaBatch.store.list_batches(status: status, limit: PER_PAGE + 1, offset: offset, search: search)
      has_next = batches.size > PER_PAGE
      batches  = batches.first(PER_PAGE)

      summary = summary_cards(counts, safe_pending_jobs)
      filters = status_filters(status, counts, search)
      rows    = batches.map { |b| batch_row(b, search: search) }.join
      empty   = search ? "“No batches match “#{h(search)}”.”" : "No batches found."
      rows    = "<tr><td colspan='10' class='empty'>#{empty}</td></tr>" if batches.empty?
      pager   = pagination(page, has_next, status, search)

      <<~HTML
        #{summary}
        <div class="toolbar"><button id="kb-live-toggle" type="button" class="btn">○ Live</button> <a class="btn" href="#{failures_path}">⚠ View all failures</a> <a class="btn" href="#{live_path}">▶ Live activity</a> <a class="btn" href="#{lag_path}">▦ Topic lag</a> <a class="btn" href="#{fairness_path}">⚖ Fairness</a></div>
        <div class="filterbar">#{filters}#{search_box(search, status)}</div>
        <div class="card">
          <table>
            <thead>
              <tr>
                <th>Batch</th><th>Tenant</th><th>Status</th><th>Total</th><th>Done</th>
                <th>Failed</th><th>Pending</th><th>Progress</th><th>Created</th><th>Actions</th>
              </tr>
            </thead>
            <tbody>#{rows}</tbody>
          </table>
        </div>
        #{pager}
        #{live_toggle_script}
      HTML
    end

    # Client-side "Live" toggle for the batch-list page: when on, reloads the
    # page every 5s (full server render, so all counters refresh). The choice is
    # persisted in localStorage so it survives the reloads and navigation, and a
    # small countdown is shown on the button. Reload keeps the current URL, so
    # the active filter / search / page are preserved.
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
          #{auto_reload_script}
        HTML
      end

      consumers = KafkaBatch::Liveness.consumers
      jobs      = KafkaBatch::Liveness.running_jobs

      consumer_rows = consumers.map do |c|
        "<tr><td class='mono'>#{h(c['consumer_id'])}</td><td>#{h(c['hostname'])}</td>" \
        "<td>#{h(c['pid'])}</td><td>#{h(c['topic'])}</td><td>#{fmt_time(c['last_seen'])}</td></tr>"
      end.join
      consumer_rows = "<tr><td colspan='5' class='empty'>No active consumers seen.</td></tr>" if consumers.empty?

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
            <thead><tr><th>Consumer</th><th>Host</th><th>PID</th><th>Topic</th><th>Last seen</th></tr></thead>
            <tbody>#{consumer_rows}</tbody>
          </table>
        </div>
        <div class="card">
          <h3>Running jobs</h3>
          <p class="muted">Backend: <code>#{h(KafkaBatch::Liveness.backend)}</code>. Approximate snapshot#{KafkaBatch::Liveness.backend == :store ? ' (sampled per consumer at heartbeat)' : ''} — short-lived jobs may not always appear. Auto-refreshing every 5s.</p>
          <table>
            <thead><tr><th>Job</th><th>Batch</th><th>Worker</th><th>Consumer</th><th>Topic/Part</th><th>Started</th></tr></thead>
            <tbody>#{job_rows}</tbody>
          </table>
        </div>
        #{auto_reload_script}
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
          #{auto_reload_script}
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
            <p class="muted">Could not read lag from Kafka: <code>#{h(e.message)}</code></p>
          </div>
          #{auto_reload_script}
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
          case KafkaBatch::ConsumptionControl.backend
          when :redis
            "<p class='muted'>Pause/resume uses Redis (<code>#{h(KafkaBatch.config.redis_url)}</code>). " \
            "Karafka consumers refresh pause state every #{refresh}s.</p>"
          when :mysql
            "<p class='muted'>Pause/resume uses MySQL (<code>kafka_batch_consumption_pauses</code>). " \
            "Karafka consumers refresh pause state every #{refresh}s.</p>"
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
        #{auto_reload_script}
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

    def render_fairness(params = {})
      tenant_q = non_empty(params["tenant_id"])
      back = "<p><a class=\"back\" href=\"#{index_path}\">← All batches</a></p>"

      inactive_notice =
        unless KafkaBatch.fairness?
          "<div class='card'><p class='muted'>No registered workers opt into multi-tenant fairness yet (set <code>fairness true</code> on a Worker class). Lag below reflects the configured ingest/ready topics.</p></div>"
        end
      unless KafkaBatch::Lag.available?
        return "#{back}#{inactive_notice}#{ingest_partition_lookup_widget(tenant_q, action: fairness_path)}<div class='card'><h2>Fairness</h2><p class='muted'>This view needs Karafka's admin API (<code>Karafka::Admin</code>), which isn't available in this process.</p></div>#{auto_reload_script}"
      end

      cfg            = KafkaBatch.config
      ingest, ready =
        begin
          [lag_partitions(KafkaBatch.dispatch_consumer_group, cfg.fairness_ingest_topic),
           lag_partitions(KafkaBatch.jobs_fair_consumer_group, cfg.fairness_ready_topic)]
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch::Web] fairness lag read failed: #{e.message}")
          return "#{back}#{inactive_notice}#{ingest_partition_lookup_widget(tenant_q, action: fairness_path)}<div class='card'><h2>Fairness</h2><p class='muted'>Could not read lag from Kafka: <code>#{h(e.message)}</code></p></div>#{auto_reload_script}"
        end

      # Augment ingest rows with :group and :topic so ingest_partition_lookup_result
      # can match against them (same shape as KafkaBatch::Lag.partitions rows).
      full_ingest_rows = ingest.map do |p|
        { group:     KafkaBatch.dispatch_consumer_group,
          topic:     cfg.fairness_ingest_topic,
          partition: p[:partition],
          lag:       p[:lag] }
      end
      lookup_html = ingest_partition_lookup_widget(tenant_q, lag_rows: full_ingest_rows, action: fairness_path)

      ingest_total = ingest.sum { |p| p[:lag] }
      ready_total  = ready.sum { |p| p[:lag] }
      lanes        = ingest.count { |p| p[:lag].positive? }
      throttled    = ready_total >= cfg.fairness_ready_lag_high.to_i
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
          <p class="muted">Jobs land on the ingest topic (keyed one-tenant-per-partition), the dispatcher forwards them fairly onto the ready topic, and the JobConsumer swarm drains it. The dispatcher throttles to keep the ready buffer between <code>#{cfg.fairness_ready_lag_low}</code> and <code>#{cfg.fairness_ready_lag_high}</code>. Auto-refreshing every 5s.</p>
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
        #{auto_reload_script}
      HTML
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
      # #23: lag forms POST all params via query string (no body); embed the CSRF
      # token there too so it arrives in env["QUERY_STRING"] and passes validation.
      csrf_qs = "&#{url_encode(CSRF_FIELD)}=#{url_encode(csrf_token)}"
      %(<form class="inline-form" method="post" action="#{path}?#{qs}#{csrf_qs}"><button type="submit" class="#{cls}">#{verb}</button></form>)
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
    def ingest_partition_lookup_widget(tenant_id, lag_rows: nil, action: nil)
      action ||= lag_path
      topic = KafkaBatch.config.fairness_ingest_topic
      result =
        if tenant_id.nil?
          ""
        else
          ingest_partition_lookup_result(tenant_id, topic, lag_rows)
        end

      <<~HTML
        <div class="card ingest-lookup">
          <h3>Ingest partition lookup</h3>
          <p class="muted">Fair jobs are keyed by <code>tenant_id</code> on <code>#{h(topic)}</code>. Uses Kafka's default murmur2 partitioner (same as the producer).</p>
          <form class="search" method="get" action="#{action}">
            <input type="text" name="tenant_id" value="#{h(tenant_id.to_s)}" placeholder="tenant_id (e.g. acme)" autocomplete="off">
            <button type="submit" class="btn">Lookup</button>
          </form>
          #{result}
        </div>
      HTML
    end

    def ingest_partition_lookup_result(tenant_id, topic, lag_rows)
      count = KafkaBatch.fairness_ingest_partition_count
      unless count
        return "<p class='muted'>Could not read partition count for <code>#{h(topic)}</code>.</p>"
      end

      partition = KafkaBatch::Partition.for_key(tenant_id, count)
      lag_row   = lag_rows&.find do |r|
        r[:topic] == topic && r[:partition].to_i == partition &&
          r[:group] == KafkaBatch.dispatch_consumer_group
      end
      lag_note  =
        if lag_row
          " Pending on this partition (dispatch group): #{lag_badge(lag_row[:lag])}."
        elsif lag_rows
          " No dispatch-group lag row for this partition (may be zero or not yet consumed)."
        end

      <<~HTML
        <p><strong>Tenant</strong> <code>#{h(tenant_id)}</code> → <strong>partition #{partition}</strong> of #{count} on <code>#{h(topic)}</code>.#{lag_note}</p>
      HTML
    end

    # Per-partition lag rows for a (group, topic), sorted by lag desc.
    def lag_partitions(group, topic)
      data  = KafkaBatch::Lag.read_group(group, [topic])
      parts = (data[group] || {})[topic] || {}
      parts.map { |partition, info| { partition: partition.to_i, lag: [info[:lag].to_i, 0].max } }
           .sort_by { |p| -p[:lag] }
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

    def summary_cards(counts, pending_jobs = nil)
      total = counts.values.sum
      cards = [["Total", total, "#111827"]]
      %w[running success complete cancelled].each do |s|
        cards << [s.capitalize, counts[s].to_i, STATUS_COLORS[s]]
      end
      # System-wide backlog: jobs pushed but not yet completed/failed.
      cards << ["Pending jobs", pending_jobs, STATUS_COLORS["pending"]] unless pending_jobs.nil?
      inner = cards.map do |label, value, color|
        "<div class='metric'><div class='metric-value' style='color:#{color}'>#{value}</div>" \
        "<div class='metric-label'>#{label}</div></div>"
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
      # #23: embed CSRF token as a hidden field on all destructive forms.
      "<form method='post' action='#{action}' onsubmit=\"return confirm('#{h(confirm)}')\" style='display:inline'>" \
      "#{csrf_hidden}<button type='submit' class='btn #{css}'>#{label}</button></form>"
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
      idx  = tenant_id.bytes.sum % TENANT_COLORS.size
      fg, bg = TENANT_COLORS[idx]
      "<span class='tenant-chip' style='color:#{fg};background:#{bg}'>#{h(tenant_id)}</span>"
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

    def fairness_path
      "#{@script_name}/fairness"
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

    def parse_query(qs)
      CGI.parse(qs.to_s).transform_values(&:first)
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

    def layout(title, body)
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
          <header><a href="#{index_path}" class="logo">KafkaBatch</a><span class="tag">batches</span></header>
          <main>#{body}</main>
        </body>
        </html>
      HTML
    end

    CSS = <<~CSS
      * { box-sizing: border-box; }
      body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
             background: #f3f4f6; color: #111827; }
      header { background: #111827; color: #fff; padding: 14px 24px; display: flex; align-items: baseline; gap: 10px; }
      header .logo { color: #fff; text-decoration: none; font-weight: 700; font-size: 18px; }
      header .tag { color: #9ca3af; font-size: 13px; }
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
      .toolbar { margin-bottom: 12px; }
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
      .search { display: flex; gap: 8px; margin-bottom: 12px; }
      .search input[type=text] { width: 280px; padding: 6px 12px; border: 1px solid #d1d5db;
                                 border-radius: 7px; font-size: 14px; }
      .detail th { width: 180px; color: #6b7280; vertical-align: top; }
      pre { white-space: pre-wrap; word-break: break-word; margin: 0; font-size: 12px; }
      h2 { margin-top: 0; }
      .tenant-chip { display:inline-block; font-size:11px; font-weight:600; padding:1px 8px;
                     border-radius:999px; white-space:nowrap; }
    CSS
  end
end
