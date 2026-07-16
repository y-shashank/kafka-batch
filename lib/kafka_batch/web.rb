# frozen_string_literal: true

require "cgi"
require "base64"
require "securerandom"
require "time"
require "stringio"
require "oj"

require_relative "web/json"
require_relative "web/assets"
require_relative "web/api"
require_relative "system_info"
require_relative "weight_shares"
require_relative "reconciler/run_summary"
require_relative "dlt/stats"
require_relative "dlt/reader"

module KafkaBatch
  # Rack app for the KafkaBatch ops dashboard (React SPA + JSON API).
  #
  #   mount KafkaBatch::Web => "/kafka_batch"
  #
  # Mount behind host authentication. Optional defence-in-depth:
  #   config.web_authenticator = ->(env) { ... }
  class Web
    PER_PAGE = 25
    BULK_MAX = 100
    BULK_ALL_MAX = 1000
    MAX_BODY_BYTES = 1_048_576
    WEIGHTS_MAX = 500
    FILTER_SCAN_MAX = BULK_ALL_MAX + 1
    VALID_STATUSES = %w[running success complete cancelled pending].freeze

    CSRF_COOKIE = "_kb_csrf"
    CSRF_FIELD  = "_csrf"
    CSRF_HEADER = "HTTP_X_CSRF_TOKEN"

    FAVICON_SVG = <<~SVG.freeze
      <svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32" fill="none">
        <defs><linearGradient id="kb-bg" x1="0" y1="0" x2="32" y2="32" gradientUnits="userSpaceOnUse">
          <stop offset="0" stop-color="#0F766E"/><stop offset="1" stop-color="#0369A1"/></linearGradient></defs>
        <rect width="32" height="32" rx="7" fill="url(#kb-bg)"/>
        <g stroke="#FFFFFF" stroke-width="1.9" stroke-linecap="round" fill="none">
          <path d="M11 16 Q16.5 16 21 8" opacity="0.72"/><path d="M11 16 H21" opacity="0.92"/>
          <path d="M11 16 Q16.5 16 21 24" opacity="0.72"/></g>
        <circle cx="10" cy="16" r="3.1" fill="#FFFFFF"/><circle cx="10" cy="16" r="1.3" fill="#0F766E"/>
        <rect x="21" y="6.4" width="4.6" height="3.2" rx="1" fill="#FFFFFF" opacity="0.80"/>
        <rect x="21" y="14.4" width="4.6" height="3.2" rx="1" fill="#FFFFFF"/>
        <rect x="21" y="22.4" width="4.6" height="3.2" rx="1" fill="#FFFFFF" opacity="0.80"/>
      </svg>
    SVG
    FAVICON_DATA_URI = "data:image/svg+xml;base64,#{Base64.strict_encode64(FAVICON_SVG)}".freeze

    TENANT_COLORS = [
      ["#1d4ed8", "#dbeafe"],
      ["#0f766e", "#ccfbf1"],
      ["#b45309", "#fef3c7"],
      ["#be185d", "#fce7f3"],
      ["#047857", "#d1fae5"],
      ["#4338ca", "#e0e7ff"],
      ["#b91c1c", "#fee2e2"],
      ["#0369a1", "#e0f2fe"],
      ["#6d28d9", "#ede9fe"],
      ["#9a3412", "#ffedd5"]
    ].freeze

    def self.call(env)
      new.call(env)
    end

    attr_reader :script_name, :csrf_token

    def call(env)
      @script_name = env["SCRIPT_NAME"].to_s
      method       = env["REQUEST_METHOD"]
      path         = env["PATH_INFO"].to_s
      path         = "/" if path.empty?
      @path        = path
      @secure      = request_secure?(env)
      params       = parse_query(env["QUERY_STRING"])

      unless web_authenticated?(env)
        return inject_csrf_cookie(unauthorized)
      end

      request_cookies = parse_cookies(env)
      @csrf_token     = request_cookies[CSRF_COOKIE] || SecureRandom.hex(16)

      if %w[POST PUT PATCH DELETE].include?(method)
        cookie_token = request_cookies[CSRF_COOKIE]
        submitted    = env[CSRF_HEADER] || body_csrf_token(env) || params[CSRF_FIELD]
        unless cookie_token && submitted && secure_compare(submitted, cookie_token)
          return inject_csrf_cookie(csrf_forbidden)
        end
        @csrf_token = cookie_token
      end

      response =
        if path.start_with?("/api/")
          Api.new(self).call(method, path, params, env)
        elsif method == "GET" && (static = Assets.serve_file(path))
          static
        elsif method == "GET"
          Assets.spa_shell(
            script_name: @script_name,
            csrf_token: @csrf_token,
            favicon_data_uri: FAVICON_DATA_URI
          )
        else
          Json.error(404, "Not found")
        end

      audit_web_action!(env, path, params, response) if %w[POST PUT PATCH DELETE].include?(method)
      inject_csrf_cookie(response)
    rescue StandardError => e
      audit_web_action!(env, path, params, nil, error: e.message) if %w[POST PUT PATCH DELETE].include?(method)
      KafkaBatch.logger.error(
        "[KafkaBatch][Web] #{e.class}: #{e.message}\n#{e.backtrace&.first(8)&.join("\n")}"
      )
      inject_csrf_cookie(Json.error(500, "An internal error occurred. Check server logs."))
    end

    # ── helpers used by Api ─────────────────────────────────────────────────

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

    def pending(b)
      return 0 if %w[success complete cancelled].include?(b[:status])

      [b[:total_jobs].to_i - b[:touched_count].to_i, 0].max
    end

    def retry_lag_by_tier
      return nil unless KafkaBatch::Lag.available?

      parts = KafkaBatch::Lag.partitions
      KafkaBatch.config.retry_tiers.keys.each_with_object({}) do |tier, h|
        topic = KafkaBatch.config.retry_topic_for(tier)
        h[tier] = parts.select { |r| r[:topic] == topic }.sum { |r| r[:lag].to_i }
      end
    rescue StandardError => e
      KafkaBatch.logger.warn("[KafkaBatch::Web] retry_lag_by_tier failed: #{e.message}")
      nil
    end

    def retry_lag
      retry_lag_by_tier&.values&.sum
    end

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

    def fairness_ready_lag_partitions(type)
      cfg = KafkaBatch.config
      rows = []
      if cfg.runtime_split_fair_ready?(type)
        ruby_topic = cfg.fairness_ready_topic(type, :ruby)
        lag_partitions(KafkaBatch.jobs_fair_consumer_group(type), ruby_topic).each do |p|
          rows << p.merge(topic: ruby_topic, runtime: :ruby)
        end
        go_topic = cfg.fairness_ready_topic(type, :go)
        go_group = KafkaBatch.go_worker_fair_ready_consumer_group(type)
        lag_partitions(go_group, go_topic).each do |p|
          rows << p.merge(topic: go_topic, runtime: :go)
        end
      else
        ready_topic = cfg.fairness_ready_topic(type)
        lag_partitions(KafkaBatch.jobs_fair_consumer_group(type), ready_topic).each do |p|
          rows << p.merge(topic: ready_topic)
        end
      end
      rows.sort_by { |p| -p[:lag] }
    end

    def fairness_ready_topics_description_text(type)
      cfg = KafkaBatch.config
      if cfg.runtime_split_fair_ready?(type)
        "#{cfg.fairness_ready_topic(type, :go)} (Go) and #{cfg.fairness_ready_topic(type, :ruby)} (Ruby)"
      else
        cfg.fairness_ready_topic(type).to_s
      end
    end

    def schedule_log_group?(group)
      group.to_s.end_with?("-schedule")
    end

    def lag_pause_tooltip(refresh_seconds)
      secs = refresh_seconds.to_i
      secs = 30 if secs <= 0
      "Pause/resume is written immediately, but Karafka consumers cache pause state " \
        "for up to #{secs}s (consumption_control_refresh_interval). Processing may " \
        "continue briefly after you click Pause; lag can take up to #{secs}s to stop falling."
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
        note = "Processed first #{BULK_ALL_MAX} of #{BULK_ALL_MAX}+ matching batches; run again for the rest."
        [all.first(BULK_ALL_MAX), note]
      else
        [all, nil]
      end
    end

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

    def bulk_batch_ids(params)
      ids = params["batch_ids"]
      ids =
        case ids
        when Array then ids
        when nil, "" then []
        else [ids]
        end
      ids.map(&:to_s).reject(&:empty?).uniq.first(BULK_MAX)
    end

    def scalarize_params(hash)
      hash.transform_values do |v|
        next v unless v.is_a?(Array)
        # Keep multi-select fields (batch_ids) as arrays even when size == 1.
        v
      end
    end

    def form_param(params, key)
      v = params[key]
      v = v.first if v.is_a?(Array) && v.size == 1
      return nil if v.is_a?(Array)

      non_empty(v.to_s)
    end

    def non_empty(v)
      v.nil? || v.to_s.empty? ? nil : v.to_s
    end

    def valid_status(v)
      s = non_empty(v)
      s if s && VALID_STATUSES.include?(s)
    end

    def parse_query(qs)
      CGI.parse(qs.to_s).transform_values(&:first)
    end

    def body_params_multi(env)
      input = env["rack.input"]
      return {} unless input

      ctype = env["CONTENT_TYPE"].to_s
      return {} if ctype.include?("application/json")

      input.rewind
      raw = input.read(MAX_BODY_BYTES + 1)
      input.rewind
      return {} if raw.bytesize > MAX_BODY_BYTES

      CGI.parse(raw.to_s)
    rescue StandardError
      {}
    end

    def body_params(env)
      body_params_multi(env).transform_values { |v| v.is_a?(Array) ? v.first : v }
    end

    def json_body_params(env)
      input = env["rack.input"]
      return {} unless input

      ctype = env["CONTENT_TYPE"].to_s
      return {} unless ctype.include?("application/json")

      input.rewind
      raw = input.read(MAX_BODY_BYTES + 1).to_s
      input.rewind
      return {} if raw.bytesize > MAX_BODY_BYTES || raw.strip.empty?

      parsed = Oj.load(raw, mode: :compat)
      return {} unless parsed.is_a?(Hash)

      parsed.transform_keys(&:to_s)
    rescue StandardError
      {}
    end

    def tenant_colors(tenant_id)
      idx = tenant_id.to_s.bytes.sum % TENANT_COLORS.size
      TENANT_COLORS[idx]
    end

    def audit_metadata_preview(metadata, limit: 160)
      return "—" if metadata.nil? || (metadata.respond_to?(:empty?) && metadata.empty?)

      str = metadata.is_a?(String) ? metadata : Oj.dump(metadata, mode: :compat)
      str.length > limit ? "#{str[0, limit]}…" : str
    rescue StandardError
      metadata.to_s
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

    def fmt_eta(value)
      return "—" if value.nil? || (value.respond_to?(:empty?) && value.empty?)

      t = value.respond_to?(:to_time) ? value.to_time : Time.parse(value.to_s)
      secs = (t - Time.now).round
      return "due now" if secs <= 0

      d = secs / 86_400
      secs %= 86_400
      hh = secs / 3_600
      secs %= 3_600
      mm = secs / 60
      ss = secs % 60
      parts = []
      parts << "#{d}d" if d.positive?
      parts << "#{hh}h" if hh.positive?
      parts << "#{mm}m" if mm.positive?
      parts << "#{ss}s" if ss.positive?
      "in #{parts.first(2).join(' ')}"
    rescue StandardError
      "—"
    end

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
      value.to_s
    end

    def fmt_mem_text(bytes)
      return "—" if bytes.nil?

      b = bytes.to_i
      return "—" if b <= 0
      if b >= 1_073_741_824
        "#{(b / 1_073_741_824.0).round(1)} GB"
      elsif b >= 1_048_576
        "#{(b / 1_048_576.0).round(1)} MB"
      else
        "#{(b / 1024.0).round(0)} KB"
      end
    end

    private

    def inject_csrf_cookie(response)
      status, headers, body = response
      headers = headers.dup
      path_scope = @script_name.empty? ? "/" : "#{@script_name}/"
      attrs = ["#{CSRF_COOKIE}=#{csrf_token}", "Path=#{path_scope}", "SameSite=Strict", "HttpOnly"]
      attrs << "Secure" if @secure
      cookie = attrs.join("; ")
      existing = headers["set-cookie"]
      headers["set-cookie"] =
        case existing
        when nil, "" then cookie
        when Array   then existing + [cookie]
        else "#{existing}\n#{cookie}"
        end
      [status, headers, body]
    end

    def request_secure?(env)
      return true if env["HTTPS"].to_s.casecmp?("on")
      return true if env["rack.url_scheme"].to_s.casecmp?("https")
      return true if env["HTTP_X_FORWARDED_PROTO"].to_s.split(",").first.to_s.strip.casecmp?("https")
      return true if env["HTTP_X_FORWARDED_SSL"].to_s.casecmp?("on")

      false
    end

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

    def body_csrf_token(env)
      json = json_body_params(env)
      return json[CSRF_FIELD] if json[CSRF_FIELD]

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

    def web_authenticated?(env)
      auth = KafkaBatch.config.respond_to?(:web_authenticator) ? KafkaBatch.config.web_authenticator : nil
      return true if auth.nil?

      !!auth.call(env)
    rescue StandardError => e
      KafkaBatch.logger.error("[KafkaBatch][Web] web_authenticator raised #{e.class}: #{e.message} — denying request")
      false
    end

    def unauthorized
      Json.error(401, "Unauthorized").tap do |resp|
        resp[1] = resp[1].merge("www-authenticate" => 'Basic realm="kafka-batch"')
      end
    end

    def csrf_forbidden
      Json.error(403, "CSRF check failed. Refresh and try again.")
    end

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
  end
end
