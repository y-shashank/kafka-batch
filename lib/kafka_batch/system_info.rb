require "uri"

module KafkaBatch
  # Read-only snapshot of KafkaBatch configuration for the /system dashboard.
  # Sensitive values (passwords, secrets, SASL keys) are masked before display.
  module SystemInfo
    SENSITIVE_KEY = /password|secret|token|api[-_]?key|credential|private|sasl|ssl|cert/i.freeze
    MASK = "***".freeze

    Section = Struct.new(:id, :title, :icon, :accent, :rows, :wide, keyword_init: true)
    Row     = Struct.new(:label, :value, :masked, keyword_init: true)

    class << self
      def sections(config = KafkaBatch.config)
        [
          overview_section(config),
          kafka_section(config),
          redis_section(config),
          mysql_section(config),
          super_fetch_section(config),
          uniqueness_section(config),
          liveness_section(config),
          fairness_section(config),
          schedule_section(config),
          retry_section(config),
          reconciliation_section(config),
          cancellation_section(config),
          priority_section(config),
          retention_section(config),
          performance_section(config),
          metrics_section(config),
          audit_section(config),
          ai_section(config),
          rdkafka_section("Producer", config.producer_config, accent: "#8b5cf6"),
          rdkafka_section("Consumer", config.consumer_config, accent: "#ec4899")
        ].compact
      end

      def mask_redis_url(url)
        return "—" if url.nil? || url.to_s.empty?

        uri = URI.parse(url.to_s)
        if uri.password
          user = uri.user ? "#{uri.user[0]}***" : nil
          uri.user     = user
          uri.password = MASK
        end
        uri.to_s
      rescue URI::InvalidURIError
        url.to_s.sub(/:\/\/([^:@\/]+):([^@\/]+)@/, '://\1:***@')
      end

      def mask_config_value(key, value)
        return "—" if value.nil?
        return MASK if sensitive_key?(key)
        return MASK if value.is_a?(String) && value.match?(/BEGIN (RSA |EC )?PRIVATE KEY/)

        case value
        when Hash  then value.map { |k, v| "#{k}: #{mask_config_value(k, v)}" }.join(", ")
        when Array then value.map { |v| mask_config_value(key, v) }.join(", ")
        else value.to_s
        end
      end

      def sensitive_key?(key)
        key.to_s.match?(SENSITIVE_KEY)
      end

      def fmt_bool(v)
        v ? "yes" : "no"
      end

      def fmt_duration(seconds)
        s = seconds.to_i
        return "—" if s <= 0

        parts = []
        parts << "#{s / 86_400}d" if s >= 86_400
        parts << "#{(s % 86_400) / 3600}h" if s >= 3600
        parts << "#{(s % 3600) / 60}m" if s >= 60 && s < 86_400
        parts << "#{s % 60}s" if s < 3600 || (s % 60).positive?
        "#{parts.join(' ')} (#{s}s)"
      end

      private

      def row(label, value, masked: false)
        Row.new(label: label, value: value.to_s, masked: masked)
      end

      def overview_section(config)
        Section.new(
          id: "overview", title: "Overview", icon: "◆", accent: "#111827",
          rows: [
            row("Gem version", KafkaBatch::VERSION),
            row("Store", config.store),
            row("Store DB connection", fmt_connection(config.store_database_connection)),
            row("Topic prefix", config.topic_prefix.to_s.empty? ? "(none)" : config.topic_prefix),
            row("Execution mode", config.execution_mode),
            row("Daemon mode", fmt_bool(config.daemon_mode)),
            row("Handler manifest", blank_or(config.handler_manifest_path)),
            row("Web authenticator", config.web_authenticator ? "set" : "none")
          ]
        )
      end

      def kafka_section(config)
        topic_rows = KafkaBatch::Configuration::PREFIXED_SETTINGS.map do |name, _base|
          row(name.to_s.tr("_", " "), config.public_send(name))
        end

        Section.new(
          id: "kafka", title: "Kafka", icon: "▦", accent: "#2563eb", wide: true,
          rows: [
            row("Brokers", Array(config.brokers).join(", ")),
            *topic_rows,
            row("Extra job topics", list_or_none(config.extra_job_topics)),
            row("Jobs topics (Go lag)", list_or_none(config.jobs_topics)),
            row("Topics replication factor", config.topics_replication_factor),
            row("Validate topics on boot", fmt_bool(config.validate_topics_on_boot))
          ]
        )
      end

      def super_fetch_section(config)
        claim = config.super_fetch_claim_window.to_i
        claim_label =
          if claim <= 0
            "auto (2× concurrency = #{config.super_fetch_concurrency.to_i * 2})"
          else
            claim
          end
        Section.new(
          id: "super_fetch", title: "SuperFetch", icon: "⚡", accent: "#ea580c",
          rows: [
            row("Concurrency", config.super_fetch_concurrency),
            row("Claim window", claim_label),
            row("Recommended Redis pool", config.recommended_redis_pool_size),
            row("Lease TTL", fmt_duration(config.super_fetch_lease_ttl)),
            row("Orphan grace", fmt_duration(config.super_fetch_orphan_grace)),
            row("Reclaim enabled", fmt_bool(config.super_fetch_reclaim_enabled)),
            row("Reclaim interval", fmt_duration(config.super_fetch_reclaim_interval)),
            row("Reclaim limit", config.super_fetch_reclaim_limit),
            row("Drain timeout", fmt_duration(config.super_fetch_drain_timeout))
          ]
        )
      end

      def uniqueness_section(config)
        Section.new(
          id: "uniqueness", title: "Uniqueness", icon: "➀", accent: "#4f46e5",
          rows: [
            row("Enabled", fmt_bool(config.uniq_enabled)),
            row("Lock TTL", fmt_duration(config.uniq_lock_ttl)),
            row("On duplicate", config.uniq_on_duplicate)
          ]
        )
      end

      def schedule_section(config)
        Section.new(
          id: "schedule", title: "Scheduled jobs", icon: "⏱", accent: "#0d9488",
          rows: [
            row("Schedule store", config.schedule_store),
            row("Schedule DB connection", fmt_connection(config.schedule_store_database_connection)),
            row("Poller enabled", fmt_bool(config.schedule_poller_enabled)),
            row("Poll interval", "#{config.schedule_poll_interval}s"),
            row("Poll max interval", "#{config.schedule_poll_max_interval}s"),
            row("Poll jitter", "±#{(config.schedule_poll_jitter.to_f * 100).round}%"),
            row("Batch size", config.schedule_batch_size),
            row("Lease seconds", fmt_duration(config.schedule_lease_seconds)),
            row("Reclaim interval", fmt_duration(config.schedule_reclaim_interval)),
            row("Max schedule horizon", fmt_duration(config.max_schedule_horizon)),
            row("Index write retries", config.schedule_index_write_retries),
            row("Index write backoff", "#{config.schedule_index_write_backoff}s")
          ]
        )
      end

      def redis_section(config)
        rows = [row("Configured", fmt_bool(config.redis_configured?))]
        return Section.new(id: "redis", title: "Redis", icon: "⬡", accent: "#dc2626", rows: rows) unless config.redis_configured?

        if config.redis_url_raw
          rows << row("Source", "redis_url")
          rows << row("URL", mask_redis_url(config.redis_url_raw), masked: config.redis_url_raw.to_s.include?("@"))
        elsif config.redis.is_a?(Hash)
          rows << row("Source", "redis hash")
          h = config.redis
          %i[host port db namespace driver id location].each do |key|
            next unless h.key?(key)
            val = h[key]
            masked = sensitive_key?(key)
            rows << row(key.to_s, masked ? MASK : val, masked: masked)
          end
          if h[:password]
            rows << row("password", MASK, masked: true)
          end
          if h[:username]
            rows << row("username", h[:username].to_s.empty? ? "—" : h[:username])
          end
          rows << row("Display URL", mask_redis_url(config.redis_url))
        end

        rows << row("Pool size", config.redis_pool_size)
        rows << row("Batch TTL", fmt_duration(config.batch_ttl))
        rows << row("All-index max size", config.all_index_max_size)

        Section.new(id: "redis", title: "Redis", icon: "⬡", accent: "#dc2626", rows: rows)
      end

      def mysql_section(config)
        return nil unless config.store == :mysql

        rows = [
          row("Role", "failures, consumption pauses"),
          row("Batch ledger", "Redis (shared with :redis store)"),
          row("Failures TTL", fmt_duration(config.failures_ttl))
        ]

        if defined?(ActiveRecord::Base)
          begin
            db_config = ActiveRecord::Base.connection_db_config
            hash      = db_config.configuration_hash
            %i[adapter host port database username].each do |key|
              rows << row(key.to_s, hash[key]) if hash.key?(key)
            end
            rows << row("password", MASK, masked: true) if hash[:password]
            rows << row("pool", hash[:pool]) if hash.key?(:pool)
          rescue StandardError => e
            rows << row("Connection", "unavailable (#{e.class})")
          end
        else
          rows << row("Connection", "ActiveRecord not loaded")
        end

        Section.new(id: "mysql", title: "MySQL", icon: "🗄", accent: "#0891b2", rows: rows)
      end

      def liveness_section(config)
        Section.new(
          id: "liveness", title: "Liveness", icon: "▶", accent: "#0ea5e9",
          rows: [
            row("Backend", config.liveness_backend),
            row("Track running jobs", fmt_bool(config.track_running_jobs)),
            row("TTL", fmt_duration(config.liveness_ttl)),
            row("Heartbeat interval", fmt_duration(config.liveness_heartbeat_interval)),
            row("Stats interval", config.liveness_stats_interval.zero? ? "off" : fmt_duration(config.liveness_stats_interval)),
            row("Consumption control refresh", fmt_duration(config.consumption_control_refresh_interval))
          ]
        )
      end

      def fairness_section(config)
        slot_dedup = config.fairness_slot_dedup_ttl.to_i
        slot_dedup_label =
          if slot_dedup <= 0
            "lease TTL (#{fmt_duration(config.fairness_lease_ttl)})"
          else
            fmt_duration(slot_dedup)
          end
        pinned = config.fairness_tenant_partitions
        pinned_label =
          if pinned.nil? || pinned.empty?
            "none"
          else
            pinned.map { |k, v| "#{k}→#{v}" }.join(", ")
          end

        Section.new(
          id: "fairness", title: "Fairness", icon: "⚖", accent: "#7c3aed", wide: true,
          rows: [
            row("Lanes", "per-worker fairness_type (:time / :throughput)"),
            row("Runtime-split ready (time)", fmt_bool(config.runtime_split_fair_ready?(:time))),
            row("Runtime-split ready (throughput)", fmt_bool(config.runtime_split_fair_ready?(:throughput))),
            row("Global concurrency (per lane)", config.fairness_global_concurrency),
            row("Max inflight / tenant", config.fairness_max_inflight_per_tenant.zero? ? "dynamic fair share" : config.fairness_max_inflight_per_tenant),
            row("Weighted concurrency", fmt_bool(config.fairness_weighted_concurrency)),
            row("Lease TTL", fmt_duration(config.fairness_lease_ttl)),
            row("Forwarding recovery grace", "#{config.fairness_forwarding_recovery_grace}s"),
            row("Slot dedup TTL", slot_dedup_label),
            row("Active count TTL", fmt_duration(config.fairness_active_count_ttl)),
            row("Active count source", config.fairness_active_count_source),
            row("Ready window", config.fairness_ready_window),
            row("Default weight", config.fairness_default_weight),
            row("Weight cache TTL", fmt_duration(config.fairness_weight_cache_ttl)),
            row("Forwarder idle sleep", "#{config.fairness_forwarder_idle_sleep}s"),
            row("Dispatcher batch size", config.fairness_dispatcher_batch_size),
            row("Dispatcher concurrency (hint)", config.fairness_dispatcher_concurrency),
            row("Min ingest partitions", config.fairness_min_ingest_partitions),
            row("Dynamic tenant partitions", fmt_bool(config.fairness_dynamic_tenant_partitions)),
            row("Tenant partition cache TTL", fmt_duration(config.fairness_tenant_partition_cache_ttl)),
            row("Pinned tenant partitions", pinned_label)
          ]
        )
      end

      def retry_section(config)
        tiers = config.retry_tiers.map { |tier, secs| "#{tier}: #{fmt_duration(secs)}" }.join("; ")

        Section.new(
          id: "retry", title: "Retry & events", icon: "↻", accent: "#f59e0b",
          rows: [
            row("Max retries", config.max_retries),
            row("Retry jitter", "±#{(config.retry_jitter.to_f * 100).round}%"),
            row("Retry tiers", tiers),
            row("Tier progression", config.retry_tier_progression.join(" → ")),
            row("Retry max pause", fmt_duration(config.retry_max_pause_seconds)),
            row("Event emit retries", config.event_emit_retries),
            row("Event emit backoff", fmt_duration(config.event_emit_backoff))
          ]
        )
      end

      def reconciliation_section(config)
        Section.new(
          id: "reconciliation", title: "Reconciliation", icon: "⟳", accent: "#059669",
          rows: [
            row("Interval", fmt_duration(config.reconciliation_interval)),
            row("Lock TTL", fmt_duration(config.reconciler_lock_ttl)),
            row("Max per run", config.max_reconcile_per_run)
          ]
        )
      end

      def cancellation_section(config)
        Section.new(
          id: "cancellation", title: "Cancellation", icon: "⊘", accent: "#6b7280",
          rows: [
            row("Skip cancelled jobs", fmt_bool(config.skip_cancelled_jobs)),
            row("Cache TTL", fmt_duration(config.cancellation_cache_ttl))
          ]
        )
      end

      def priority_section(config)
        paths = config.resolved_priority_config_paths
        rows  = [
          row("Config paths", paths.empty? ? "(none)" : paths.join(", ")),
          row("Lag check interval", fmt_duration(config.priority_lag_check_interval)),
          row("Weighted interleave", config.priority_weighted_interleave.to_s)
        ]
        if paths.any?
          begin
            registry = KafkaBatch::Priority::Registry.load(paths, cfg: config)
            registry.configs.each do |c|
              rows << row(c.consumer_group, "#{c.mode} — #{c.topics.join(' → ')}")
            end
          rescue StandardError => e
            rows << row("Load error", e.message)
          end
        end
        Section.new(
          id: "priority", title: "Priority queues", icon: "⇅", accent: "#db2777",
          rows: rows
        )
      end

      def retention_section(config)
        Section.new(
          id: "retention", title: "Retention & limits", icon: "📦", accent: "#64748b",
          rows: [
            row("Batch TTL", fmt_duration(config.batch_ttl)),
            row("Failures TTL", fmt_duration(config.failures_ttl)),
            row("Max failures per batch", config.max_failures_per_batch.to_i.zero? ? "unlimited" : config.max_failures_per_batch),
            row("Retry cancel TTL", fmt_duration(config.retry_cancel_ttl)),
            row("All-index max", config.all_index_max_size),
            row("Max message bytes", config.max_message_bytes.to_i.zero? ? "disabled" : config.max_message_bytes),
            row("Push-many chunk size", config.push_many_chunk_size)
          ]
        )
      end

      def performance_section(config)
        Section.new(
          id: "performance", title: "Performance metrics", icon: "📈", accent: "#0284c7",
          rows: [
            row("Enabled", fmt_bool(config.performance_metrics_enabled)),
            row("Retention", fmt_duration(config.performance_metrics_retention)),
            row("Max job types", config.performance_metrics_max_job_types),
            row("Bucket seconds", config.performance_metrics_bucket_seconds),
            row("Sample rate", config.performance_metrics_sample_rate)
          ]
        )
      end

      def metrics_section(config)
        Section.new(
          id: "metrics", title: "Instrumentation metrics", icon: "📡", accent: "#9333ea",
          rows: [
            row("Enabled", fmt_bool(config.metrics_enabled)),
            row("Adapter", config.metrics_adapter),
            row("Prefix", config.metrics_prefix),
            row("Client", config.metrics_client ? "set" : "none"),
            row("Proc", config.metrics_proc ? "set" : "none")
          ]
        )
      end

      def audit_section(config)
        Section.new(
          id: "audit", title: "Audit log", icon: "✎", accent: "#a16207",
          rows: [
            row("Enabled", fmt_bool(config.audit_enabled)),
            row("DB connection", fmt_connection(config.audit_database_connection)),
            row("Actor", config.audit_actor ? "set" : "none")
          ]
        )
      end

      def ai_section(config)
        salt = config.ai_encryption_salt.to_s
        Section.new(
          id: "ai", title: "AI assistant", icon: "✦", accent: "#be185d",
          rows: [
            row("Knowledge enabled", fmt_bool(config.ai_knowledge_enabled)),
            row("Encryption salt", salt.empty? ? "unset" : MASK, masked: !salt.empty?),
            row("Chat history max lines", config.ai_chat_history_max_lines),
            row("Chat context chunks", config.ai_chat_context_chunks),
            row("Default OpenRouter model", blank_or(config.ai_openrouter_default_model))
          ]
        )
      end

      def rdkafka_section(title, hash, accent:)
        return nil if hash.nil? || hash.empty?

        rows = hash.map do |key, value|
          masked = sensitive_key?(key)
          row(key.to_s, masked ? MASK : mask_config_value(key, value), masked: masked)
        end

        Section.new(
          id: title.downcase, title: "#{title} (rdkafka)", icon: "⚙", accent: accent, rows: rows
        )
      end

      def blank_or(value, empty: "(none)")
        s = value.to_s.strip
        s.empty? ? empty : s
      end

      def list_or_none(list)
        arr = Array(list).map(&:to_s).reject(&:empty?)
        arr.empty? ? "(none)" : arr.join(", ")
      end

      def fmt_connection(value)
        case value
        when nil then "default"
        when Symbol then value.to_s
        when Class then value.name
        when Hash then mask_config_value("connection", value)
        else value.to_s.empty? ? "default" : value.to_s
        end
      end
    end
  end
end
