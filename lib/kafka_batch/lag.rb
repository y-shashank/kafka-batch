module KafkaBatch
  # Kafka consumer-group lag (a.k.a. pending messages) per topic/partition.
  #
  # Backed by Karafka::Admin, which reads committed offsets and high-watermarks
  # straight from the cluster. Read-only and best-effort: if Karafka/Admin isn't
  # loaded (e.g. a pure web process) or the cluster can't be reached, the methods
  # degrade gracefully so the dashboard never breaks.
  #
  # "Lag" == messages produced to a partition that the consumer group hasn't
  # committed yet == pending work for that topic/partition.
  module Lag
    module_function

    def available?
      # Try to load Karafka if it's not already required. Karafka is a
      # kafka-batch dependency so it's always installed, but a UI-only service
      # that never runs karafka.rb may not have required it yet.
      unless defined?(Karafka)
        begin
          require "karafka"
        rescue LoadError
          return false
        end
      end

      return false unless defined?(Karafka::Admin) &&
                          Karafka::Admin.respond_to?(:read_lags_with_offsets)

      # In a UI-only process karafka.rb is never executed, so Karafka may be
      # loaded but not configured with broker settings. Provide a minimal setup
      # from KafkaBatch.config so Karafka::Admin.read_lags_with_offsets works.
      KafkaBatch.ensure_karafka_configured!
      true
    rescue StandardError
      false
    end

    # Per-partition lag/offset rows across all routed consumer groups.
    #
    # @return [Array<Hash>] each: {
    #   group:, topic:, partition:, committed:, end_offset:, lag:, never_consumed:
    # } sorted by group, topic, partition. `committed`/`end_offset` are nil for a
    # partition the group has never committed to (reported by Kafka as -1).
    def partitions
      return [] unless available?

      rows = []
      read.each do |group, topics|
        topics.each do |topic, parts|
          parts.each do |partition, info|
            committed = info[:offset].to_i
            lag       = info[:lag].to_i
            consumed  = committed >= 0
            rows << {
              group:          group.to_s,
              topic:          topic.to_s,
              partition:      partition.to_i,
              committed:      consumed ? committed : nil,
              lag:            lag.negative? ? 0 : lag,
              end_offset:     (consumed && !lag.negative?) ? committed + lag : nil,
              never_consumed: !consumed
            }
          end
        end
      end
      rows.sort_by { |r| [r[:group], r[:topic], r[:partition]] }
    end

    # Per-topic aggregation: total pending + partition count per (group, topic).
    # @return [Array<Hash>] each: { group:, topic:, partitions:, lag: }
    def topics(rows = partitions)
      rows
        .group_by { |r| [r[:group], r[:topic]] }
        .map do |(group, topic), prs|
          { group: group, topic: topic, partitions: prs.size, lag: prs.sum { |r| r[:lag] } }
        end
        .sort_by { |r| [r[:group], r[:topic]] }
    end

    # Total pending messages across everything.
    def total(rows = partitions)
      rows.sum { |r| r[:lag] }
    end

    # Read committed lag for a specific consumer group + topics.
    # @return [Hash] { group => { topic => { partition => { offset:, lag: } } } }
    def read_group(group, topics)
      return {} unless available?

      Karafka::Admin.read_lags_with_offsets({ group => topics })
    end

    # @api private
    # Read lags ONLY for this gem's consumer groups, so the dashboard never
    # reports on the host app's unrelated topics.
    def read
      groups = gem_groups_with_topics
      return {} if groups.empty?

      Karafka::Admin.read_lags_with_offsets(groups)
    end

    # @api private
    # Returns { consumer_group_id => [topic_names] } for every KafkaBatch
    # consumer group.
    #
    # Two-path resolution — no dependency on the worker registry:
    #
    # 1. Route-based (preferred): when Karafka routes owned by this gem exist in
    #    this process (e.g. the Karafka server with draw_routes called), read
    #    directly from Karafka::App.routes filtered by the gem's group prefix.
    #    This is exact — only groups with at least one registered topic appear.
    #
    # 2. Config-based fallback: when NO gem-prefixed routes are found — e.g. a
    #    UI-only process where karafka.rb exists (for admin access) but draws no
    #    KafkaBatch consumer routes. Derives the full topic set from config.
    #    All groups (control, priority, fairness, jobs) are included because
    #    their topics are always provisioned (see topics.rb).
    #
    # NOTE: We gate on gem-owned routes specifically, not just Karafka::App.routes.any?,
    # because a UI-only service may have a karafka.rb with an empty routes block
    # (needed to configure Karafka::Admin) but no KafkaBatch consumer groups.
    # In that case routes.any? is truthy but the select returns nothing — we must
    # fall through to the config-based path.
    #
    # @return [Hash<String, Array<String>>]
    def gem_groups_with_topics
      cfg    = KafkaBatch.config
      prefix = cfg.consumer_group   # e.g. "kafka-batch"

      own_routes =
        if defined?(Karafka::App)
          begin
            Karafka::App.routes.select { |r| r.id == prefix || r.id.start_with?("#{prefix}-") }
          rescue StandardError
            []
          end
        else
          []
        end

      if own_routes.any?
        # Route-based: gem-owned consumer groups found in this process's routes.
        own_routes.each_with_object({}) { |r, h| h[r.id] = r.topics.map(&:name) }
      else
        # Config-based fallback: no gem routes drawn here (UI-only service or
        # process where karafka.rb runs but doesn't call draw_routes).
        config_based_groups(cfg, prefix)
      end
    end

    # @api private
    def config_based_groups(cfg, prefix)
      groups = {}

      # Control plane — always present.
      groups["#{prefix}-control"] =
        [cfg.events_topic, cfg.callbacks_topic].compact + Array(cfg.retry_topics)

      # Priority groups — from YAML config when paths are set.
      registry = KafkaBatch::Priority::Registry.load(
        cfg.resolved_priority_config_paths, cfg: cfg
      )
      registry.configs.each do |prio|
        groups[prio.consumer_group] = prio.topics
      end

      # Fair lanes — each lane has its OWN dispatch / jobs-fair groups.
      KafkaBatch::Configuration::FAIRNESS_TYPES.each do |t|
        ingest = cfg.fairness_ingest_topic(t)
        groups["#{prefix}-dispatch-#{t}"] = [ingest] if ingest && !ingest.to_s.empty?
        if cfg.runtime_split_fair_ready?(t)
          ruby_ready = cfg.fairness_ready_topic(t, :ruby)
          go_ready   = cfg.fairness_ready_topic(t, :go)
          groups["#{prefix}-jobs-fair-#{t}"] = [ruby_ready] if ruby_ready && !ruby_ready.to_s.empty?
          go_group = "#{prefix}-go-worker-fair-ready-#{t}"
          groups[go_group] = [go_ready] if go_ready && !go_ready.to_s.empty?
        else
          ready = cfg.fairness_ready_topic(t)
          groups["#{prefix}-jobs-fair-#{t}"] = [ready] if ready && !ready.to_s.empty?
        end
      end

      # Plain jobs group — the default topic PLUS any custom plain-worker topics.
      # draw_routes was never called in this process, so recover the customs from
      # (a) the worker registry when the full backend happens to be loaded, and
      # (b) config.extra_job_topics (the reliable path for a pure UI-only service
      # that never loads worker classes).
      reserved = registry.reserved_topics
      groups["#{prefix}-jobs"] =
        ([cfg.jobs_topic] + registry_job_topics(reserved) + Array(cfg.extra_job_topics))
        .compact.uniq

      groups.reject { |_, topics| topics.empty? }
    end

    # Custom plain-worker topics recovered from the in-process worker registry,
    # if the full backend is loaded (UI-only processes require "kafka_batch/ui"
    # and have no registry — respond_to? guards that). Fair workers own no topic;
    # priority topics belong to priority YAML groups, so both are excluded.
    # @api private
    def registry_job_topics(priority)
      return [] unless KafkaBatch.respond_to?(:workers)

      KafkaBatch.workers
                .reject { |w| w.respond_to?(:fairness?) && w.fairness? }
                .map    { |w| begin; w.kafka_topic; rescue StandardError; nil; end }
                .compact
                .reject { |t| priority.include?(t) }
    rescue StandardError
      []
    end
  end
end
