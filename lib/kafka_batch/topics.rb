module KafkaBatch
  # Declarative provisioning for the Kafka topics the gem uses — the Kafka
  # equivalent of a database migration.
  #
  # Kafka has no schema/migration system, so topic creation is normally a manual
  # ops step (or relies on auto-create, which is discouraged in production because
  # it can't control partition counts). This module derives the full topic set
  # from the current configuration and creates any that are missing, idempotently.
  #
  #   KafkaBatch::Topics.create_all!                 # sensible per-topic defaults
  #   KafkaBatch::Topics.create_all!(partitions: 12) # force every topic to 12
  #
  # Usually invoked via `rake kafka_batch:create_topics` (see the Railtie).
  module Topics
    module_function

    # Per-topic default partition counts (used when the caller doesn't force a
    # single count). Sized for a mid-size enterprise swarm (~150 Karafka pods ×
    # concurrency 10). Rule of thumb: execution topics (jobs, priority, fair
    # ready) ≈ peak_pods × concurrency; events ≈ execution / 16; fair ingest ≈
    # max concurrent tenants. Override via create_all!(partitions: N) or the
    # PARTITIONS env in bin/create_kafka_topics.sh before first deploy — Kafka
    # cannot shrink partitions later.
    DEFAULT_PARTITIONS = {
      jobs:        768,  # plain worker topics: pods × concurrency
      priority:    768, # fast/slow p0+p1 (override per topic if needed)
      events:      48,  # completion events; keep control pods small (3–6)
      callbacks:   6,
      retry:       12,  # per tier
      scheduled:   48,  # perform_in/at payload store; poller reads by partition
      dead_letter: 3,
      ingest:      300, # fairness ingest (per lane): ≈ max unique tenants on fair lane
      ready:       768  # fairness ready (per lane): pods × concurrency
    }.freeze

    # Kafka topic configs applied at creation time. Scheduled retention must
    # exceed max_schedule_horizon so a delayed pointer never references a
    # log-cleaned offset; dead_letter gets a long audit retention.
    SCHEDULE_RETENTION_BUFFER_SEC = 86_400 # 1 day slack beyond max_schedule_horizon

    def scheduled_retention_ms(cfg = KafkaBatch.config)
      horizon = cfg.max_schedule_horizon.to_i
      [(horizon + SCHEDULE_RETENTION_BUFFER_SEC) * 1000, 7 * 24 * 3600 * 1000].max
    end

    def topic_config_for(category, cfg = KafkaBatch.config)
      case category
      when :scheduled
        {
          "retention.ms"   => scheduled_retention_ms(cfg).to_s,
          "cleanup.policy" => "delete"
        }
      when :dead_letter
        {
          "retention.ms"   => (30 * 24 * 3600 * 1000).to_s,
          "cleanup.policy" => "delete"
        }
      else
        {}
      end
    end

    def resolved_replication_factor(replication_factor)
      (replication_factor || KafkaBatch.config.topics_replication_factor).to_i
    end

    # The full set of topics implied by the current config.
    #
    # @param partitions [Integer, nil] force this count for every topic; when nil
    #   each topic uses its DEFAULT_PARTITIONS entry.
    # @param replication_factor [Integer, nil] when nil uses config.topics_replication_factor
    # @return [Array<Hash>] specs: { name:, partitions:, replication_factor:, category:, config: }
    def specs(partitions: nil, replication_factor: nil)
      cfg   = KafkaBatch.config
      rf    = resolved_replication_factor(replication_factor)
      specs = []
      add   = lambda do |name, category|
        return if name.nil? || name.to_s.empty?

        specs << {
          name:               name,
          partitions:         (partitions || DEFAULT_PARTITIONS.fetch(category)).to_i,
          replication_factor: rf,
          category:           category,
          config:             topic_config_for(category, cfg)
        }
      end

      # KafkaBatch.workers is defined by the full backend (kafka_batch.rb).
      # In UI-only processes (kafka_batch/ui only) the method doesn't exist yet —
      # treat that as no workers loaded so all infrastructure topics are still
      # provisioned via the fallback below.
      workers       = KafkaBatch.respond_to?(:workers) ? KafkaBatch.workers : []
      fair_workers  = workers.select { |w| w.respond_to?(:fairness?) && w.fairness? }
      plain_workers = workers - fair_workers

      # Fair workers share, per lane, an ingest -> dispatcher -> ready path.
      # Provision the ingest/ready pair for each fairness lane actually in use.
      lane_types = fair_workers.map { |w| w.respond_to?(:fairness_type) ? w.fairness_type : :time }.uniq
      lane_types.each do |ft|
        add.call(cfg.fairness_ingest_topic(ft), :ingest)
        add.call(cfg.fairness_ready_topic(ft), :ready)
        add.call(cfg.fairness_ready_topic(ft, :go), :ready)
        add.call(cfg.fairness_ready_topic(ft, :ruby), :ready)
      end

      # Plain workers are produced to their own topic (see Batch#produce_job).
      # When no workers are loaded at all (e.g. a producer-only process), fall
      # back to the shared default queue so the control plane is still complete.
      plain_topics = plain_workers.filter_map do |w|
        w.kafka_topic
      rescue StandardError
        nil
      end.uniq
      plain_topics = [cfg.jobs_topic].compact if plain_topics.empty? && fair_workers.empty?
      plain_topics.each { |t| add.call(t, :jobs) }

      # Priority topics from Sidekiq.yml-style config files.
      registry = KafkaBatch::Priority::Registry.load(
        cfg.resolved_priority_config_paths, cfg: cfg
      )
      registry.all_topics.each { |t| add.call(t, :priority) }

      add.call(cfg.events_topic, :events)
      add.call(cfg.callbacks_topic, :callbacks)
      cfg.retry_topics.each { |t| add.call(t, :retry) }
      # Durable payload store for delayed jobs (perform_in / perform_at).
      # NOTE: set this topic's retention.ms >= config.max_schedule_horizon so a
      # scheduled pointer can never reference a log-cleaned offset.
      add.call(cfg.scheduled_topic, :scheduled)
      add.call(cfg.dead_letter_topic, :dead_letter)

      specs.uniq { |s| s[:name] }
    end

    # Create every configured topic that doesn't already exist. Existing topics
    # are left untouched (Kafka can only grow partitions, never shrink, so we
    # never silently mutate them — log and skip instead).
    #
    # @return [Hash] { created: [names], skipped: [names], failed: [{name:, error:}] }
    def create_all!(partitions: nil, replication_factor: nil, logger: KafkaBatch.logger)
      unless defined?(Karafka) && defined?(Karafka::Admin)
        raise KafkaBatch::ConfigurationError,
              "Karafka::Admin is required to create topics (load Karafka first)"
      end

      existing = existing_topic_names
      result   = { created: [], skipped: [], failed: [] }

      specs(partitions: partitions, replication_factor: replication_factor).each do |spec|
        if existing.include?(spec[:name])
          logger&.info("[KafkaBatch::Topics] exists  #{spec[:name]} (skipped)")
          result[:skipped] << spec[:name]
          next
        end

        begin
          Karafka::Admin.create_topic(
            spec[:name], spec[:partitions], spec[:replication_factor], spec[:config] || {}
          )
          logger&.info(
            "[KafkaBatch::Topics] created #{spec[:name]} " \
            "(partitions=#{spec[:partitions]} rf=#{spec[:replication_factor]} " \
            "config=#{spec[:config]})"
          )
          result[:created] << spec[:name]
        rescue StandardError => e
          # A racing creator (or eventual-consistency on the topic list) shows up
          # as "already exists" — treat that as skipped, not failed.
          if e.message.to_s.match?(/exist/i)
            logger&.info("[KafkaBatch::Topics] exists  #{spec[:name]} (skipped, raced)")
            result[:skipped] << spec[:name]
          else
            logger&.error("[KafkaBatch::Topics] FAILED  #{spec[:name]}: #{e.class}: #{e.message}")
            result[:failed] << { name: spec[:name], error: e.message }
          end
        end
      end

      result
    end

    # Names of topics that already exist on the cluster.
    # @return [Array<String>]
    def existing_topic_names
      return [] unless defined?(Karafka) && defined?(Karafka::Admin)

      KafkaBatch.ensure_karafka_configured! if KafkaBatch.respond_to?(:ensure_karafka_configured!)
      topics = Karafka::Admin.cluster_info.topics
      list = topics.is_a?(Hash) ? topics.values : Array(topics)
      list.map { |t| topic_name_of(t) }.reject(&:empty?).uniq
    rescue StandardError => e
      KafkaBatch.logger&.warn("[KafkaBatch::Topics] could not list existing topics: #{e.message}")
      []
    end

    # Partition counts for every topic on the cluster (best-effort).
    # Topics with unknown partition metadata are omitted.
    # @return [Hash{String => Integer}] topic name → partition count
    def broker_partition_counts
      return {} unless defined?(Karafka) && defined?(Karafka::Admin)

      KafkaBatch.ensure_karafka_configured! if KafkaBatch.respond_to?(:ensure_karafka_configured!)
      info = Karafka::Admin.cluster_info
      topics = info.topics
      list =
        if topics.is_a?(Hash)
          topics.values
        else
          Array(topics)
        end

      list.each_with_object({}) do |t, h|
        name = topic_name_of(t)
        next if name.nil? || name.empty?

        count = partition_count_of(t)
        next if count.nil?

        h[name] = count.to_i
      end
    rescue StandardError => e
      KafkaBatch.logger&.warn("[KafkaBatch::Topics] broker_partition_counts failed: #{e.message}")
      {}
    end

    # Merge create_topics defaults (config) with live broker partition counts.
    # Used by the AI knowledge config snapshot so chat has actual cluster sizing.
    # Always includes both fairness lanes (ingest + ready + .go/.ruby) even when
    # no fair workers are loaded in this process (typical for UI-only pods).
    #
    # @return [Hash] {
    #   available:, refreshed_at:, topics: [ { name:, category:, configured_partitions:,
    #     broker_partitions:, replication_factor:, status: } ],
    #   topic_count:, broker_known_count:
    # }
    def inventory(partitions: nil, replication_factor: nil)
      broker = broker_partition_counts
      available = !broker.empty?
      rows = inventory_specs(partitions: partitions, replication_factor: replication_factor).map do |s|
        name = s[:name].to_s
        configured = s[:partitions].to_i
        bp = broker[name]
        status =
          if !available
            "broker_unavailable"
          elsif bp.nil?
            "missing_on_broker"
          elsif bp == configured
            "matches_default"
          else
            "differs_from_default"
          end
        {
          "name" => name,
          "category" => s[:category].to_s,
          "configured_partitions" => configured,
          "broker_partitions" => bp,
          "replication_factor" => s[:replication_factor].to_i,
          "status" => status
        }
      end

      {
        "available" => available,
        "refreshed_at" => Time.now.utc.iso8601,
        "topic_count" => rows.size,
        "broker_known_count" => rows.count { |r| !r["broker_partitions"].nil? },
        "topics" => rows
      }
    end

    # Like specs, but always lists both fairness lanes so AI/UI inventory is
    # complete even when Worker classes are not loaded in this process.
    def inventory_specs(partitions: nil, replication_factor: nil)
      cfg = KafkaBatch.config
      rf  = resolved_replication_factor(replication_factor)
      by_name = specs(partitions: partitions, replication_factor: replication_factor)
                .each_with_object({}) { |s, h| h[s[:name].to_s] = s }

      add = lambda do |name, category|
        n = name.to_s
        return if n.empty?
        return if by_name.key?(n)

        by_name[n] = {
          name:               n,
          partitions:         (partitions || DEFAULT_PARTITIONS.fetch(category)).to_i,
          replication_factor: rf,
          category:           category,
          config:             topic_config_for(category, cfg)
        }
      end

      KafkaBatch::Configuration::FAIRNESS_TYPES.each do |ft|
        add.call(cfg.fairness_ingest_topic(ft), :ingest)
        add.call(cfg.fairness_ready_topic(ft), :ready)
        add.call(cfg.fairness_ready_topic(ft, :go), :ready)
        add.call(cfg.fairness_ready_topic(ft, :ruby), :ready)
      end
      add.call(cfg.jobs_topic, :jobs)
      Array(cfg.extra_job_topics).each { |t| add.call(t, :jobs) }
      Array(cfg.jobs_topics).each { |t| add.call(t, :jobs) }

      by_name.values.sort_by { |s| s[:name].to_s }
    end

    # @api private
    def topic_name_of(t)
      if t.respond_to?(:topic_name)
        t.topic_name.to_s
      elsif t.is_a?(Hash)
        (t[:topic_name] || t["topic_name"]).to_s
      else
        ""
      end
    end
    module_function :topic_name_of

    # @api private
    def partition_count_of(t)
      if t.respond_to?(:partition_count) && !t.partition_count.nil?
        t.partition_count
      elsif t.respond_to?(:partitions)
        t.partitions.size
      elsif t.is_a?(Hash)
        t[:partition_count] || t["partition_count"] ||
          (t[:partitions] || t["partitions"]).then { |p| p.respond_to?(:size) ? p.size : nil }
      end
    end
    module_function :partition_count_of
  end
end
