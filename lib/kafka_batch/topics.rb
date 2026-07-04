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
    # single count). These are starting points — size them for your throughput.
    DEFAULT_PARTITIONS = {
      jobs:        6,
      priority:    6,   # fast/slow p0+p1 topics (override per topic if needed)
      events:      3,
      callbacks:   1,
      retry:       3,   # per tier
      scheduled:   6,   # durable payload store for perform_in/perform_at
      dead_letter: 1,
      ingest:      12,  # fairness ingest (per lane): ≈ max concurrent tenants
      ready:       6    # fairness ready (per lane): swarm parallelism
    }.freeze

    # The full set of topics implied by the current config.
    #
    # @param partitions [Integer, nil] force this count for every topic; when nil
    #   each topic uses its DEFAULT_PARTITIONS entry.
    # @param replication_factor [Integer]
    # @return [Array<Hash>] specs: { name:, partitions:, replication_factor: }
    def specs(partitions: nil, replication_factor: 1)
      cfg   = KafkaBatch.config
      specs = []
      add   = lambda do |name, category|
        return if name.nil? || name.to_s.empty?

        specs << {
          name:               name,
          partitions:         (partitions || DEFAULT_PARTITIONS.fetch(category)).to_i,
          replication_factor: replication_factor.to_i
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

      # Priority queue topics: always provisioned so they're available when
      # workers adopt them. uniq at the end deduplicates against any plain
      # worker that already declared the same kafka_topic.
      add.call(cfg.fast_p0_topic, :priority)
      add.call(cfg.fast_p1_topic, :priority)
      add.call(cfg.slow_p0_topic, :priority)
      add.call(cfg.slow_p1_topic, :priority)

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
    def create_all!(partitions: nil, replication_factor: 1, logger: KafkaBatch.logger)
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
          Karafka::Admin.create_topic(spec[:name], spec[:partitions], spec[:replication_factor])
          logger&.info(
            "[KafkaBatch::Topics] created #{spec[:name]} " \
            "(partitions=#{spec[:partitions]} rf=#{spec[:replication_factor]})"
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
      Karafka::Admin.cluster_info.topics.map { |t| t[:topic_name] }
    rescue StandardError => e
      KafkaBatch.logger&.warn("[KafkaBatch::Topics] could not list existing topics: #{e.message}")
      []
    end
  end
end
