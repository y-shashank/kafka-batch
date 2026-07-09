require "rails/railtie"

module KafkaBatch
  class Railtie < Rails::Railtie
    railtie_name :kafka_batch

    # Make KafkaBatch.config.logger default to Rails.logger
    initializer "kafka_batch.logger" do
      KafkaBatch.config.logger = Rails.logger if KafkaBatch.config.logger.is_a?(Logger)
    end

    initializer "kafka_batch.handler_manifest", after: :load_config_initializers do
      KafkaBatch.load_handler_manifest! if defined?(KafkaBatch.load_handler_manifest!)
    end

    # Validate configuration once the app is fully loaded
    initializer "kafka_batch.validate_config", after: :load_config_initializers do
      KafkaBatch.config.validate!
    rescue KafkaBatch::ConfigurationError => e
      raise e
    end

    initializer "kafka_batch.metrics", after: "kafka_batch.validate_config" do
      KafkaBatch::Metrics.install! if KafkaBatch.config.metrics_enabled
    end

    # Validate that all required Kafka topics exist at boot time.
    # Opt-in via: config.validate_topics_on_boot = true in the initializer.
    # Skipped when WaterDrop is not yet configured or the broker is unreachable.
    initializer "kafka_batch.validate_topics", after: "kafka_batch.validate_config" do
      if KafkaBatch.config.validate_topics_on_boot
        begin
          KafkaBatch.validate_topics!
        rescue KafkaBatch::ConfigurationError => e
          raise e
        rescue => e
          KafkaBatch.logger.warn(
            "[KafkaBatch] Topic validation failed (non-fatal): #{e.message}"
          )
        end
      end
    end

    # Gracefully close the WaterDrop producer when Karafka stops.
    # Uses the Karafka monitor event "app.stopped" so the producer is flushed
    # cleanly inside the Karafka lifecycle rather than relying on at_exit.
    config.after_initialize do
      if defined?(Karafka::App)
        # When the consumer process starts, check fairness ingest partition count.
        # Respects validate_topics_on_boot: warn by default, raise when strict.
        Karafka::App.monitor.subscribe("app.running") do
          begin
            KafkaBatch.validate_fairness_partitions!
          rescue KafkaBatch::ConfigurationError => e
            raise e
          rescue => e
            KafkaBatch.logger.warn("[KafkaBatch] fairness partition check skipped: #{e.message}")
          end

          # Start the delayed-job poller when schedule_poller_enabled is true
          # (gated on app.running so producer-only web/puma processes never poll).
          begin
            KafkaBatch::SchedulePoller.ensure_running! if defined?(KafkaBatch::SchedulePoller)
          rescue => e
            KafkaBatch.logger.warn("[KafkaBatch] schedule poller start skipped: #{e.message}")
          end
        end

        Karafka::App.monitor.subscribe("app.stopped") do
          # Stop the background threads (if this process ran them) before closing
          # the producer, so no in-flight work is cut off mid-produce.
          KafkaBatch::Fairness::Forwarder.stop! if defined?(KafkaBatch::Fairness::Forwarder)
          KafkaBatch::SchedulePoller.stop!       if defined?(KafkaBatch::SchedulePoller)
          KafkaBatch::Producer.reset!
        end
      else
        # Fallback for non-Karafka environments (e.g. Sidekiq, plain Puma).
        at_exit { KafkaBatch::Producer.reset! }
      end
    end

    # ── Rake tasks ───────────────────────────────────────────────────────────
    rake_tasks do
      namespace :kafka_batch do
        desc "Run the stuck-batch reconciler once"
        task reconcile: :environment do
          KafkaBatch::Reconciler.run
        end

        desc "Create all KafkaBatch Kafka topics (idempotent). " \
             "Env: PARTITIONS=N forces every topic to N partitions; " \
             "REPLICATION_FACTOR=N (default config.topics_replication_factor, currently 3); " \
             "INCLUDE_FAIRNESS=false skips ingest/ready topics."
        task create_topics: :environment do
          # Topics is part of the full backend — not loaded when only
          # kafka_batch/ui is required (dashboard-only processes). Require it
          # lazily here so the task works from any process type.
          require "kafka_batch/topics"

          # Worker classes register their job topics on load. In dev/test Zeitwerk
          # loads them lazily, so eager-load first to discover every job topic.
          # Best-effort: never fail provisioning over this.
          begin
            Rails.application.eager_load! if defined?(Rails) && Rails.respond_to?(:application) && Rails.application
          rescue StandardError => e
            warn "[KafkaBatch] eager_load skipped: #{e.message}"
          end

          partitions = ENV["PARTITIONS"] && !ENV["PARTITIONS"].empty? ? ENV["PARTITIONS"].to_i : nil
          rf         = (ENV["REPLICATION_FACTOR"] || KafkaBatch.config.topics_replication_factor).to_i
          cfg        = KafkaBatch.config

          puts ""
          puts "[KafkaBatch] Creating topics"
          puts "  store              : #{cfg.store}"
          puts "  brokers            : #{Array(cfg.brokers).join(', ')}"
          puts "  partitions         : #{partitions || 'per-topic defaults'}"
          puts "  replication_factor : #{rf}"
          puts ""

          result = KafkaBatch::Topics.create_all!(partitions: partitions, replication_factor: rf)

          result[:created].each { |n| puts "  \e[32m[created]\e[0m  #{n}" }
          result[:skipped].each { |n| puts "  \e[33m[skip]\e[0m     #{n}" }
          result[:failed].each  { |f| puts "  \e[31m[FAILED]\e[0m   #{f[:name]}: #{f[:error]}" }

          puts ""
          puts "  Summary: #{result[:created].size} created, " \
               "#{result[:skipped].size} skipped, #{result[:failed].size} failed."
          puts ""

          unless result[:failed].empty?
            abort "[KafkaBatch] topic creation had failures — see above"
          end
        end

        desc "Print all topics that would be created by rake kafka_batch:create_topics (dry-run)"
        task topics: :environment do
          require "kafka_batch/topics"

          begin
            Rails.application.eager_load! if defined?(Rails) && Rails.respond_to?(:application) && Rails.application
          rescue StandardError => e
            warn "[KafkaBatch] eager_load skipped: #{e.message}"
          end

          puts ""
          puts "[KafkaBatch] Topic plan (dry-run — nothing created):"
          puts ""
          KafkaBatch::Topics.specs.each do |s|
            puts "  %-50s  partitions=%-3d  rf=%-2d" % [s[:name], s[:partitions], s[:replication_factor]]
          end
          puts ""
        end

        desc "Generate KafkaBatch migrations (store :mysql — failures, pauses)"
        task :install_migrations do
          source = File.expand_path("../../db/migrate", __dir__)
          dest   = Rails.root.join("db", "migrate")
          Dir["#{source}/*.rb"].sort.each do |file|
            base    = File.basename(file)
            target  = dest.join(base)
            if File.exist?(target)
              puts "  [skip] #{base} already exists"
            else
              FileUtils.cp(file, target)
              puts "  [copy] #{base}"
            end
          end
        end

        desc "Print all registered KafkaBatch workers"
        task workers: :environment do
          KafkaBatch.workers.each do |w|
            lane = w.fairness? ? "  fairness: #{w.fairness_type}" : ""
            puts "  #{w.name} → topic: #{w.kafka_topic}  retries: #{w.max_retries}#{lane}"
          end
        end

        desc "List pending delayed jobs (perform_in / perform_at). LIMIT=N (default 50)"
        task scheduled: :environment do
          store = KafkaBatch.schedule_store or abort "[KafkaBatch] schedule_store unavailable"
          limit = (ENV["LIMIT"] || 50).to_i
          rows  = store.list(limit: limit)
          puts ""
          puts "[KafkaBatch] #{store.size} scheduled job(s) pending (showing #{rows.size}):"
          rows.each do |r|
            puts "  %-36s  run_at=%-25s  loc=%s/%s:%s  batch=%s" %
                 [r[:job_id], r[:run_at], KafkaBatch.config.scheduled_topic, r[:partition], r[:offset], r[:batch_id]]
          end
          puts ""
        end

        desc "Cancel a pending delayed job by JOB_ID (native only when schedule_store=:mysql)"
        task cancel_scheduled: :environment do
          job_id = ENV["JOB_ID"] or abort "[KafkaBatch] set JOB_ID=<uuid>"
          store  = KafkaBatch.schedule_store or abort "[KafkaBatch] schedule_store unavailable"
          if store.cancel(job_id)
            puts "[KafkaBatch] cancelled scheduled job #{job_id}"
          else
            puts "[KafkaBatch] #{job_id} was not pending (already dispatched, unknown, or " \
                 "schedule_store=:redis — cancel the batch instead)."
          end
        end
      end
    end

    # ── Generators ───────────────────────────────────────────────────────────
    generators do
      require_relative "../../lib/generators/kafka_batch/install/install_generator"
    end
  end
end
