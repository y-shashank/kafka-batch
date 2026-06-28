require "rails/railtie"

module KafkaBatch
  class Railtie < Rails::Railtie
    railtie_name :kafka_batch

    # Make KafkaBatch.config.logger default to Rails.logger
    initializer "kafka_batch.logger" do
      KafkaBatch.config.logger = Rails.logger if KafkaBatch.config.logger.is_a?(Logger)
    end

    # Validate configuration once the app is fully loaded
    initializer "kafka_batch.validate_config", after: :load_config_initializers do
      KafkaBatch.config.validate!
    rescue KafkaBatch::ConfigurationError => e
      raise e
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
        # When the consumer process starts, warn if the fairness ingest topic has
        # too few partitions (best-effort; strict raising is handled by
        # validate_topics! at boot when config.validate_topics_on_boot = true).
        Karafka::App.monitor.subscribe("app.running") do
          begin
            KafkaBatch.validate_fairness_partitions!(strict: false)
          rescue => e
            KafkaBatch.logger.warn("[KafkaBatch] fairness partition check skipped: #{e.message}")
          end
        end

        Karafka::App.monitor.subscribe("app.stopped") do
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

        desc "Generate KafkaBatch migrations (MySQL store only)"
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
            puts "  #{w.name} → topic: #{w.kafka_topic}  retries: #{w.max_retries}"
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
