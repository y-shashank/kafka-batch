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

    initializer "kafka_batch.performance_metrics", after: "kafka_batch.validate_config" do
      KafkaBatch::PerformanceMetrics.install! if KafkaBatch.config.performance_metrics_enabled
    end

    # Load prebuilt AI knowledge chunks into Redis; refresh config snapshot
    # at most every 24h. Safe for many UI pods: one writer, version-gated corpus.
    initializer "kafka_batch.ai_knowledge", after: "kafka_batch.validate_config" do
      config.after_initialize do
        begin
          KafkaBatch::Ai::KnowledgeIndex.sync! if defined?(KafkaBatch::Ai::KnowledgeIndex)
        rescue => e
          KafkaBatch.logger.warn("[KafkaBatch] AI knowledge sync skipped: #{e.message}")
        end
      end
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

          # Start the recurring (cron) scheduler when enabled. Loaded lazily so
          # UI/producer-only processes never pull ActiveRecord for it. Shares the
          # leader lock + fire ledger with the Go daemon, so both can run safely.
          begin
            if KafkaBatch.config.recurring_scheduler_enabled
              require_relative "recurring/ticker"
              KafkaBatch::Recurring::Ticker.ensure_running!
            end
          rescue => e
            KafkaBatch.logger.warn("[KafkaBatch] recurring scheduler start skipped: #{e.message}")
          end

          # Dedicated heartbeat loop (default every 20s, TTL 180s) so CPU-heavy
          # jobs that starve the consume path cannot look dead to reclaim/health.
          begin
            KafkaBatch::Liveness.start_heartbeat_loop!
          rescue => e
            KafkaBatch.logger.warn("[KafkaBatch] liveness heartbeat loop start skipped: #{e.message}")
          end

          # SuperFetch orphan reclaim (parity with Go kbatch daemon). NX-locked
          # so multiple control/execution replicas can safely share the loop.
          # Watermark mode owns durability via Kafka offset commits and writes
          # nothing to the working set, so reclaim has nothing to do — skip it to
          # keep the "one execution mode per topic" contract unambiguous.
          if KafkaBatch.config.watermark_mode?
            KafkaBatch.logger.info(
              "[KafkaBatch] EXECUTION MODE = watermark (Redis-free): workset reclaim DISABLED " \
              "(durability is via Kafka offset watermarks). REQUIRED: idempotent handlers, " \
              "similar per-topic runtimes, and one mode per topic (never mix with SuperFetch)."
            )
          else
            begin
              KafkaBatch::Workset::ReclaimScheduler.ensure_running! if defined?(KafkaBatch::Workset::ReclaimScheduler)
            rescue => e
              KafkaBatch.logger.warn("[KafkaBatch] workset reclaim start skipped: #{e.message}")
            end
          end
        end

        Karafka::App.monitor.subscribe("app.stopped") do
          # Drain the active execution pool (SuperFetch or Watermark) before
          # tearing down producer/heartbeats so in-flight #perform can finish
          # (SuperFetch: Complete or stay in workset for reclaim; Watermark:
          # commit what it can, the rest re-runs on restart).
          remaining = 0
          begin
            remaining = KafkaBatch.job_executor_drain.to_i
          rescue => e
            KafkaBatch.logger.warn("[KafkaBatch] execution drain: #{e.message}")
          end

          # Stop the background threads (if this process ran them) before closing
          # the producer, so no in-flight work is cut off mid-produce.
          KafkaBatch::Workset::ReclaimScheduler.stop! if defined?(KafkaBatch::Workset::ReclaimScheduler)
          KafkaBatch::Liveness.stop_heartbeat_loop! if defined?(KafkaBatch::Liveness)

          # If drain timed out with leftovers, drop the live key so reclaim does
          # not wait for liveness_ttl (~180s).
          if remaining.positive? && defined?(KafkaBatch::Workset) && defined?(KafkaBatch::Liveness)
            begin
              KafkaBatch::Workset.store.delete_consumer(KafkaBatch::Liveness.consumer_id)
            rescue => e
              KafkaBatch.logger.warn("[KafkaBatch] delete live consumer: #{e.message}")
            end
          end

          KafkaBatch::Fairness::Forwarder.stop! if defined?(KafkaBatch::Fairness::Forwarder)
          KafkaBatch::SchedulePoller.stop!       if defined?(KafkaBatch::SchedulePoller)
          KafkaBatch::Recurring::Ticker.stop!    if defined?(KafkaBatch::Recurring::Ticker)
          KafkaBatch::Producer.reset!            if defined?(KafkaBatch::Producer)
        end
      else
        # Fallback for non-Karafka environments (e.g. Sidekiq, plain Puma).
        # Guard on the constant: UI/producer-only load paths (require
        # "kafka_batch/ui") pull in the railtie but never require producer.rb,
        # so Producer is undefined at exit — e.g. during `rails g
        # kafka_batch:install`, which boots the app but not the backend.
        at_exit { KafkaBatch::Producer.reset! if defined?(KafkaBatch::Producer) }
      end
    end

    # ── Rake tasks ───────────────────────────────────────────────────────────
    rake_tasks do
      namespace :kafka_batch do
        desc "Run the stuck-batch reconciler once"
        task reconcile: :environment do
          KafkaBatch::Reconciler.run
        end

        namespace :recurring do
          desc "Run the recurring (cron) scheduler loop in the foreground " \
               "(dedicated pod alternative to the Karafka-embedded ticker)."
          task run: :environment do
            require "kafka_batch/recurring/ticker"
            ticker = KafkaBatch::Recurring::Ticker.new
            trap("INT")  { exit }
            trap("TERM") { exit }
            KafkaBatch.logger.info("[KafkaBatch][Recurring] rake kafka_batch:recurring:run")
            loop do
              ticker.tick
              sleep(KafkaBatch.config.recurring_window.to_f.clamp(1.0, 3600.0))
            end
          end

          desc "Run a single recurring scheduler tick (one leader-gated pass) and exit."
          task tick: :environment do
            require "kafka_batch/recurring/ticker"
            result = KafkaBatch::Recurring::Ticker.new.tick
            puts "[KafkaBatch] recurring tick → #{result}"
          end
        end

        desc "Create all KafkaBatch Kafka topics (idempotent). " \
             "Env: PARTITIONS=N forces every topic to N partitions " \
             "(default: per-category KafkaBatch::Topics::DEFAULT_PARTITIONS); " \
             "REPLICATION_FACTOR=N (default config.topics_replication_factor, currently 3)."
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

          # Honor the same env knobs as create_topics so the preview reflects
          # exactly what would be created.
          partitions = ENV["PARTITIONS"] && !ENV["PARTITIONS"].empty? ? ENV["PARTITIONS"].to_i : nil
          rf         = (ENV["REPLICATION_FACTOR"] || KafkaBatch.config.topics_replication_factor).to_i

          puts ""
          puts "[KafkaBatch] Topic plan (dry-run — nothing created):"
          puts ""
          KafkaBatch::Topics.specs(partitions: partitions, replication_factor: rf).each do |s|
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

        desc "Rebuild packaged AI knowledge chunks from ai/README.md + ai/FAQ.md " \
             "(run in the gem repo before release). Writes lib/kafka_batch/ai/knowledge_chunks.json"
        task :build_ai_chunks do
          require "kafka_batch/ai/chunker"
          gem_root = File.expand_path("../../..", __dir__)
          ai_dir = File.join(gem_root, "ai")
          readme = File.join(ai_dir, "README.md")
          faq    = File.join(ai_dir, "FAQ.md")
          abort "[KafkaBatch] missing #{readme}" unless File.file?(readme)
          abort "[KafkaBatch] missing #{faq}" unless File.file?(faq)

          out = File.join(gem_root, "lib/kafka_batch/ai/knowledge_chunks.json")
          payload = KafkaBatch::Ai::Chunker.write!(
            output_path: out,
            readme_path: readme,
            faq_path: faq
          )
          puts "[KafkaBatch] wrote #{out}"
          puts "  chunks=#{payload['chunk_count']} corpus=#{payload['corpus_version']}"
        end

        desc "Force-sync AI knowledge into Redis. FORCE=1 clears meta (full corpus rewrite). " \
             "Without FORCE, sync follows normal rules: corpus on version change, config every 24h."
        task sync_ai_knowledge: :environment do
          if ENV["FORCE"].to_s == "1" && KafkaBatch.config.redis_configured?
            begin
              client = KafkaBatch::RedisClient.new(KafkaBatch.config)
              client.del(KafkaBatch::Ai::KnowledgeIndex::META_KEY) if client
            rescue StandardError => e
              warn "[KafkaBatch] FORCE meta clear failed: #{e.message}"
            end
          end
          result = KafkaBatch::Ai::KnowledgeIndex.sync!
          puts "[KafkaBatch] AI knowledge sync => #{result}"
          meta = KafkaBatch::Ai::KnowledgeIndex.meta
          puts "  meta=#{meta.inspect}" unless meta.empty?
        end
      end
    end

    # ── Generators ───────────────────────────────────────────────────────────
    generators do
      require_relative "../../lib/generators/kafka_batch/install/install_generator"
    end
  end
end
