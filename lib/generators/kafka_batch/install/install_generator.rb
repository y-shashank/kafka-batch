require "rails/generators"

module KafkaBatch
  module Generators
    class InstallGenerator < Rails::Generators::Base
      # Two source roots: initializer template + the gem's migrations, so we can
      # copy only the migration(s) the chosen stores actually need.
      def self.source_paths
        [
          File.expand_path("templates", __dir__),
          File.expand_path("../../../../db/migrate", __dir__)
        ]
      end
      source_root File.expand_path("templates", __dir__)
      namespace "kafka_batch:install"

      desc "Creates a KafkaBatch initializer, copies the topic-creation shell " \
           "script, and copies the migrations required by the chosen --store / " \
           "--schedule-store (mysql)."

      class_option :store, type: :string, default: "redis",
                           desc: "Batch-ledger store: redis (default) or mysql (failures/pauses in MySQL)"

      class_option :schedule_store, type: :string, default: "redis",
                                    desc: "Delayed-job (perform_in/at) index store: redis (default) or mysql. " \
                                          "Independent of --store."

      class_option :audit, type: :boolean, default: false,
                           desc: "Also copy the kafka_batch_audit_logs migration (Web UI action audit log). " \
                                 "Requires ActiveRecord; enable at runtime with config.audit_enabled = true."

      # Migration filenames per store (kept verbatim so they stay idempotent/skippable).
      LEDGER_MIGRATION    = "20240101000001_create_kafka_batch_tables.rb".freeze
      SCHEDULED_MIGRATION = "20240101000002_create_kafka_batch_scheduled_jobs.rb".freeze
      AUDIT_MIGRATION     = "20240101000004_create_kafka_batch_audit_logs.rb".freeze

      def validate_store_options
        %i[store schedule_store].each do |opt|
          val = options[opt]
          unless %w[mysql redis].include?(val)
            raise ArgumentError, "--#{opt.to_s.tr('_', '-')} must be 'mysql' or 'redis', got '#{val}'"
          end
        end
        @store          = options[:store]
        @schedule_store = options[:schedule_store]
        @audit          = options[:audit]
      end

      def create_initializer
        template "initializer.rb", "config/initializers/kafka_batch.rb"
      end

      def copy_shell_script
        copy_file "create_kafka_topics.sh", "bin/create_kafka_topics.sh"
        # Make it executable right away so the developer can run it immediately.
        in_root { chmod "bin/create_kafka_topics.sh", 0o755 rescue nil }
      end

      def copy_priority_examples
        copy_file "priority/jobs-fast.yml", "config/kafka_batch/priority/jobs-fast.yml"
        copy_file "priority/jobs-slow.yml", "config/kafka_batch/priority/jobs-slow.yml"
      end

      def copy_migrations
        # Copy only what each chosen store / feature needs:
        #   --store mysql          → failures / pauses tables
        #   --schedule-store mysql → kafka_batch_scheduled_jobs table
        #   --audit                → kafka_batch_audit_logs table (Web UI audit log)
        copy_file LEDGER_MIGRATION,    "db/migrate/#{LEDGER_MIGRATION}"    if @store == "mysql"
        copy_file SCHEDULED_MIGRATION, "db/migrate/#{SCHEDULED_MIGRATION}" if @schedule_store == "mysql"
        copy_file AUDIT_MIGRATION,     "db/migrate/#{AUDIT_MIGRATION}"     if @audit
      end

      def show_next_steps
        say "\n"
        say "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", :green
        say "  KafkaBatch installed  (store: #{@store}, schedule_store: #{@schedule_store})", :green
        say "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n", :green

        say "\n1. Add KafkaBatch routes to your karafka.rb:\n"
        say <<~ROUTES
          class KarafkaApp < Karafka::App
            routes.draw do
              KafkaBatch.draw_routes(self)
              # ... your other routes
            end
          end
        ROUTES

        if @store == "mysql" || @schedule_store == "mysql" || @audit
          copied = []
          copied << "failures / pauses"     if @store == "mysql"
          copied << "scheduled-jobs index"  if @schedule_store == "mysql"
          copied << "audit log"             if @audit
          say "\n2. Run the migrations (#{copied.join(' + ')}):\n"
          say "     rails db:migrate\n"
          if @audit
            say "   Then enable the Web UI audit log in the initializer:\n"
            say "     config.audit_enabled = true\n"
          end
          say "\n3. Create Kafka topics (choose one):\n"
        else
          say "\n2. Create Kafka topics (choose one):\n"
        end

        say "   # Rake task (requires a running Kafka broker + Karafka loaded):"
        say "     bundle exec rake kafka_batch:create_topics\n"
        say ""
        say "   # Shell script (works without Rails — for CI, Docker init, etc.):"
        say "     KAFKA_BROKERS=localhost:9092 ./bin/create_kafka_topics.sh\n"
        say ""
        say "   # With a topic prefix (must match KAFKA_PREFIX in your env):"
        say "     KAFKA_BROKERS=kafka:9092 KAFKA_PREFIX=myapp ./bin/create_kafka_topics.sh\n"
        say ""
        say "   # Override partition count (defaults target ~150 pods × concurrency 10):"
        say "     PARTITIONS=1500 REPLICATION_FACTOR=3 ./bin/create_kafka_topics.sh\n"
        say "   # Per-topic defaults live in bin/create_kafka_topics.sh — edit before first deploy.\n"

        say "\n4. Mount the web dashboard in config/routes.rb:\n"
        say <<~ROUTES
          # Protect this behind authentication (e.g. authenticate :admin).
          mount KafkaBatch::Web => "/kafka_batch"
        ROUTES

        say "\n5. Review config/initializers/kafka_batch.rb and tune:\n"
        say "   - retry_tiers / max_retries\n"
        say "   - per-worker fairness: `fairness_type :time | :throughput`\n"
        say "   - schedule_store (:redis / :mysql) + max_schedule_horizon (perform_in/at)\n"
        say "   - max_message_bytes  (1 MiB default; match your broker limit)\n"
        say "   - reconciliation_interval / max_reconcile_per_run\n"
        say "   - liveness_backend  (:redis / :off)\n"
        say "   - redis_url / redis_pool_size / batch_ttl / all_index_max_size  (required)\n"
        say "\n"
      end
    end
  end
end
