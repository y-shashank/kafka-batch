require "active_record"

module KafkaBatchSpec
  module ActiveRecordSupport
    module_function

    def establish!
      return if @established

      ActiveRecord::Base.establish_connection(
        adapter:  "sqlite3",
        database: ":memory:"
      )
      load_schema!
      @established = true
    end

    # SQLite-compatible schema for the MySQL store's relational tables.
    # Batch ledger state (counters, dedup, reconciler indexes) lives in Redis.
    def load_schema!
      ActiveRecord::Schema.verbose = false
      ActiveRecord::Schema.define do
        create_table :kafka_batch_failures, force: true do |t|
          t.string   :batch_id,      null: false
          t.string   :job_id,        null: false
          t.string   :worker_class
          t.string   :error_class
          t.text     :error_message
          t.integer  :attempt,       null: false, default: 0
          t.string   :status,        null: false, default: "failed"
          t.datetime :next_retry_at
          t.datetime :failed_at,     null: false
        end
        add_index :kafka_batch_failures, %i[batch_id job_id], unique: true
        add_index :kafka_batch_failures, :batch_id

        create_table :kafka_batch_consumption_pauses, force: true do |t|
          t.string   :consumer_group, null: false
          t.string   :topic_name,     null: false
          t.integer  :partition_id,   null: false
          t.datetime :created_at,     null: false
        end
        add_index :kafka_batch_consumption_pauses,
                  %i[consumer_group topic_name partition_id],
                  unique: true

        create_table :kafka_batch_scheduled_jobs, id: false, force: true do |t|
          t.string   :job_id,       null: false
          t.datetime :run_at,       null: false
          t.integer  :partition_id, null: false
          t.bigint   :kafka_offset, null: false
          t.string   :batch_id
          t.datetime :lease_until
          t.datetime :created_at,   null: false
        end
        add_index :kafka_batch_scheduled_jobs, :job_id, unique: true
        add_index :kafka_batch_scheduled_jobs, %i[run_at lease_until]
        add_index :kafka_batch_scheduled_jobs, :batch_id
      end
    end

    def truncate!
      establish!
      conn = ActiveRecord::Base.connection
      conn.execute("DELETE FROM kafka_batch_failures")
      conn.execute("DELETE FROM kafka_batch_consumption_pauses")
      conn.execute("DELETE FROM kafka_batch_scheduled_jobs")
    end
  end
end
