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

    # SQLite-compatible equivalent of the gem's MySQL migrations. The shipped
    # migrations use MySQL-only DDL (ALTER TABLE ... ADD PRIMARY KEY), so for
    # the test database we declare the same logical schema portably.
    def load_schema!
      ActiveRecord::Schema.verbose = false
      ActiveRecord::Schema.define do
        create_table :kafka_batch_records, id: :string, force: true do |t|
          t.integer  :total_jobs,             null: false
          t.integer  :completed_count,        null: false, default: 0
          t.integer  :failed_count,           null: false, default: 0
          t.string   :status,                 null: false, default: "running"
          t.string   :on_success
          t.string   :on_complete
          t.string   :description
          t.text     :meta
          t.datetime :created_at,             null: false
          t.datetime :finished_at
          t.datetime :callback_dispatched_at
          t.string   :callback_dispatched_by
          t.datetime :locked_at
        end
        add_index :kafka_batch_records, :status
        add_index :kafka_batch_records, %i[status created_at]
        add_index :kafka_batch_records, %i[status callback_dispatched_at finished_at]

        create_table :kafka_batch_consumer_offsets, force: true do |t|
          t.string   :source_topic,     null: false
          t.integer  :source_partition, null: false
          t.bigint   :last_offset,      null: false, default: 0
          t.datetime :updated_at
        end
        add_index :kafka_batch_consumer_offsets, %i[source_topic source_partition], unique: true

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

        create_table :kafka_batch_consumer_heartbeats, id: false, force: true do |t|
          t.string   :consumer_id,      null: false
          t.string   :hostname
          t.integer  :pid
          t.string   :topic
          t.string   :current_job_id
          t.string   :current_worker
          t.string   :current_batch_id
          t.string   :current_topic
          t.integer  :current_partition
          t.integer  :jobs_done,  null: false, default: 0
          t.datetime :last_seen,  null: false
        end
        add_index :kafka_batch_consumer_heartbeats, :consumer_id, unique: true
        add_index :kafka_batch_consumer_heartbeats, :last_seen

        create_table :kafka_batch_consumption_pauses, force: true do |t|
          t.string   :consumer_group, null: false
          t.string   :topic_name,     null: false
          t.integer  :partition_id,   null: false
          t.datetime :created_at,     null: false
        end
        add_index :kafka_batch_consumption_pauses,
                  %i[consumer_group topic_name partition_id],
                  unique: true
      end
    end

    def truncate!
      establish!
      conn = ActiveRecord::Base.connection
      conn.execute("DELETE FROM kafka_batch_records")
      conn.execute("DELETE FROM kafka_batch_consumer_offsets")
      conn.execute("DELETE FROM kafka_batch_failures")
      conn.execute("DELETE FROM kafka_batch_consumer_heartbeats")
      conn.execute("DELETE FROM kafka_batch_consumption_pauses")
    end
  end
end
