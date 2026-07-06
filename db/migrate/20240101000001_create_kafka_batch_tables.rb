class CreateKafkaBatchTables < ActiveRecord::Migration[6.0]
  # Single v1 schema migration — creates every table the gem needs in one pass.
  # Run this instead of individual incremental migrations.
  #
  # Tables:
  #   kafka_batch_failures             – per-job failure tracking for the dashboard
  #   kafka_batch_consumption_pauses   – pause/resume state (store: :mysql fallback)
  #
  # Tenant fairness weights live in Redis (per-lane WEIGHT hash), not MySQL.
  #
  # Batch ledger (counters, dedup, reconciler indexes) is always in Redis.
  def change

    # Batch ledger (counters, completion dedup, reconciler indexes) lives in Redis
    # for all store modes. MySQL tables below are optional relational data only.


    # ── kafka_batch_failures ─────────────────────────────────────────────────
    # One row per failing job (upserted on each failure event). Surfaces
    # problems in the dashboard immediately (status "retrying") rather than
    # only after the retry budget is exhausted ("failed").
    create_table :kafka_batch_failures do |t|
      t.string   :batch_id,      limit: 36,  null: false
      t.string   :job_id,        limit: 36,  null: false
      t.string   :worker_class,  limit: 255
      t.string   :error_class,   limit: 255
      t.text     :error_message
      t.integer  :attempt,                   null: false, default: 0   # 0-based
      t.string   :status,        limit: 20,  null: false, default: "failed"  # "retrying"|"failed"
      t.datetime :next_retry_at                                         # nil once exhausted
      t.datetime :failed_at,                 null: false
    end

    # Idempotent upsert: redelivered exhaustion for the same job is a no-op
    add_index :kafka_batch_failures, %i[batch_id job_id],
              unique: true, name: "uq_kb_failures"

    # list_failures: WHERE batch_id = ? ORDER BY failed_at DESC
    # Composite covers both the equality filter and the sort in one index.
    add_index :kafka_batch_failures, %i[batch_id failed_at],
              name: "idx_kb_failures_batch_failed_at"

    # list_all_failures: ORDER BY failed_at DESC (cross-batch recency view)
    add_index :kafka_batch_failures, :failed_at,
              name: "idx_kb_failures_failed_at"


    # ── kafka_batch_consumption_pauses ───────────────────────────────────────
    # Pause/resume state for the /lag dashboard when store: :mysql and Redis
    # is unavailable. partition_id = -1 pauses the whole topic; any other
    # value pauses a single partition.
    create_table :kafka_batch_consumption_pauses do |t|
      t.string   :consumer_group, limit: 255, null: false
      t.string   :topic_name,     limit: 255, null: false
      t.integer  :partition_id,              null: false
      t.datetime :created_at,               null: false
    end

    add_index :kafka_batch_consumption_pauses,
              %i[consumer_group topic_name partition_id],
              unique: true, name: "uq_kb_consumption_pauses"
  end
end
