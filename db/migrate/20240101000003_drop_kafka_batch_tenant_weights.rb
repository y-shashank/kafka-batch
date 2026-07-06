class DropKafkaBatchTenantWeights < ActiveRecord::Migration[6.0]
  # Tenant weights are Redis-only (per-lane WEIGHT hash). This table is unused.
  def up
    drop_table :kafka_batch_tenant_weights, if_exists: true
  end

  def down
    create_table :kafka_batch_tenant_weights do |t|
      t.string  :tenant_id,     limit: 255, null: false
      t.string  :fairness_type, limit: 16,  null: false, default: "time"
      t.decimal :weight, precision: 10, scale: 4, null: false, default: "1.0"
      t.datetime :updated_at, null: false
    end

    add_index :kafka_batch_tenant_weights, %i[tenant_id fairness_type],
              unique: true, name: "uq_kb_tenant_weights_tenant_type"
  end
end
