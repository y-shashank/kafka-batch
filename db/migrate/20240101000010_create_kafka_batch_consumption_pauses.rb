class CreateKafkaBatchConsumptionPauses < ActiveRecord::Migration[6.0]
  # Pause/resume state for the /lag dashboard when config.store = :mysql
  # (and Redis is unavailable). partition_id = -1 means the whole topic is
  # paused; otherwise the row pauses a single partition.
  def change
    create_table :kafka_batch_consumption_pauses do |t|
      t.string   :consumer_group, null: false, limit: 255
      t.string   :topic_name,     null: false, limit: 255
      t.integer  :partition_id,   null: false
      t.datetime :created_at,     null: false
    end

    add_index :kafka_batch_consumption_pauses,
              %i[consumer_group topic_name partition_id],
              unique: true,
              name: "uq_kafka_batch_consumption_pauses"
  end
end
