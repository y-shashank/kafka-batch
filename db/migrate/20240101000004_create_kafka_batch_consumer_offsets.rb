class CreateKafkaBatchConsumerOffsets < ActiveRecord::Migration[6.0]
  # Only used when config.counting_mode = :offset_inbox.
  #
  # Holds one row per (source_topic, source_partition) – i.e. O(number of
  # worker-topic partitions), NOT O(number of jobs). The EventConsumer advances
  # last_offset monotonically as it applies completion events, deduping both
  # redelivered and re-produced events by the job message's immutable source
  # coordinates. This is the table that lets large batches avoid per-job rows.
  def change
    create_table :kafka_batch_consumer_offsets do |t|
      t.string  :source_topic,     limit: 255, null: false
      t.integer :source_partition, null: false
      t.bigint  :last_offset,      null: false, default: 0

      t.datetime :updated_at
    end

    add_index :kafka_batch_consumer_offsets,
              %i[source_topic source_partition],
              unique: true,
              name:   "uq_kafka_batch_consumer_offsets"
  end
end
