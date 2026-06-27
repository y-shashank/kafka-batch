class AddLockedAtToKafkaBatchRecords < ActiveRecord::Migration[6.0]
  # Supports open/streaming batches: a batch accepts new jobs (which increment
  # total_jobs) until it is locked. Completion callbacks only fire once
  # locked_at is set, so a batch can be built incrementally across processes
  # without completing prematurely.
  def change
    add_column :kafka_batch_records, :locked_at, :datetime, null: true, default: nil
  end
end
