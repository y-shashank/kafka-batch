class AddCallbackTrackingToKafkaBatchRecords < ActiveRecord::Migration[6.0]
  def change
    # Set atomically by CallbackConsumer (UPDATE WHERE callback_dispatched_at IS NULL)
    # before invoking on_success / on_complete.  Acts as an at-most-once claim guard:
    # whichever consumer process wins the UPDATE is the only one that fires the callback.
    #
    # Also used by the reconciler: batches in success/complete with a null value here
    # and finished_at older than the reconciliation threshold have a lost callback and
    # need to be re-triggered.
    add_column :kafka_batch_records, :callback_dispatched_at, :datetime, null: true, default: nil

    add_index :kafka_batch_records,
              %i[status callback_dispatched_at finished_at],
              name: "idx_kafka_batch_records_callback_reconcile"
  end
end
