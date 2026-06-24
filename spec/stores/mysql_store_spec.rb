RSpec.describe KafkaBatch::Stores::MysqlStore do
  subject(:store) { described_class.new }

  def new_batch(id: SecureRandom.uuid, total: 2, **opts)
    store.create_batch(id: id, total_jobs: total, **opts)
    id
  end

  describe "#create_batch / #find_batch" do
    it "persists and round-trips batch fields including meta" do
      id = new_batch(total: 3, on_success: "S", on_complete: "C", meta: { "k" => "v" })
      batch = store.find_batch(id)

      expect(batch[:total_jobs]).to eq(3)
      expect(batch[:status]).to eq("running")
      expect(batch[:on_success]).to eq("S")
      expect(batch[:meta]).to eq("k" => "v")
    end

    it "is idempotent on duplicate id" do
      id = new_batch
      expect { store.create_batch(id: id, total_jobs: 2) }.not_to raise_error
      expect(store.batch_record_class.count).to eq(1)
    end
  end

  describe "#record_completion_by_offset" do
    it "counts continue -> done by monotonic source offset" do
      id = new_batch(total: 2)
      r1 = store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 10, status: "success")
      expect(r1[:status]).to eq(:continue)

      r2 = store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 11, status: "success")
      expect(r2[:status]).to eq(:done)
      expect(r2[:outcome]).to eq("success")
      expect(store.find_batch(id)[:status]).to eq("success")
    end

    it "marks the batch :done with outcome complete when any job fails" do
      id = new_batch(total: 2)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 10, status: "success")
      result = store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 11, status: "failed")
      expect(result[:status]).to eq(:done)
      expect(result[:outcome]).to eq("complete")
    end

    it "dedups a replayed/re-produced source offset (<= cursor)" do
      id = new_batch(total: 2)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 10, status: "success")

      # same offset again (redelivery) and a lower offset both dedup
      expect(store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 10, status: "success")[:status]).to eq(:duplicate)
      expect(store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 9,  status: "success")[:status]).to eq(:duplicate)

      expect(store.find_batch(id)[:completed_count]).to eq(1)
    end

    it "tracks cursors independently per (topic, partition)" do
      id = new_batch(total: 2)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 5, status: "success")
      # different partition, low offset still counts
      r = store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 1, source_offset: 1, status: "success")
      expect(r[:status]).to eq(:done)
    end

    it "returns :not_found for an unknown batch (but still advances the cursor)" do
      r = store.record_completion_by_offset(batch_id: "nope", source_topic: "wt", source_partition: 0, source_offset: 1, status: "success")
      expect(r[:status]).to eq(:not_found)
    end
  end

  describe "#claim_callback / #callback_dispatched?" do
    it "lets exactly one caller win the claim" do
      id = new_batch
      expect(store.callback_dispatched?(id)).to be(false)
      expect(store.claim_callback(id)).to be(true)
      expect(store.claim_callback(id)).to be(false)
      expect(store.callback_dispatched?(id)).to be(true)
    end
  end

  describe "reconciler queries" do
    it "#stale_batches returns running batches older than the threshold" do
      id = new_batch
      stale = store.stale_batches(older_than: Time.now + 60)
      expect(stale.map { |b| b[:id] }).to include(id)
    end

    it "#done_batches_without_callback finds finished, unclaimed batches" do
      id = new_batch(total: 1)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success")

      lost = store.done_batches_without_callback(older_than: Time.now + 60)
      expect(lost.map { |b| b[:id] }).to include(id)

      store.claim_callback(id)
      after = store.done_batches_without_callback(older_than: Time.now + 60)
      expect(after.map { |b| b[:id] }).not_to include(id)
    end
  end

  describe "#delete_batch" do
    it "removes the batch record" do
      id = new_batch(total: 2)
      store.delete_batch(id)
      expect(store.find_batch(id)).to be_nil
    end
  end
end
