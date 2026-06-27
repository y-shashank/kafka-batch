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

  describe "open batches (add_jobs / lock_batch)" do
    it "add_jobs grows total_jobs while the batch is open" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 0, locked: false)
      expect(store.add_jobs(id, 5)).to eq(:ok)
      expect(store.find_batch(id)[:total_jobs]).to eq(5)
      expect(store.find_batch(id)[:locked_at]).to be_nil
    end

    it "does not finalize an open batch even when complete" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, locked: false)
      r = store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success")
      expect(r[:status]).to eq(:continue)
      expect(store.find_batch(id)[:status]).to eq("running")
    end

    it "lock_batch finalizes an already-complete batch and blocks further add_jobs" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, locked: false)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success")

      res = store.lock_batch(id)
      expect(res[:status]).to eq(:done)
      expect(res[:outcome]).to eq("success")
      expect(store.add_jobs(id, 1)).to eq(:locked)
    end

    it "lock_batch on an incomplete open batch just locks it" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 3, locked: false)
      expect(store.lock_batch(id)[:status]).to eq(:locked)
      expect(store.find_batch(id)[:locked_at]).not_to be_nil
    end

    it "add_jobs reports :not_found and :cancelled" do
      expect(store.add_jobs("nope", 1)).to eq(:not_found)
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 0, locked: false)
      store.update_batch_status(id, "cancelled")
      expect(store.add_jobs(id, 1)).to eq(:cancelled)
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

  describe "admin UI queries" do
    it "#batch_status returns the status (or nil when unknown)" do
      id = new_batch
      expect(store.batch_status(id)).to eq("running")
      expect(store.batch_status("nope")).to be_nil
    end

    it "#list_batches returns batches newest-first with optional status filter" do
      a = new_batch
      b = new_batch
      store.update_batch_status(b, "cancelled")

      all = store.list_batches
      expect(all.map { |x| x[:id] }).to include(a, b)

      cancelled = store.list_batches(status: "cancelled")
      expect(cancelled.map { |x| x[:id] }).to eq([b])
    end

    it "#list_batches paginates" do
      ids = Array.new(3) { new_batch }
      page1 = store.list_batches(limit: 2, offset: 0)
      page2 = store.list_batches(limit: 2, offset: 2)
      expect(page1.size).to eq(2)
      expect(page2.size).to eq(1)
      expect((page1 + page2).map { |x| x[:id] }).to match_array(ids)
    end

    it "#batch_counts groups by status" do
      new_batch
      c = new_batch
      store.update_batch_status(c, "cancelled")

      counts = store.batch_counts
      expect(counts["running"]).to eq(1)
      expect(counts["cancelled"]).to eq(1)
    end

    it "#cancelled_batch_ids returns only cancelled batch ids" do
      new_batch
      c = new_batch
      store.update_batch_status(c, "cancelled")
      expect(store.cancelled_batch_ids).to eq([c])
    end
  end
end
