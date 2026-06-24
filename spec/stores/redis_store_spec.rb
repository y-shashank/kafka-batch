RSpec.describe KafkaBatch::Stores::RedisStore do
  let(:store) { described_class.new }

  before(:each) do
    skip "Redis unavailable at #{KafkaBatchSpec::RedisHelper::TEST_URL}" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.store     = :redis
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.batch_ttl = 3600
    KafkaBatchSpec::RedisHelper.flush!
  end

  def new_batch(id: SecureRandom.uuid, total: 2, **opts)
    store.create_batch(id: id, total_jobs: total, **opts)
    id
  end

  describe "#create_batch / #find_batch" do
    it "round-trips fields including meta" do
      id = new_batch(total: 3, on_success: "S", on_complete: "C", meta: { "k" => "v" })
      batch = store.find_batch(id)

      expect(batch[:total_jobs]).to eq(3)
      expect(batch[:status]).to eq("running")
      expect(batch[:on_success]).to eq("S")
      expect(batch[:meta]).to eq("k" => "v")
    end

    it "is idempotent on duplicate id" do
      id = new_batch
      expect(store.create_batch(id: id, total_jobs: 2)).to eq(0)
    end
  end

  describe "#record_completion_by_offset" do
    it "counts continue -> done by monotonic source offset" do
      id = new_batch(total: 2)
      expect(store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 10, status: "success")[:status]).to eq(:continue)
      r = store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 11, status: "success")
      expect(r[:status]).to eq(:done)
      expect(r[:outcome]).to eq("success")
    end

    it "reports complete when a job failed" do
      id = new_batch(total: 2)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 10, status: "success")
      r = store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 11, status: "failed")
      expect(r[:outcome]).to eq("complete")
    end

    it "dedups a replayed/re-produced source offset (<= cursor)" do
      id = new_batch(total: 2)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 10, status: "success")
      expect(store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 10, status: "success")[:status]).to eq(:duplicate)
      expect(store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 9, status: "success")[:status]).to eq(:duplicate)
      expect(store.find_batch(id)[:completed_count]).to eq(1)
    end

    it "tracks cursors independently per (topic, partition)" do
      id = new_batch(total: 2)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 5, status: "success")
      r = store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 1, source_offset: 1, status: "success")
      expect(r[:status]).to eq(:done)
    end

    it "moves the batch into the done index on completion" do
      id = new_batch(total: 1)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success")
      expect(store.done_batches_without_callback(older_than: Time.now + 60).map { |b| b[:id] }).to include(id)
    end
  end

  describe "#claim_callback / #callback_dispatched?" do
    it "lets exactly one caller win" do
      id = new_batch
      expect(store.callback_dispatched?(id)).to be(false)
      expect(store.claim_callback(id)).to be(true)
      expect(store.claim_callback(id)).to be(false)
      expect(store.callback_dispatched?(id)).to be(true)
    end

    # Fix #3: a stale callback for an expired/absent batch must not recreate a
    # partial, TTL-less hash (orphan key).
    it "does not claim or recreate a hash for an absent batch" do
      id = SecureRandom.uuid
      expect(store.claim_callback(id)).to be(false)
      expect(store.find_batch(id)).to be_nil

      raw = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL).exists?("kafka_batch:b:#{id}")
      expect(raw).to be(false)
    end
  end

  describe "index-backed reconciler queries (fix #3)" do
    it "#stale_batches returns running batches via the running index" do
      id = new_batch
      expect(store.stale_batches(older_than: Time.now + 60).map { |b| b[:id] }).to include(id)
    end

    it "drops a batch from the running index once it completes" do
      id = new_batch(total: 1)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success")
      expect(store.stale_batches(older_than: Time.now + 60).map { |b| b[:id] }).not_to include(id)
    end

    it "#done_batches_without_callback finds finished, unclaimed batches and prunes after claim" do
      id = new_batch(total: 1)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success")

      lost = store.done_batches_without_callback(older_than: Time.now + 60)
      expect(lost.map { |b| b[:id] }).to include(id)

      store.claim_callback(id)
      after = store.done_batches_without_callback(older_than: Time.now + 60)
      expect(after.map { |b| b[:id] }).not_to include(id)
    end

    it "removes cancelled batches from the running index" do
      id = new_batch
      store.update_batch_status(id, "cancelled")
      expect(store.stale_batches(older_than: Time.now + 60).map { |b| b[:id] }).not_to include(id)
    end

    it "#mark_finished stamps finished_at and moves running -> done" do
      id = new_batch
      store.mark_finished(id, "success")

      batch = store.find_batch(id)
      expect(batch[:status]).to eq("success")
      expect(batch[:finished_at]).not_to be_nil

      expect(store.stale_batches(older_than: Time.now + 60).map { |b| b[:id] }).not_to include(id)
      expect(store.done_batches_without_callback(older_than: Time.now + 60).map { |b| b[:id] }).to include(id)
    end
  end

  describe "#delete_batch" do
    it "removes the hash and both index entries" do
      id = new_batch(total: 1)
      store.delete_batch(id)
      expect(store.find_batch(id)).to be_nil
      expect(store.stale_batches(older_than: Time.now + 60).map { |b| b[:id] }).not_to include(id)
    end
  end

  describe "#with_reconciler_lock" do
    it "yields when the lock is free and is mutually exclusive" do
      ran = false
      store.with_reconciler_lock(ttl: 30) { ran = true }
      expect(ran).to be(true)
    end

    # Fix #4: a raise inside the block is swallowed+logged (like MysqlStore) and
    # the lock is still released so the next sweep can acquire it.
    it "swallows errors raised in the block and releases the lock" do
      expect { store.with_reconciler_lock(ttl: 30) { raise "boom" } }.not_to raise_error

      reacquired = false
      store.with_reconciler_lock(ttl: 30) { reacquired = true }
      expect(reacquired).to be(true)
    end
  end
end
