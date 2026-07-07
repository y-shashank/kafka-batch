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

  def complete(batch_id:, seq:, status: "success", job_id: nil, source_topic: "wt", source_partition: 0, source_offset: nil)
    store.record_completion_by_offset(
      batch_id:         batch_id,
      batch_seq:        seq,
      source_topic:     source_topic,
      source_partition: source_partition,
      job_id:           job_id || "j#{seq}",
      source_offset:    source_offset || seq,
      status:           status
    )
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

  describe "open batches (add_jobs / seal_batch)" do
    it "add_jobs grows total_jobs while held (block-form) and does not finalize" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 0, sealed: false)
      expect(store.add_jobs(id, 4)).to eq({ status: :ok, seq_start: 1, seq_end: 4 })
      expect(store.find_batch(id)[:total_jobs]).to eq(4)
      expect(store.find_batch(id)[:locked_at]).to be_nil
    end

    it "does not finalize a held batch even when complete, then finalizes on seal" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, sealed: false)
      r = complete(batch_id: id, seq: 1, status: "success")
      expect(r[:status]).to eq(:continue)
      expect(store.find_batch(id)[:status]).to eq("running")

      res = store.seal_batch(id)
      expect(res[:status]).to eq(:done)
      expect(res[:outcome]).to eq("success")
      expect(store.add_jobs(id, 1)).to eq(:closed)  # completed → no more jobs
    end

    it "does not clear the last job dedup bit when sealing after an early completion" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 0, sealed: false)
      store.add_jobs(id, 3)
      complete(batch_id: id, seq: 3, status: "success")
      expect(store.find_batch(id)[:completed_count]).to eq(1)

      store.seal_batch(id)

      expect(complete(batch_id: id, seq: 3, status: "success")[:status]).to eq(:duplicate)
      expect(store.find_batch(id)[:completed_count]).to eq(1)
    end

    it "a sealed but still-running batch keeps accepting jobs (jobs adding jobs)" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, sealed: true)  # bare create
      expect(store.find_batch(id)[:locked_at]).not_to be_nil
      # one job still outstanding → batch open → can add more
      expect(store.add_jobs(id, 2)).to eq({ status: :ok, seq_start: 1, seq_end: 2 })
      expect(store.find_batch(id)[:total_jobs]).to eq(3)
    end

    it "add_jobs reports :not_found and :cancelled" do
      expect(store.add_jobs(SecureRandom.uuid, 1)).to eq(:not_found)
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 0, sealed: false)
      store.update_batch_status(id, "cancelled")
      expect(store.add_jobs(id, 1)).to eq(:cancelled)
    end

    it "persists and returns an optional description" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 0, description: "weekly digest")
      expect(store.find_batch(id)[:description]).to eq("weekly digest")
    end

    it "list_batches filters by id or description (case-insensitive)" do
      a = SecureRandom.uuid
      store.create_batch(id: a, total_jobs: 0, description: "Nightly Report")
      b = SecureRandom.uuid
      store.create_batch(id: b, total_jobs: 0, description: "weekly digest")

      expect(store.list_batches(search: "nightly").map { |x| x[:id] }).to include(a)
      expect(store.list_batches(search: "nightly").map { |x| x[:id] }).not_to include(b)
      expect(store.list_batches(search: a[0, 8]).map { |x| x[:id] }).to include(a)
    end
  end

  describe "#claim_callback dispatched_by" do
    it "records which consumer dispatched the callback" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 0)
      store.claim_callback(id, "pod-7#123")
      expect(store.find_batch(id)[:callback_dispatched_by]).to eq("pod-7#123")
    end
  end

  describe "#pending_jobs_total" do
    it "sums pending jobs across running batches only" do
      a = new_batch(total: 10)
      complete(batch_id: a, seq: 1, source_topic: "t", status: "success")  # pending 9
      new_batch(total: 5)  # pending 5
      done = new_batch(total: 2)
      store.update_batch_status(done, "success")  # excluded

      expect(store.pending_jobs_total).to eq(14)
    end
  end

  describe "#record_completions_batch" do
    it "dedups by batch_seq, aggregates per batch, and finalizes once" do
      id = new_batch(total: 2)
      events = [
        { batch_id: id, job_id: "j1", batch_seq: 1, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success" },
        { batch_id: id, job_id: "j1", batch_seq: 1, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success" }, # dup
        { batch_id: id, job_id: "j2", batch_seq: 2, source_topic: "wt", source_partition: 0, source_offset: 2, status: "failed" }
      ]
      result = store.record_completions_batch(events)

      b = store.find_batch(id)
      expect(b[:completed_count]).to eq(1)  # dup batch_seq counted once
      expect(b[:failed_count]).to eq(1)
      expect(result[:finished].size).to eq(1)
      expect(result[:finished].first[:outcome]).to eq("complete")
      expect(result[:replays]).to include(id)  # the duplicate event surfaced as a replay
    end

    it "is a no-op for an empty list" do
      expect(store.record_completions_batch([])).to eq(finished: [], replays: [])
    end
  end

  describe "failure metadata bounds" do
    it "caps the number of distinct failing jobs tracked per batch" do
      KafkaBatch.config.max_failures_per_batch = 2
      id = new_batch
      3.times do |i|
        store.record_failure(batch_id: id, job_id: "j#{i}", worker_class: "W",
                             error_class: "E", error_message: "m", status: "failed")
      end
      expect(store.list_failures(id).size).to eq(2)  # 3rd new job skipped at cap
    end

    it "still updates an already-tracked job once at the cap" do
      KafkaBatch.config.max_failures_per_batch = 1
      id = new_batch
      store.record_failure(batch_id: id, job_id: "j0", worker_class: "W",
                           error_class: "E", error_message: "m", status: "retrying")
      store.record_failure(batch_id: id, job_id: "j0", worker_class: "W",
                           error_class: "E", error_message: "m", status: "failed")
      failures = store.list_failures(id)
      expect(failures.size).to eq(1)
      expect(failures.first[:status]).to eq("failed")
    end
  end

  describe "#record_completion_by_offset" do
    it "counts continue -> done" do
      id = new_batch(total: 2)
      expect(complete(batch_id: id, seq: 1)[:status]).to eq(:continue)
      r = complete(batch_id: id, seq: 2)
      expect(r[:status]).to eq(:done)
      expect(r[:outcome]).to eq("success")
    end

    it "reports complete when a job failed" do
      id = new_batch(total: 2)
      complete(batch_id: id, seq: 1, status: "success")
      r = complete(batch_id: id, seq: 2, status: "failed")
      expect(r[:outcome]).to eq("complete")
    end

    it "dedups a replayed event for the same batch_seq" do
      id = new_batch(total: 2)
      complete(batch_id: id, seq: 1, status: "success")
      expect(complete(batch_id: id, seq: 1, status: "success")[:status]).to eq(:duplicate)
      expect(store.find_batch(id)[:completed_count]).to eq(1)
    end

    it "counts out-of-order completions on the same partition" do
      id = new_batch(total: 2)
      complete(batch_id: id, seq: 2, status: "success")
      r = complete(batch_id: id, seq: 1, status: "success")
      expect(r[:status]).to eq(:done)
      expect(store.find_batch(id)[:completed_count]).to eq(2)
    end

    it "tracks completions independently per (topic, partition)" do
      id = new_batch(total: 2)
      complete(batch_id: id, seq: 1, source_topic: "wt", source_partition: 0)
      r = complete(batch_id: id, seq: 2, source_topic: "wt", source_partition: 1)
      expect(r[:status]).to eq(:done)
    end

    it "moves the batch into the done index on completion" do
      id = new_batch(total: 1)
      complete(batch_id: id, seq: 1)
      expect(store.done_batches_without_callback(older_than: Time.now + 60).map { |b| b[:id] }).to include(id)
    end

    it "rejects completions without batch_seq" do
      id = new_batch(total: 1)
      expect(store.record_completion_by_offset(
        batch_id: id, source_topic: "wt", source_partition: 0,
        job_id: "j1", source_offset: 1, status: "success", batch_seq: 0
      )[:status]).to eq(:invalid)
    end

    it "uses O(1) bitmap storage for large batch_seq values" do
      id = new_batch(total: 10_000)
      store.seal_batch(id)
      complete(batch_id: id, seq: 10_000)
      expect(store.find_batch(id)[:completed_count]).to eq(1)
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
      complete(batch_id: id, seq: 1)
      expect(store.stale_batches(older_than: Time.now + 60).map { |b| b[:id] }).not_to include(id)
    end

    it "#done_batches_without_callback finds finished, unclaimed batches and prunes after claim" do
      id = new_batch(total: 1)
      complete(batch_id: id, seq: 1)

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

    it "#mark_finished stamps finished_at, moves running -> done, and syncs COUNTS_KEY" do
      id = new_batch
      store.mark_finished(id, "success")

      batch = store.find_batch(id)
      expect(batch[:status]).to eq("success")
      expect(batch[:finished_at]).not_to be_nil

      counts = store.batch_counts
      expect(counts["running"]).to eq(0)
      expect(counts["success"]).to eq(1)

      expect(store.stale_batches(older_than: Time.now + 60).map { |b| b[:id] }).not_to include(id)
      expect(store.done_batches_without_callback(older_than: Time.now + 60).map { |b| b[:id] }).to include(id)
    end
  end

  describe "#delete_batch" do
    it "removes the hash and all index entries" do
      id = new_batch(total: 1)
      store.delete_batch(id)
      expect(store.find_batch(id)).to be_nil
      expect(store.stale_batches(older_than: Time.now + 60).map { |b| b[:id] }).not_to include(id)
      expect(store.list_batches.map { |b| b[:id] }).not_to include(id)
    end
  end

  describe "failure tracking (#record_failure / #list_failures)" do
    it "records and lists failures, idempotent per job_id" do
      id = new_batch
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W", error_class: "RuntimeError", error_message: "boom")
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W", error_class: "RuntimeError", error_message: "boom")
      store.record_failure(batch_id: id, job_id: "j2", worker_class: "W", error_class: "ArgumentError", error_message: "boom2")

      failures = store.list_failures(id)
      expect(failures.map { |f| f[:job_id] }).to contain_exactly("j1", "j2")
      expect(failures.first[:error_class]).to be_a(String)
    end

    it "is removed with the batch" do
      id = new_batch
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W", error_class: "E", error_message: "x")
      store.delete_batch(id)
      expect(store.list_failures(id)).to be_empty
    end

    it "#clear_failure removes a single job's failure" do
      id = new_batch
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W", error_class: "E", error_message: "x", status: "retrying")
      store.clear_failure(id, "j1")
      expect(store.list_failures(id)).to be_empty
    end

    it "#list_all_failures aggregates across batches with status filter" do
      a = new_batch
      b = new_batch
      store.record_failure(batch_id: a, job_id: "j1", worker_class: "W", error_class: "E", error_message: "x", status: "retrying")
      store.record_failure(batch_id: b, job_id: "j2", worker_class: "W", error_class: "E", error_message: "y", status: "failed")

      expect(store.list_all_failures.map { |f| f[:batch_id] }).to contain_exactly(a, b)
      expect(store.list_all_failures(status: "failed").map { |f| f[:job_id] }).to eq(["j2"])
    end
  end

  describe "admin UI queries" do
    it "#batch_status returns the status (or nil when unknown)" do
      id = new_batch
      expect(store.batch_status(id)).to eq("running")
      expect(store.batch_status("nope")).to be_nil
    end

    it "#list_batches lists via the all-index with optional status filter" do
      a = new_batch
      b = new_batch
      store.update_batch_status(b, "cancelled")

      expect(store.list_batches.map { |x| x[:id] }).to include(a, b)
      expect(store.list_batches(status: "cancelled").map { |x| x[:id] }).to eq([b])
    end

    it "#batch_counts groups by status" do
      new_batch
      c = new_batch
      store.update_batch_status(c, "cancelled")

      counts = store.batch_counts
      expect(counts["running"]).to eq(1)
      expect(counts["cancelled"]).to eq(1)
    end

    it "#cancelled_batch_ids tracks cancellations and prunes on delete" do
      new_batch
      c = new_batch
      store.update_batch_status(c, "cancelled")
      expect(store.cancelled_batch_ids).to contain_exactly(c)

      store.delete_batch(c)
      expect(store.cancelled_batch_ids).to be_empty
    end
  end

  # ── batch_counts empty-key fallback (Bug #2) ─────────────────────────────
  describe "#batch_counts empty-key fallback" do
    it "rebuilds counts from ALL_INDEX when COUNTS_KEY is absent" do
      # Create two batches; let the normal Lua scripts populate COUNTS_KEY.
      a = new_batch(total: 1)
      b = new_batch(total: 1)
      store.update_batch_status(b, "cancelled")

      # Delete COUNTS_KEY to simulate a first-deploy / eviction scenario.
      Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
           .del(KafkaBatch::Stores::RedisStore::COUNTS_KEY)

      # batch_counts must fall back to scanning ALL_INDEX and reconstruct.
      counts = store.batch_counts
      expect(counts["running"]).to eq(1)
      expect(counts["cancelled"]).to eq(1)
    end

    it "returns {} when both COUNTS_KEY and ALL_INDEX are empty" do
      # After a full flush there are no batches and no keys.
      expect(store.batch_counts).to eq({})
    end

    it "prunes expired batch keys from ALL_INDEX during the fallback scan" do
      a = new_batch(total: 1)

      redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      # Manually inject a ghost id into ALL_INDEX (simulates a key that expired)
      ghost = SecureRandom.uuid
      redis.zadd(KafkaBatch::Stores::RedisStore::ALL_INDEX, Time.now.to_f, ghost)
      redis.del(KafkaBatch::Stores::RedisStore::COUNTS_KEY)

      store.batch_counts  # runs the fallback path

      # Ghost id should have been pruned from ALL_INDEX
      remaining = redis.zrange(KafkaBatch::Stores::RedisStore::ALL_INDEX, 0, -1)
      expect(remaining).not_to include(ghost)
      expect(remaining).to include(a)
    end
  end

  # ── cancelled_batch_ids WRONGTYPE migration (Bug #6) ─────────────────────
  describe "#cancelled_batch_ids WRONGTYPE legacy SET migration" do
    it "migrates a plain-SET CANCELLED_INDEX to ZSET on the first read after upgrade" do
      redis = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
      key   = KafkaBatch::Stores::RedisStore::CANCELLED_INDEX

      # Plant a legacy plain SET (pre-Bug#6 data shape)
      redis.del(key)
      redis.sadd(key, "legacy-batch-1")
      redis.sadd(key, "legacy-batch-2")

      # cancelled_batch_ids should gracefully migrate and return the ids
      ids = store.cancelled_batch_ids
      expect(ids).to include("legacy-batch-1", "legacy-batch-2")

      # The key must now be a ZSET (type migrated)
      expect(redis.type(key)).to eq("zset")
    end

    it "works normally when CANCELLED_INDEX is already a ZSET" do
      id = new_batch(total: 1)
      store.update_batch_status(id, "cancelled")
      expect(store.cancelled_batch_ids).to include(id)
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
