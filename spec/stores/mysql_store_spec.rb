RSpec.describe KafkaBatch::Stores::MysqlStore do
  subject(:store) { described_class.new }

  before(:each) do
    skip "Redis unavailable at #{KafkaBatchSpec::RedisHelper::TEST_URL}" unless KafkaBatchSpec::RedisHelper.available?
    KafkaBatch.config.store     = :mysql
    KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
    KafkaBatch.config.batch_ttl = 3600
    KafkaBatchSpec::RedisHelper.flush!
    KafkaBatchSpec::ActiveRecordSupport.truncate!
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
      expect(store.create_batch(id: id, total_jobs: 2)).to eq(0)
      expect(store.find_batch(id)[:total_jobs]).to eq(2)
    end
  end

  describe "open batches (add_jobs / seal_batch)" do
    it "add_jobs grows total_jobs while the batch is held (block-form)" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 0, sealed: false)
      expect(store.add_jobs(id, 5)).to eq({ status: :ok, seq_start: 1, seq_end: 5 })
      expect(store.find_batch(id)[:total_jobs]).to eq(5)
      expect(store.find_batch(id)[:locked_at]).to be_nil
    end

    it "does not finalize a held batch even when complete" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, sealed: false)
      r = complete(batch_id: id, seq: 1, status: "success")
      expect(r[:status]).to eq(:continue)
      expect(store.find_batch(id)[:status]).to eq("running")
    end

    it "seal_batch finalizes an already-complete batch and then closes it to add_jobs" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, sealed: false)
      complete(batch_id: id, seq: 1, status: "success")

      res = store.seal_batch(id)
      expect(res[:status]).to eq(:done)
      expect(res[:outcome]).to eq("success")
      expect(store.add_jobs(id, 1)).to eq(:closed)
    end

    it "seal_batch on an incomplete batch just seals it" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 3, sealed: false)
      expect(store.seal_batch(id)[:status]).to eq(:sealed)
      expect(store.find_batch(id)[:locked_at]).not_to be_nil
    end

    it "a sealed but still-running batch keeps accepting jobs (jobs adding jobs)" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, sealed: true)
      expect(store.add_jobs(id, 2)).to eq({ status: :ok, seq_start: 1, seq_end: 2 })
      expect(store.find_batch(id)[:total_jobs]).to eq(3)
    end

    it "add_jobs reports :not_found and :cancelled" do
      expect(store.add_jobs("nope", 1)).to eq(:not_found)
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

  describe "#pending_jobs_total" do
    it "sums pending jobs across running batches only" do
      a = SecureRandom.uuid
      store.create_batch(id: a, total_jobs: 10)
      complete(batch_id: a, seq: 1, source_topic: "t", status: "success")  # pending 9
      b = SecureRandom.uuid
      store.create_batch(id: b, total_jobs: 5)  # pending 5
      done = SecureRandom.uuid
      store.create_batch(id: done, total_jobs: 2)
      store.update_batch_status(done, "success")  # excluded (not running)

      expect(store.pending_jobs_total).to eq(14)
    end
  end

  describe "#record_completions_batch" do
    it "dedups by batch_seq, aggregates per batch, and finalizes once" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 2)
      events = [
        { batch_id: id, job_id: "j1", batch_seq: 1, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success" },
        { batch_id: id, job_id: "j1", batch_seq: 1, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success" }, # dup
        { batch_id: id, job_id: "j2", batch_seq: 2, source_topic: "wt", source_partition: 0, source_offset: 2, status: "failed" }
      ]
      result = store.record_completions_batch(events)

      b = store.find_batch(id)
      expect(b[:completed_count]).to eq(1)
      expect(b[:failed_count]).to eq(1)
      expect(result[:finished].size).to eq(1)
      expect(result[:finished].first[:outcome]).to eq("complete")
    end

    it "does not double-count across calls (dedup persists)" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 5)
      ev = ->(seq, st) { { batch_id: id, job_id: "j#{seq}", batch_seq: seq, source_topic: "wt", source_partition: 0, source_offset: seq, status: st } }

      store.record_completions_batch([ev.call(1, "success"), ev.call(2, "success")])
      store.record_completions_batch([ev.call(2, "success"), ev.call(3, "success")]) # seq 2 replayed

      b = store.find_batch(id)
      expect(b[:completed_count]).to eq(3)  # seq 1,2,3 — seq 2 not counted twice
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

  describe "#record_completion_by_offset" do
    it "counts continue -> done" do
      id = new_batch(total: 2)
      r1 = complete(batch_id: id, seq: 1, status: "success")
      expect(r1[:status]).to eq(:continue)

      r2 = complete(batch_id: id, seq: 2, status: "success")
      expect(r2[:status]).to eq(:done)
      expect(r2[:outcome]).to eq("success")
      expect(store.find_batch(id)[:status]).to eq("success")
    end

    it "marks the batch :done with outcome complete when any job fails" do
      id = new_batch(total: 2)
      complete(batch_id: id, seq: 1, status: "success")
      result = complete(batch_id: id, seq: 2, status: "failed")
      expect(result[:status]).to eq(:done)
      expect(result[:outcome]).to eq("complete")
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

    it "returns :not_found for an unknown batch" do
      r = store.record_completion_by_offset(batch_id: "nope", source_topic: "wt", source_partition: 0,
                                            job_id: "j1", batch_seq: 1, source_offset: 1, status: "success")
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
      complete(batch_id: id, seq: 1, status: "success")

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

  describe "failure tracking (#record_failure / #list_failures)" do
    it "records and lists failures (newest first)" do
      id = new_batch
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W", error_class: "RuntimeError", error_message: "boom1")
      store.record_failure(batch_id: id, job_id: "j2", worker_class: "W", error_class: "ArgumentError", error_message: "boom2")

      failures = store.list_failures(id)
      expect(failures.size).to eq(2)
      expect(failures.map { |f| f[:job_id] }).to contain_exactly("j1", "j2")
      expect(failures.first[:error_class]).to be_a(String)
    end

    it "upserts per (batch_id, job_id), updating status retrying -> failed" do
      id = new_batch
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W", error_class: "E", error_message: "x", attempt: 0, status: "retrying")
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W", error_class: "E2", error_message: "y", attempt: 2, status: "failed")

      failures = store.list_failures(id)
      expect(failures.size).to eq(1)
      expect(failures.first[:status]).to eq("failed")
      expect(failures.first[:attempt]).to eq(2)
      expect(failures.first[:error_class]).to eq("E2")
    end

    it "paginates" do
      id = new_batch
      5.times { |i| store.record_failure(batch_id: id, job_id: "j#{i}", worker_class: "W", error_class: "E", error_message: "x") }
      expect(store.list_failures(id, limit: 2).size).to eq(2)
      expect(store.list_failures(id, limit: 2, offset: 4).size).to eq(1)
    end

    it "is removed with the batch" do
      id = new_batch
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W", error_class: "E", error_message: "x")
      store.delete_batch(id)
      expect(store.list_failures(id)).to be_empty
    end

    it "#clear_failure removes a single job's failure (e.g. on a successful retry)" do
      id = new_batch
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W", error_class: "E", error_message: "x", status: "retrying")
      store.record_failure(batch_id: id, job_id: "j2", worker_class: "W", error_class: "E", error_message: "y", status: "retrying")
      store.clear_failure(id, "j1")
      expect(store.list_failures(id).map { |f| f[:job_id] }).to eq(["j2"])
    end

    it "#list_all_failures aggregates across batches, with batch_id and status filter" do
      a = new_batch
      b = new_batch
      store.record_failure(batch_id: a, job_id: "j1", worker_class: "W", error_class: "E", error_message: "x", status: "retrying")
      store.record_failure(batch_id: b, job_id: "j2", worker_class: "W", error_class: "E", error_message: "y", status: "failed")

      all = store.list_all_failures
      expect(all.map { |f| f[:batch_id] }).to contain_exactly(a, b)
      expect(all.first).to have_key(:batch_id)

      expect(store.list_all_failures(status: "failed").map { |f| f[:job_id] }).to eq(["j2"])
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
      expect(store.cancelled_batch_ids).to contain_exactly(c)
    end
  end

  # ── record_failure race / retry loop (Bug #4) ──────────────────────────
  describe "#record_failure retry loop on RecordNotUnique" do
    it "retries on RecordNotUnique and eventually updates the existing row" do
      id = new_batch
      # Pre-create the failure row so the race scenario (insert conflict) is real.
      store.record_failure(batch_id: id, job_id: "j1", worker_class: "W",
                           error_class: "E", error_message: "first", status: "retrying")

      call_count = 0
      original_create = store.send(:failure_class).method(:create!)
      # Force the first create! attempt to raise RecordNotUnique to simulate a race,
      # then fall through so the retry finds the row via find_by.
      allow(store.send(:failure_class)).to receive(:create!).and_wrap_original do |orig, **attrs|
        call_count += 1
        # Let the first simulated race raise, then let real logic take over.
        raise ActiveRecord::RecordNotUnique, "simulated race" if call_count == 1
        orig.call(**attrs)
      end

      # The update should succeed despite the simulated race.
      expect do
        store.record_failure(batch_id: id, job_id: "j1", worker_class: "W",
                             error_class: "E2", error_message: "updated", status: "failed")
      end.not_to raise_error

      failures = store.list_failures(id)
      expect(failures.size).to eq(1)
      expect(failures.first[:status]).to eq("failed")
    end

    it "logs an error and gives up after 3 retries" do
      id = new_batch
      # Force every create! to raise (worst-case: conflict never resolves).
      allow(store.send(:failure_class)).to receive(:create!).and_raise(
        ActiveRecord::RecordNotUnique, "persistent conflict"
      )
      # Also make find_by return nil each time so it always hits create!.
      allow(store.send(:failure_class)).to receive(:find_by).and_return(nil)

      expect(KafkaBatch.logger).to receive(:error).with(/record_failure upsert failed after 3 retries/)

      expect do
        store.record_failure(batch_id: id, job_id: "j1", worker_class: "W",
                             error_class: "E", error_message: "x")
      end.not_to raise_error
    end
  end

  describe "consumption pause/resume" do
    it "pauses and resumes a whole topic" do
      store.pause_consumption_topic(group: "g", topic: "demo")
      snap = store.consumption_pause_snapshot
      expect(snap[:topics]).to include(KafkaBatch::ConsumptionControl.topic_key("g", "demo"))

      store.resume_consumption_topic(group: "g", topic: "demo")
      expect(store.consumption_pause_snapshot[:topics]).to be_empty
    end

    it "pauses and resumes a single partition" do
      store.pause_consumption_partition(group: "g", topic: "demo", partition: 2)
      key = KafkaBatch::ConsumptionControl.partition_key("g", "demo", 2)
      expect(store.consumption_pause_snapshot[:partitions]).to include(key)

      store.resume_consumption_partition(group: "g", topic: "demo", partition: 2)
      expect(store.consumption_pause_snapshot[:partitions]).to be_empty
    end
  end
end
