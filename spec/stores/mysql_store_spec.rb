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

  describe "open batches (add_jobs / seal_batch)" do
    it "add_jobs grows total_jobs while the batch is held (block-form)" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 0, sealed: false)
      expect(store.add_jobs(id, 5)).to eq(:ok)
      expect(store.find_batch(id)[:total_jobs]).to eq(5)
      expect(store.find_batch(id)[:locked_at]).to be_nil
    end

    it "does not finalize a held batch even when complete" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, sealed: false)
      r = store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success")
      expect(r[:status]).to eq(:continue)
      expect(store.find_batch(id)[:status]).to eq("running")
    end

    it "seal_batch finalizes an already-complete batch and then closes it to add_jobs" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 1, sealed: false)
      store.record_completion_by_offset(batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success")

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
      expect(store.add_jobs(id, 2)).to eq(:ok)
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
      store.record_completion_by_offset(batch_id: a, source_topic: "t", source_partition: 0, source_offset: 1, status: "success")  # pending 9
      b = SecureRandom.uuid
      store.create_batch(id: b, total_jobs: 5)  # pending 5
      done = SecureRandom.uuid
      store.create_batch(id: done, total_jobs: 2)
      store.update_batch_status(done, "success")  # excluded (not running)

      expect(store.pending_jobs_total).to eq(14)
    end
  end

  describe "#record_completions_batch" do
    it "dedups by offset, aggregates per batch, and finalizes once" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 2)
      events = [
        { batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success" },
        { batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 1, status: "success" }, # dup
        { batch_id: id, source_topic: "wt", source_partition: 0, source_offset: 2, status: "failed" }
      ]
      finalized = store.record_completions_batch(events)

      b = store.find_batch(id)
      expect(b[:completed_count]).to eq(1)
      expect(b[:failed_count]).to eq(1)
      expect(finalized.size).to eq(1)
      expect(finalized.first[:outcome]).to eq("complete")
    end

    it "does not double-count across calls (cursor persists)" do
      id = SecureRandom.uuid
      store.create_batch(id: id, total_jobs: 5)
      ev = ->(off, st) { { batch_id: id, source_topic: "wt", source_partition: 0, source_offset: off, status: st } }

      store.record_completions_batch([ev.call(1, "success"), ev.call(2, "success")])
      store.record_completions_batch([ev.call(2, "success"), ev.call(3, "success")]) # offset 2 replayed

      b = store.find_batch(id)
      expect(b[:completed_count]).to eq(3)  # offsets 1,2,3 — 2 not counted twice
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

  describe "liveness heartbeats (:store backend)" do
    it "upserts, lists active, and sweeps stale heartbeats" do
      store.record_heartbeat("c1", hostname: "h1", pid: 11, topic: "t",
                             current_job_id: "j1", current_worker: "W", jobs_done: 2)

      active = store.list_heartbeats(Time.now - 60)
      expect(active.map { |h| h[:consumer_id] }).to eq(["c1"])
      expect(active.first[:current_job_id]).to eq("j1")
      expect(active.first[:jobs_done]).to eq(2)

      # upsert same consumer (no duplicate row)
      store.record_heartbeat("c1", hostname: "h1", pid: 11, topic: "t2", current_job_id: nil, jobs_done: 3)
      reread = store.list_heartbeats(Time.now - 60)
      expect(reread.size).to eq(1)
      expect(reread.first[:current_job_id]).to be_nil
      expect(reread.first[:jobs_done]).to eq(3)

      # sweep everything
      store.sweep_stale_heartbeats(Time.now + 60)
      expect(store.list_heartbeats(Time.now - 60)).to be_empty
    end

    it "excludes heartbeats older than the since cutoff" do
      store.record_heartbeat("c1", hostname: "h", pid: 1, jobs_done: 0)
      expect(store.list_heartbeats(Time.now + 60)).to be_empty  # nothing newer than the future
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
