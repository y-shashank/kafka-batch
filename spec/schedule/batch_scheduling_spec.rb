RSpec.describe "KafkaBatch::Batch delayed scheduling (perform_in / perform_at)" do
  let(:sched) do
    instance_double(KafkaBatch::Schedule::RedisStore).tap { |s| allow(s).to receive(:schedule) }
  end

  before(:each) do
    allow(KafkaBatch).to receive(:schedule_store).and_return(sched)
    # produce_sync must return a delivery report carrying the (partition, offset)
    # the pointer is built from — while still recording the produce.
    allow(KafkaBatch::Producer).to receive(:produce_sync) do |topic:, payload:, key: nil, partition: nil, headers: {}|
      FakeProducer.record(topic: topic, payload: payload, key: key, partition: partition, headers: headers)
      double("report", partition: 2, offset: 77)
    end
  end

  describe ".enqueue_at" do
    it "produces the payload to the scheduled topic and records a compact pointer" do
      at = Time.now + 3600
      job_id = KafkaBatch::Batch.enqueue_at(at, SuccessfulWorker, { "id" => 1 })

      produced = FakeProducer.for_topic(KafkaBatch.config.scheduled_topic)
      expect(produced.size).to eq(1)
      expect(sched).to have_received(:schedule).with(
        hash_including(job_id: job_id, partition: 2, offset: 77, batch_id: nil)
      )
    end

    it "clamps a run-at beyond max_schedule_horizon down to the horizon" do
      KafkaBatch.config.max_schedule_horizon = 60
      KafkaBatch::Batch.enqueue_at(Time.now + 99_999, SuccessfulWorker, {})

      expect(sched).to have_received(:schedule) do |args|
        expect(args[:run_at]).to be <= (Time.now + 61)
      end
    end
  end

  describe ".enqueue_in" do
    it "schedules relative to now" do
      KafkaBatch::Batch.enqueue_in(120, SuccessfulWorker, {})

      expect(sched).to have_received(:schedule) do |args|
        expect(args[:run_at].to_i).to be_within(3).of((Time.now + 120).to_i)
      end
    end
  end

  describe "#push_at (batch-scoped)" do
    before(:each) do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "grows total_jobs immediately so the batch waits for the delayed job" do
      batch = KafkaBatch::Batch.create
      batch.push_at(Time.now + 300, SuccessfulWorker, { "id" => 9 })

      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(1)
      expect(sched).to have_received(:schedule).with(hash_including(batch_id: batch.id))
    end

    it "rolls back the reserved job count if scheduling fails" do
      allow(sched).to receive(:schedule).and_raise(KafkaBatch::ProducerError, "boom")
      batch = KafkaBatch::Batch.create

      expect { batch.push_at(Time.now + 300, SuccessfulWorker, {}) }
        .to raise_error(KafkaBatch::ProducerError)
      expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(0)
    end
  end

  describe "bulk delayed scheduling (one delay for many jobs)" do
    before(:each) do
      # WaterDrop#produce_many_sync returns an array of Rdkafka DeliveryHANDLES
      # (not reports) — each responds to #create_result, which yields the report
      # carrying partition/offset. Mirror that here so delivery_coords is exercised
      # exactly as in production (a plain report double would hide the handle path).
      allow(KafkaBatch::Producer).to receive(:produce_many_sync) do |messages|
        messages.each_with_index.map do |m, i|
          FakeProducer.record(topic: m[:topic], payload: m[:payload], key: m[:key])
          report = double("report", partition: 0, offset: 100 + i)
          double("handle", create_result: report)
        end
      end
    end

    describe ".enqueue_many_at" do
      it "produces all payloads in one call and bulk-writes pointers sharing one run_at" do
        at = Time.now + 900
        allow(sched).to receive(:schedule_many)

        ids = KafkaBatch::Batch.enqueue_many_at(at, SuccessfulWorker,
                                                [{ "id" => 1 }, { "id" => 2 }, { "id" => 3 }])

        expect(ids.size).to eq(3)
        expect(KafkaBatch::Producer).to have_received(:produce_many_sync).once
        expect(sched).to have_received(:schedule_many) do |entries|
          expect(entries.size).to eq(3)
          expect(entries.map { |e| e[:offset] }).to eq([100, 101, 102])
          expect(entries.map { |e| e[:run_at] }.uniq.size).to eq(1) # single shared delay
          expect(entries).to all(include(batch_id: nil))
        end
      end

      it "returns [] for empty payloads without producing" do
        expect(KafkaBatch::Batch.enqueue_many_at(Time.now + 5, SuccessfulWorker, [])).to eq([])
        expect(KafkaBatch::Producer).not_to have_received(:produce_many_sync)
      end
    end

    describe "#push_many_at (batch-scoped)" do
      before(:each) do
        skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
        KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
        KafkaBatchSpec::RedisHelper.flush!
      end

      it "grows total_jobs by the payload count in one reservation" do
        allow(sched).to receive(:schedule_many)
        batch = KafkaBatch::Batch.create

        ids = batch.push_many_at(Time.now + 300, SuccessfulWorker, [{ "id" => 1 }, { "id" => 2 }])

        expect(ids.size).to eq(2)
        expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(2)
        expect(sched).to have_received(:schedule_many) do |entries|
          expect(entries).to all(include(batch_id: batch.id))
        end
      end

      it "rolls back the full reservation if bulk scheduling fails before any produce" do
        allow(sched).to receive(:schedule_many).and_raise(KafkaBatch::ProducerError, "boom")
        batch = KafkaBatch::Batch.create

        expect { batch.push_many_at(Time.now + 300, SuccessfulWorker, [{}, {}, {}]) }
          .to raise_error(KafkaBatch::ProducerError)
        expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(0)
      end

      it "rolls back only the unscheduled remainder on partial bulk produce failure" do
        batch = KafkaBatch::Batch.create
        allow(KafkaBatch::Producer).to receive(:produce_many_sync) do |messages|
          msg = messages.first
          FakeProducer.record(topic: msg[:topic], payload: msg[:payload], key: msg[:key])
          report = double("report", partition: 0, offset: 100, error: nil)
          handle = double("handle", create_result: report)
          raise KafkaBatch::PartialProduceError.new("boom", dispatched: [handle])
        end
        allow(sched).to receive(:schedule_many)

        expect { batch.push_many_at(Time.now + 300, SuccessfulWorker, [{}, {}, {}]) }
          .to raise_error(KafkaBatch::PartialProduceError)
        expect(KafkaBatch.store.find_batch(batch.id)[:total_jobs]).to eq(1)
        expect(sched).to have_received(:schedule_many).once
      end
    end

    it "Worker.perform_bulk_in delegates to Batch.enqueue_many_in" do
      expect(KafkaBatch::Batch).to receive(:enqueue_many_in).with(60, SuccessfulWorker, [{ "id" => 1 }])
      SuccessfulWorker.perform_bulk_in(60, [{ "id" => 1 }])
    end
  end

  describe ".delivery_coords" do
    it "reads a DeliveryReport directly (produce_sync path)" do
      report = double("report", partition: 3, offset: 42)
      expect(KafkaBatch::Batch.delivery_coords(report)).to eq([3, 42])
    end

    it "resolves a DeliveryHandle via #create_result (produce_many_sync path)" do
      report = double("report", partition: 1, offset: 7)
      handle = double("handle", create_result: report)
      expect(KafkaBatch::Batch.delivery_coords(handle)).to eq([1, 7])
    end
  end

  describe "Worker convenience API" do
    it "perform_in delegates to Batch.enqueue_in" do
      expect(KafkaBatch::Batch).to receive(:enqueue_in).with(5, SuccessfulWorker, { "id" => 1 })
      SuccessfulWorker.perform_in(5, { "id" => 1 })
    end

    it "perform_at delegates to Batch.enqueue_at" do
      t = Time.now + 10
      expect(KafkaBatch::Batch).to receive(:enqueue_at).with(t, SuccessfulWorker, {})
      SuccessfulWorker.perform_at(t, {})
    end
  end
end
