require "oj"

RSpec.describe KafkaBatch::SchedulePoller do
  # In-memory schedule store double that records ack/reclaim so we can assert
  # exactly what the poller decided to remove vs. leave leased for recovery.
  class FakeScheduleStore
    attr_reader :acked, :reclaim_calls

    def initialize(due: [])
      @due           = due            # array of member strings
      @acked         = []
      @reclaim_calls = 0
    end

    def claim_due(now:, lease_seconds:, limit:)
      d = @due.first(limit)
      @due = @due.drop(limit)
      d
    end

    def ack(members)
      @acked.concat(Array(members))
      Array(members).size
    end

    def reclaim(now:)
      @reclaim_calls += 1
      0
    end
  end

  # Reader double: maps "partition:offset" -> payload string; anything listed in
  # `lost` reports as retention-deleted.
  class FakeReader
    def initialize(found: {}, lost: [])
      @found = found
      @lost  = lost
    end

    def read(_by_partition)
      { found: @found, lost: @lost }
    end

    def close; end
  end

  def payload_for(job_id:, batch_id: nil, worker: SuccessfulWorker)
    Oj.dump(
      KafkaBatch::Batch.build_message(
        worker_class: worker, payload: { "n" => 1 },
        job_id: job_id, batch_id: batch_id, attempt: 0
      ),
      mode: :compat
    )
  end

  before(:each) do
    allow(KafkaBatch::CancellationCache).to receive(:cancelled?).and_return(false)
  end

  describe "#tick — happy path" do
    it "reads the due payload, re-produces it to the worker's topic, and acks it" do
      member  = "j1:0:5"
      store   = FakeScheduleStore.new(due: [member])
      reader  = FakeReader.new(found: { "0:5" => payload_for(job_id: "j1") })
      poller  = described_class.new(store: store, reader: reader)

      dispatched = poller.tick

      expect(dispatched).to eq(1)
      produced = FakeProducer.for_topic(SuccessfulWorker.kafka_topic)
      expect(produced.size).to eq(1)
      expect(store.acked).to eq([member]) # removed only after produce
    end
  end

  describe "#tick — crash recovery (at-least-once)" do
    it "does NOT ack a job whose re-produce failed, so the lease/reclaim path retries it" do
      member = "j2:0:7"
      store  = FakeScheduleStore.new(due: [member])
      reader = FakeReader.new(found: { "0:7" => payload_for(job_id: "j2") })
      poller = described_class.new(store: store, reader: reader)

      FakeProducer.raise_for { |topic| topic == SuccessfulWorker.kafka_topic }

      dispatched = poller.tick

      expect(dispatched).to eq(0)
      expect(store.acked).to be_empty # left leased → recovered by reclaim later
    end
  end

  describe "#tick — cancelled batch" do
    it "drops (acks without producing) a job whose batch was cancelled" do
      member = "j3:0:9"
      store  = FakeScheduleStore.new(due: [member])
      reader = FakeReader.new(found: { "0:9" => payload_for(job_id: "j3", batch_id: "b1") })
      poller = described_class.new(store: store, reader: reader)

      allow(KafkaBatch::CancellationCache).to receive(:cancelled?).with("b1").and_return(true)

      poller.tick

      expect(FakeProducer.for_topic(SuccessfulWorker.kafka_topic)).to be_empty
      expect(store.acked).to eq([member]) # dropped, not retried
    end
  end

  describe "#tick — payload lost to retention" do
    it "drops the job (acks) rather than retrying forever when the offset is gone" do
      member = "j4:1:3"
      store  = FakeScheduleStore.new(due: [member])
      reader = FakeReader.new(found: {}, lost: ["1:3"])
      poller = described_class.new(store: store, reader: reader)

      poller.tick

      expect(FakeProducer.for_topic(SuccessfulWorker.kafka_topic)).to be_empty
      expect(store.acked).to eq([member])
    end
  end

  describe "#tick — payload not fetched this pass" do
    it "leaves the job leased (no ack) so a later pass/reclaim can retry" do
      member = "j5:0:1"
      store  = FakeScheduleStore.new(due: [member])
      reader = FakeReader.new(found: {}, lost: []) # neither found nor lost
      poller = described_class.new(store: store, reader: reader)

      poller.tick

      expect(store.acked).to be_empty
    end
  end

  # Adaptive idle backoff keeps many pods from hammering the schedule store while
  # nothing is due (the main throttle on idle DB/Redis load with a large fleet).
  describe "#run — adaptive idle backoff" do
    # Run the loop for exactly n iterations, capturing each sleep duration.
    def run_iterations(poller, n)
      sleeps = []
      allow(poller).to receive(:sleep) { |s| sleeps << s }
      i = 0
      allow(poller).to receive(:running?) { (i += 1) <= n }
      poller.run
      sleeps
    end

    before do
      KafkaBatch.config.schedule_poll_interval     = 1.0
      KafkaBatch.config.schedule_poll_max_interval = 8.0
      KafkaBatch.config.schedule_poll_jitter       = 0.0
    end

    it "doubles the idle sleep each empty poll, capped at schedule_poll_max_interval" do
      poller = described_class.new(store: FakeScheduleStore.new(due: []), reader: FakeReader.new)
      expect(run_iterations(poller, 5)).to eq([1.0, 2.0, 4.0, 8.0, 8.0])
    end

    it "snaps back to the base cadence the moment a poll returns work" do
      store = instance_double("store", reclaim: 0, ack: nil)
      # idle, idle, work, idle
      allow(store).to receive(:claim_due).and_return([], [], ["jw:0:5"], [])
      reader = FakeReader.new(found: { "0:5" => payload_for(job_id: "jw") })
      poller = described_class.new(store: store, reader: reader)

      # iter1 idle→sleep 1 (wait→2); iter2 idle→sleep 2 (wait→4);
      # iter3 work→NO sleep (wait→1); iter4 idle→sleep 1
      expect(run_iterations(poller, 4)).to eq([1.0, 2.0, 1.0])
    end
  end
end
