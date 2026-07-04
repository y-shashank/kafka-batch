require "oj"

# Covers the two concurrent fairness lanes (:time + :throughput): the per-worker
# fairness_type DSL, per-lane routing, active-lane detection, and per-lane weight
# isolation. A batch may mix jobs of both lanes.
RSpec.describe "Dual fairness lanes (:time + :throughput)" do
  describe "Worker#fairness_type DSL" do
    it "defaults to :time for a fair worker without an explicit type" do
      expect(FairWorker.fairness_type).to eq(:time)
    end

    it "reflects an explicit :throughput declaration" do
      expect(ThroughputFairWorker.fairness_type).to eq(:throughput)
    end

    it "rejects an unknown fairness_type" do
      klass = Class.new do
        include KafkaBatch::Worker
        kafka_topic "x"
      end
      expect { klass.fairness_type(:bogus) }.to raise_error(ArgumentError, /:time or :throughput/)
    end
  end

  describe "KafkaBatch.active_fairness_types" do
    it "returns only the lanes registered workers actually use" do
      allow(KafkaBatch).to receive(:workers).and_return([FairWorker])
      expect(KafkaBatch.active_fairness_types).to eq([:time])

      allow(KafkaBatch).to receive(:workers).and_return([FairWorker, ThroughputFairWorker, SuccessfulWorker])
      expect(KafkaBatch.active_fairness_types).to contain_exactly(:time, :throughput)
    end
  end

  describe "Batch.route_for routes each worker to its lane" do
    it "sends a :time fair worker to the time-lane ingest topic" do
      route = KafkaBatch::Batch.route_for(FairWorker, job_id: "j", tenant_id: "acme")
      expect(route[:topic]).to eq(KafkaBatch.config.fair_time_ingest_topic)
    end

    it "sends a :throughput fair worker to the throughput-lane ingest topic" do
      route = KafkaBatch::Batch.route_for(ThroughputFairWorker, job_id: "j", tenant_id: "acme")
      expect(route[:topic]).to eq(KafkaBatch.config.fair_throughput_ingest_topic)
    end

    it "sends a plain worker to its own topic" do
      route = KafkaBatch::Batch.route_for(SuccessfulWorker, job_id: "j")
      expect(route[:topic]).to eq(SuccessfulWorker.kafka_topic)
    end
  end

  describe "a single batch mixing both lanes" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "produces the time job to the time lane and the throughput job to the throughput lane" do
      batch = KafkaBatch::Batch.create(tenant_id: "acme")
      batch.push(FairWorker, {})
      batch.push(ThroughputFairWorker, {})

      expect(FakeProducer.for_topic(KafkaBatch.config.fair_time_ingest_topic).size).to eq(1)
      expect(FakeProducer.for_topic(KafkaBatch.config.fair_throughput_ingest_topic).size).to eq(1)
    end
  end

  describe "per-lane weight isolation" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.store     = :redis
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "keeps time-lane and throughput-lane weights and namespaces separate" do
      time_sched = KafkaBatch.scheduler(:time)
      tp_sched   = KafkaBatch.scheduler(:throughput)

      # Distinct Redis namespaces.
      expect(time_sched.ns).to eq("kafka_batch:fair_time")
      expect(tp_sched.ns).to eq("kafka_batch:fair_throughput")
      expect(time_sched.ring).not_to eq(tp_sched.ring)

      # A weight set in one lane is invisible to the other.
      time_sched.set_weight("acme", 5.0)
      tp_sched.set_weight("acme", 2.0)

      time_row = time_sched.all_tenants.find { |t| t[:tenant_id] == "acme" }
      tp_row   = tp_sched.all_tenants.find { |t| t[:tenant_id] == "acme" }
      expect(time_row[:weight]).to eq(5.0)
      expect(tp_row[:weight]).to eq(2.0)
    end

    it "returns the same instance per lane and distinct instances across lanes" do
      expect(KafkaBatch.scheduler(:time)).to be(KafkaBatch.scheduler(:time))
      expect(KafkaBatch.scheduler(:time)).not_to be(KafkaBatch.scheduler(:throughput))
    end
  end
end
