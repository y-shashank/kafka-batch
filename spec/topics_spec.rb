RSpec.describe KafkaBatch::Topics do
  def stub_existing(names)
    info = double("cluster_info", topics: names.map { |n| { topic_name: n } })
    allow(Karafka::Admin).to receive(:cluster_info).and_return(info)
  end

  describe ".specs" do
    it "covers each plain worker's job topic + control + tier retries + DLT" do
      allow(KafkaBatch).to receive(:workers).and_return([SuccessfulWorker, FailingWorker])
      names = described_class.specs.map { |s| s[:name] }

      expect(names).to include(
        SuccessfulWorker.kafka_topic,   # per-worker job topic, not config.jobs_topic
        FailingWorker.kafka_topic,
        KafkaBatch.config.events_topic,
        KafkaBatch.config.callbacks_topic,
        KafkaBatch.config.dead_letter_topic,
        *KafkaBatch.config.retry_topics
      )
      expect(names).not_to include(KafkaBatch.config.jobs_topic)
    end

    it "skips registry entries that don't respond to kafka_topic rather than raising" do
      bad = Class.new # not a Worker, no kafka_topic
      allow(KafkaBatch).to receive(:workers).and_return([SuccessfulWorker, bad])
      expect { described_class.specs }.not_to raise_error
    end

    it "falls back to config.jobs_topic when no workers are registered" do
      allow(KafkaBatch).to receive(:workers).and_return([])
      names = described_class.specs.map { |s| s[:name] }

      expect(names).to include(KafkaBatch.config.jobs_topic)
    end

    it "creates one topic per retry tier" do
      tiers = described_class.specs.select { |s| KafkaBatch.config.retry_topics.include?(s[:name]) }
      expect(tiers.size).to eq(3)
      expect(tiers.map { |s| s[:partitions] }).to all(eq(KafkaBatch::Topics::DEFAULT_PARTITIONS[:retry]))
    end

    it "adds the time-lane ingest/ready when a :time fair worker is present" do
      allow(KafkaBatch).to receive(:workers).and_return([FairWorker, SuccessfulWorker])
      names = described_class.specs.map { |s| s[:name] }

      expect(names).to include(
        KafkaBatch.config.fairness_ingest_topic(:time),
        KafkaBatch.config.fairness_ready_topic(:time),
        KafkaBatch.config.fairness_ready_topic(:time, :go),
        KafkaBatch.config.fairness_ready_topic(:time, :ruby)
      )
      # Only the time lane is used, so throughput-lane topics are NOT provisioned.
      expect(names).not_to include(KafkaBatch.config.fairness_ingest_topic(:throughput))
      expect(names).to include(SuccessfulWorker.kafka_topic)  # plain worker still wired
      expect(names).not_to include(FairWorker.kafka_topic)    # fair worker uses the lane
    end

    it "adds the throughput-lane topics when a :throughput fair worker is present" do
      allow(KafkaBatch).to receive(:workers).and_return([ThroughputFairWorker])
      names = described_class.specs.map { |s| s[:name] }

      expect(names).to include(
        KafkaBatch.config.fairness_ingest_topic(:throughput),
        KafkaBatch.config.fairness_ready_topic(:throughput),
        KafkaBatch.config.fairness_ready_topic(:throughput, :go),
        KafkaBatch.config.fairness_ready_topic(:throughput, :ruby)
      )
      expect(names).not_to include(KafkaBatch.config.fairness_ingest_topic(:time))
    end

    it "omits ingest/ready when no worker opts into fairness" do
      allow(KafkaBatch).to receive(:workers).and_return([SuccessfulWorker])
      names = described_class.specs.map { |s| s[:name] }

      KafkaBatch::Configuration::FAIRNESS_TYPES.each do |ft|
        expect(names).not_to include(KafkaBatch.config.fairness_ingest_topic(ft))
        expect(names).not_to include(KafkaBatch.config.fairness_ready_topic(ft))
      end
    end

    it "includes topics from priority YAML config paths" do
      KafkaBatch.config.priority_config_paths = [
        File.expand_path("fixtures/priority/fast.yml", __dir__)
      ]
      names = described_class.specs.map { |s| s[:name] }

      expect(names).to include("kafka_batch.jobs.p0", "kafka_batch.jobs.p1")
    end

    it "forces every topic to the given partition count when provided" do
      specs = described_class.specs(partitions: 9)
      expect(specs.map { |s| s[:partitions] }).to all(eq(9))
    end

    it "applies the replication factor" do
      specs = described_class.specs(replication_factor: 3)
      expect(specs.map { |s| s[:replication_factor] }).to all(eq(3))
    end

    it "defaults replication factor from config" do
      KafkaBatch.config.topics_replication_factor = 3
      specs = described_class.specs
      expect(specs.map { |s| s[:replication_factor] }).to all(eq(3))
    end

    it "sets scheduled topic retention above max_schedule_horizon" do
      KafkaBatch.config.max_schedule_horizon = 7 * 24 * 3600
      scheduled = described_class.specs.find { |s| s[:name] == KafkaBatch.config.scheduled_topic }
      retention = scheduled[:config]["retention.ms"].to_i
      expect(retention).to be > KafkaBatch.config.max_schedule_horizon * 1000
    end

    it "sets dead_letter topic retention and cleanup policy" do
      dlt = described_class.specs.find { |s| s[:name] == KafkaBatch.config.dead_letter_topic }
      expect(dlt[:config]["cleanup.policy"]).to eq("delete")
      expect(dlt[:config]["retention.ms"].to_i).to be >= 30 * 24 * 3600 * 1000
    end
  end

  describe ".create_all!" do

    it "creates only the missing topics and skips the existing ones" do
      stub_existing([KafkaBatch.config.events_topic, KafkaBatch.config.callbacks_topic])
      created = []
      allow(Karafka::Admin).to receive(:create_topic) { |name, *_| created << name }

      result = described_class.create_all!

      expect(result[:skipped]).to include(KafkaBatch.config.events_topic, KafkaBatch.config.callbacks_topic)
      expect(created).to include(*KafkaBatch.config.retry_topics, KafkaBatch.config.dead_letter_topic)
      expect(result[:created]).to match_array(created)
      expect(result[:failed]).to be_empty
    end

    it "passes partition, replication factor, and topic config through to Karafka::Admin" do
      stub_existing([])
      allow(Karafka::Admin).to receive(:create_topic)

      described_class.create_all!(partitions: 4, replication_factor: 2)

      expect(Karafka::Admin).to have_received(:create_topic)
        .with(KafkaBatch.config.retry_topic_for(:short), 4, 2, {})
      expect(Karafka::Admin).to have_received(:create_topic)
        .with(
          KafkaBatch.config.scheduled_topic, 4, 2,
          hash_including("retention.ms", "cleanup.policy" => "delete")
        )
      expect(Karafka::Admin).to have_received(:create_topic)
        .with(
          KafkaBatch.config.dead_letter_topic, 4, 2,
          hash_including("retention.ms", "cleanup.policy" => "delete")
        )
    end

    it "treats an 'already exists' error as skipped, not failed" do
      stub_existing([])  # nothing known up front
      allow(Karafka::Admin).to receive(:create_topic).and_raise(StandardError.new("Topic already exists"))

      result = described_class.create_all!

      expect(result[:created]).to be_empty
      expect(result[:failed]).to be_empty
      expect(result[:skipped]).not_to be_empty
    end

    it "records genuine failures without aborting the rest" do
      stub_existing([])
      call = 0
      allow(Karafka::Admin).to receive(:create_topic) do |name, *_|
        call += 1
        raise StandardError, "boom" if call == 1
      end

      result = described_class.create_all!

      expect(result[:failed].size).to eq(1)
      expect(result[:created]).not_to be_empty
    end

    it "raises a clear error when Karafka::Admin is unavailable" do
      hide_const("Karafka::Admin")
      expect { described_class.create_all! }
        .to raise_error(KafkaBatch::ConfigurationError, /Karafka::Admin is required/)
    end
  end
end
