RSpec.describe KafkaBatch::Lag do
  describe ".partitions" do
    it "returns [] when the admin API is unavailable" do
      allow(described_class).to receive(:available?).and_return(false)
      expect(described_class.partitions).to eq([])
    end

    it "flattens Karafka's lag map into sorted per-partition rows" do
      allow(described_class).to receive(:available?).and_return(true)
      allow(described_class).to receive(:read).and_return(
        "g-jobs" => {
          "demo" => {
            1 => { offset: 40, lag: 2 },
            0 => { offset: 10, lag: 0 }
          }
        },
        "g-control" => {
          "events" => {
            0 => { offset: -1, lag: -1 } # never consumed
          }
        }
      )

      rows = described_class.partitions
      # sorted by group, topic, partition
      expect(rows.map { |r| [r[:group], r[:topic], r[:partition]] }).to eq(
        [["g-control", "events", 0], ["g-jobs", "demo", 0], ["g-jobs", "demo", 1]]
      )

      never = rows.first
      expect(never[:never_consumed]).to eq(true)
      expect(never[:committed]).to be_nil
      expect(never[:end_offset]).to be_nil
      expect(never[:lag]).to eq(0)

      consumed = rows.last
      expect(consumed[:committed]).to eq(40)
      expect(consumed[:end_offset]).to eq(42) # committed + lag
      expect(consumed[:lag]).to eq(2)
    end
  end

  describe ".gem_groups_with_topics" do
    def fake_cg(id, topics)
      double(id: id, topics: topics.map { |t| double(name: t) })
    end

    it "selects only the gem's consumer groups (not the host app's)" do
      KafkaBatch.config.consumer_group = "kb"
      allow(Karafka::App).to receive(:routes).and_return([
        fake_cg("kb-control",   %w[events callbacks retry]),
        fake_cg("kb-dispatch",  %w[ingest]),
        fake_cg("kb-jobs-fair", %w[ready]),
        fake_cg("kb-jobs",      %w[demo]),
        fake_cg("app",          %w[orders.process])
      ])
      allow(KafkaBatch).to receive(:consumer_groups).and_return(
        %w[kb-control kb-dispatch kb-jobs-fair kb-jobs]
      )

      result = described_class.gem_groups_with_topics
      expect(result).to eq(
        "kb-control"   => %w[events callbacks retry],
        "kb-dispatch"  => %w[ingest],
        "kb-jobs-fair" => %w[ready],
        "kb-jobs"      => %w[demo]
      )
      expect(result).not_to have_key("app")
    end

    context "config-based fallback (no gem-owned routes drawn in this process)" do
      before do
        KafkaBatch.config.consumer_group = "kb"
        KafkaBatch.config.topic_prefix   = ""      # bare default topic names
        # No gem routes → gem_groups_with_topics uses config_based_groups.
        allow(Karafka::App).to receive(:routes).and_return([])
      end

      it "folds config.extra_job_topics into the -jobs group" do
        KafkaBatch.config.extra_job_topics = %w[orders.process]
        allow(KafkaBatch).to receive(:workers).and_return([]) # registry empty

        result = described_class.gem_groups_with_topics
        expect(result["kb-jobs"]).to eq(%w[kafka_batch.jobs orders.process])
      end

      it "folds registry worker topics into -jobs, excluding fair and priority workers" do
        KafkaBatch.config.extra_job_topics = []
        KafkaBatch.config.priority_config_paths = [
          File.expand_path("fixtures/priority/fast.yml", __dir__)
        ]
        plain    = double("plain",    fairness?: false, kafka_topic: "orders.process")
        fair     = double("fair",     fairness?: true,  kafka_topic: "should.be.ignored")
        priority = double("priority", fairness?: false, kafka_topic: "kafka_batch.jobs.p0")
        allow(KafkaBatch).to receive(:workers).and_return([plain, fair, priority])

        result = described_class.gem_groups_with_topics
        expect(result["kb-jobs"]).to eq(%w[kafka_batch.jobs orders.process])
        expect(result["kb-jobs-fast"]).to eq(%w[kafka_batch.jobs.p0 kafka_batch.jobs.p1])
      end

      it "includes go-worker execution groups for lag pause/resume" do
        KafkaBatch.config.handler_manifest_path = File.expand_path(
          "fixtures/handlers/go_ruby.yml", __dir__
        )
        KafkaBatch::HandlerManifest.load!(KafkaBatch.config.handler_manifest_path)
        KafkaBatch.config.priority_config_paths = [
          File.expand_path("fixtures/priority/fast.yml", __dir__)
        ]
        KafkaBatch.config.jobs_topics = %w[segment.exports]
        allow(KafkaBatch).to receive(:workers).and_return([])

        result = described_class.gem_groups_with_topics
        expect(result["kb-go-worker-jobs"]).to include("segment.exports")
        expect(result["kb-go-worker-jobs-fast"]).to eq(%w[kafka_batch.jobs.p0])
      end
    end
  end

  describe ".read" do
    it "returns {} (no admin call) when the gem's groups aren't routed" do
      allow(described_class).to receive(:gem_groups_with_topics).and_return({})
      expect(Karafka::Admin).not_to receive(:read_lags_with_offsets)
      expect(described_class.read).to eq({})
    end
  end

  describe "aggregation helpers (pure)" do
    let(:rows) do
      [
        { group: "g", topic: "a", partition: 0, lag: 5 },
        { group: "g", topic: "a", partition: 1, lag: 3 },
        { group: "g", topic: "b", partition: 0, lag: 0 }
      ]
    end

    it ".topics sums lag and counts partitions per (group, topic)" do
      expect(described_class.topics(rows)).to contain_exactly(
        { group: "g", topic: "a", partitions: 2, lag: 8 },
        { group: "g", topic: "b", partitions: 1, lag: 0 }
      )
    end

    it ".total sums lag across all rows" do
      expect(described_class.total(rows)).to eq(8)
    end
  end
end
