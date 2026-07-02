RSpec.describe KafkaBatch::Liveness do
  describe "with Redis available" do
    before do
      skip "Redis unavailable" unless KafkaBatchSpec::RedisHelper.available?
      KafkaBatch.config.redis_url          = KafkaBatchSpec::RedisHelper::TEST_URL
      KafkaBatch.config.track_running_jobs = true
      KafkaBatch.config.liveness_ttl       = 30
      described_class.reset!
      KafkaBatchSpec::RedisHelper.flush!
    end

    it "reports available" do
      expect(described_class.available?).to be(true)
    end

    it "records a running job and clears it on finish" do
      described_class.job_started(job_id: "j1", batch_id: "b1", worker_class: "W", topic: "t", partition: 0)
      running = described_class.running_jobs
      expect(running.map { |j| j["job_id"] }).to include("j1")
      expect(running.first["consumer_id"]).to eq(described_class.consumer_id)

      described_class.job_finished("j1")
      expect(described_class.running_jobs.map { |j| j["job_id"] }).not_to include("j1")
    end

    it "registers a consumer heartbeat" do
      described_class.heartbeat(topic: "test.success")
      consumers = described_class.consumers
      expect(consumers.map { |c| c["consumer_id"] }).to include(described_class.consumer_id)
      expect(consumers.first["pid"]).to eq(Process.pid)
    end

    it "no-ops job tracking when track_running_jobs is false" do
      KafkaBatch.config.track_running_jobs = false
      described_class.job_started(job_id: "j9", batch_id: "b1", worker_class: "W")
      KafkaBatch.config.track_running_jobs = true
      expect(described_class.running_jobs.map { |j| j["job_id"] }).not_to include("j9")
    end

    it "still registers heartbeats when track_running_jobs is false" do
      KafkaBatch.config.track_running_jobs = false
      described_class.heartbeat(topic: "test.success")
      KafkaBatch.config.track_running_jobs = true
      expect(described_class.consumers.map { |c| c["consumer_id"] }).to include(described_class.consumer_id)
    end

    it "includes throttled rss/cpu stats on heartbeats when enabled" do
      KafkaBatch.config.liveness_stats_interval = 0
      described_class.heartbeat(topic: "t")
      expect(described_class.consumers.first).not_to have_key("rss_bytes")

      KafkaBatch.config.liveness_stats_interval = 15
      described_class.reset!
      allow(KafkaBatch::ProcessStats).to receive(:sample).and_return("rss_bytes" => 128_000_000, "cpu_pct" => 12.5)
      described_class.heartbeat(topic: "t")
      c = described_class.consumers.first
      expect(c["rss_bytes"]).to eq(128_000_000)
      expect(c["cpu_pct"]).to eq(12.5)
    end
  end

  describe "when the backend is :off" do
    before do
      KafkaBatch.config.liveness_backend = :off
      described_class.reset!
    end

    after { KafkaBatch.config.liveness_backend = :redis }

    it "reports unavailable and no-ops all entry points" do
      expect(described_class.available?).to be(false)
      expect { described_class.job_started(job_id: "j1", batch_id: "b1", worker_class: "W") }.not_to raise_error
      expect { described_class.heartbeat(topic: "t") }.not_to raise_error
      expect(described_class.running_jobs).to eq([])
      expect(described_class.consumers).to eq([])
    end
  end

  describe "when Redis is not reachable" do
    before do
      KafkaBatch.config.redis_url          = "redis://127.0.0.1:6390/0" # nothing listening
      KafkaBatch.config.track_running_jobs = true
      described_class.reset!
    end

    it "reports unavailable and never raises on writes" do
      expect(described_class.available?).to be(false)
      expect { described_class.job_started(job_id: "j1", batch_id: "b1", worker_class: "W") }.not_to raise_error
      expect { described_class.heartbeat(topic: "t") }.not_to raise_error
      expect(described_class.running_jobs).to eq([])
    end
  end
end
