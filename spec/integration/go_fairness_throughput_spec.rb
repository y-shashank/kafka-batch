# frozen_string_literal: true

require "socket"
require "yaml"
require "fileutils"

require_relative "../support/go_daemon_helper"

RSpec.describe "Go daemon throughput fairness (integration)", :integration do
  def configured_brokers
    ENV["KAFKA_BATCH_TEST_BROKERS"].to_s
  end

  def opted_in?
    ENV["KAFKA_BATCH_INTEGRATION"] == "1" || !configured_brokers.empty?
  end

  def brokers
    @brokers ||= configured_brokers.empty? ? "localhost:9092" : configured_brokers
  end

  def suffix
    @suffix ||= SecureRandom.hex(6)
  end

  def daemon_binary
    ENV.fetch("KBATCH_DAEMON_ITEST_BIN") do
      File.expand_path("../../bin/kbatch-daemon-ittest", __dir__)
    end
  end

  def go_available?
    File.executable?(daemon_binary) || system("which go >/dev/null 2>&1")
  end

  def throughput_handlers
    {
      "integration.go_fair_throughput" => {
        "runtime" => "go",
        "topic" => @worker_topic,
        "apply_topic_prefix" => false,
        "fairness_type" => "throughput",
        "max_retries" => 2
      }
    }
  end

  before(:each) do
    skip "set KAFKA_BATCH_INTEGRATION=1 to run" unless opted_in?
    require "rdkafka"
    skip "no Kafka broker reachable at #{brokers}" unless broker_reachable?
    skip "Go daemon binary unavailable" unless go_available?

    @tmpdir = Dir.mktmpdir("kbatch-fair-tp-#{suffix}")
    @marker_path = File.join(@tmpdir, "marker_tp")
    @worker_topic = "kb.fair.tp.worker.#{suffix}"
    @fair_ingest_topic = "kb.fair.tp.ingest.#{suffix}"
    @fair_ready_topic = "kb.fair.tp.ready.#{suffix}"
    @events_topic = "kb.fair.tp.events.#{suffix}"
    @callbacks_topic = "kb.fair.tp.callbacks.#{suffix}"
    @dlt_topic = "kb.fair.tp.dlt.#{suffix}"
    @retry_base = "kb.fair.tp.retry.#{suffix}"

    write_manifest!(throughput_handlers)
    write_daemon_config!

    [@worker_topic, @fair_ingest_topic, @fair_ready_topic, @events_topic,
     @callbacks_topic, @dlt_topic, "#{@retry_base}.short",
     "#{@retry_base}.medium", "#{@retry_base}.large"].each do |t|
      create_topic!(t)
    end

    configure_kafka_batch!
    start_daemon!
  end

  after(:each) do
    stop_daemon! if @daemon_pid
    FileUtils.rm_rf(@tmpdir) if @tmpdir
    KafkaBatch::Producer.reset! if opted_in?
  end

  def write_manifest!(handlers)
    @manifest_path = File.join(@tmpdir, "handlers.yml")
    File.write(@manifest_path, { "handlers" => handlers }.to_yaml)
  end

  def write_daemon_config!
    @ready_path = File.join(@tmpdir, "ready")
    @config_path = File.join(@tmpdir, "daemon.yml")
    File.write(@config_path, {
      "brokers" => brokers.split(","),
      "consumer_group" => "kb-fair-tp-#{suffix}",
      "jobs_topics" => [@worker_topic],
      "events_topic" => @events_topic,
      "callbacks_topic" => @callbacks_topic,
      "dead_letter_topic" => @dlt_topic,
      "retry_topic" => @retry_base,
      "redis_url" => KafkaBatchSpec::RedisHelper::TEST_URL,
      "handler_manifest" => @manifest_path,
      "max_retries" => 2,
      "complete_after_retries" => 1,
      "retry_tiers" => { "short" => 0, "medium" => 0, "large" => 0 },
      "fairness_enabled" => true,
      "fairness_throughput_ingest" => @fair_ingest_topic,
      "fairness_throughput_ready" => @fair_ready_topic,
      "fairness_ready_window" => 100,
      "fairness_global_concurrency" => 4,
      "fairness_lease_ttl" => 300,
      "fairness_default_weight" => 1.0,
      "fairness_weighted_concurrency" => false
    }.to_yaml)
  end

  def configure_kafka_batch!
    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.brokers = brokers.split(",")
      c.logger = Logger.new(File::NULL)
      c.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL
      c.go_executor_socket = ""
      c.handler_manifest_path = @manifest_path
      c.callbacks_topic = @callbacks_topic
      c.fair_throughput_ingest_topic = @fair_ingest_topic
      c.fair_throughput_ready_topic = @fair_ready_topic
    end
    KafkaBatch::HandlerManifest.load!(@manifest_path)
    KafkaBatchSpec::RedisHelper.flush!

    allow(KafkaBatch::Producer).to receive(:produce_sync).and_call_original
    KafkaBatch::Producer.reset!
  end

  def broker_reachable?
    cfg = Rdkafka::Config.new(:"bootstrap.servers" => brokers)
    admin = cfg.admin
    admin.metadata(nil, 3_000)
    true
  rescue StandardError
    false
  ensure
    admin&.close
  end

  def create_topic!(name, partitions: 3)
    cfg   = Rdkafka::Config.new(:"bootstrap.servers" => brokers)
    admin = cfg.admin
    admin.create_topic(name, partitions, 1).wait(max_wait_timeout: 15)
  rescue Rdkafka::RdkafkaError => e
    raise unless e.message.to_s =~ /exist/i
  ensure
    admin&.close
  end

  def start_daemon!
    env = ENV.to_h.merge(
      "KBATCH_DAEMON_ITEST_MARKER_TP" => @marker_path,
      "KBATCH_DAEMON_READY_FILE" => @ready_path,
      "REDIS_URL" => KafkaBatchSpec::RedisHelper::TEST_URL,
      "KAFKA_PREFIX" => ""
    )
    cmd = if File.executable?(daemon_binary)
            [daemon_binary, "--config", @config_path, "--manifest", @manifest_path]
          else
            ["go", "run", "./cmd/kbatch-daemon-ittest", "--config", @config_path, "--manifest", @manifest_path]
          end
    @daemon_pid = Process.spawn(env, *cmd, chdir: File.expand_path("../../go", __dir__),
                                out: File::NULL, err: File::NULL)
    wait_for_daemon!
  end

  def wait_for_daemon!(timeout: 30)
    deadline = Time.now + timeout
    while Time.now < deadline
      return if File.exist?(@ready_path)
      Process.kill(0, @daemon_pid)
      sleep 0.2
    end
    raise "daemon did not become ready within #{timeout}s"
  rescue Errno::ESRCH
    raise "daemon process died during startup"
  end

  def stop_daemon!
    Process.kill("TERM", @daemon_pid)
    Timeout.timeout(5) { Process.wait(@daemon_pid) }
  rescue Errno::ESRCH, Timeout::Error
    Process.kill("KILL", @daemon_pid) rescue nil
  end

  def wait_for_batch!(batch_id, status: %w[success complete], timeout: 60)
    deadline = Time.now + timeout
    loop do
      data = KafkaBatch.store.find_batch(batch_id)
      return data if data && status.include?(data[:status])
      raise "timeout waiting for batch #{batch_id} (last=#{data&.dig(:status)})" if Time.now >= deadline
      sleep 0.25
    end
  end

  def fair_inflight_total
    r = Redis.new(url: KafkaBatchSpec::RedisHelper::TEST_URL)
    r.zcard("kafka_batch:fair_throughput:leases")
  ensure
    r&.close
  end

  it "routes throughput-fair jobs through ingest → forwarder → ready and completes the batch" do
    job_id = nil
    batch = KafkaBatch::Batch.create(description: "go fair throughput #{suffix}") do |b|
      job_id = b.push_job("integration.go_fair_throughput", { "tenant" => "acme" }, tenant_id: "acme")
    end

    wait_for_batch!(batch.id)

    expect(File.exist?(@marker_path)).to be(true)
    expect(File.read(@marker_path)).to eq("#{job_id}:acme")

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("success")
    expect(reloaded[:completed_count]).to eq(1)
    expect(fair_inflight_total).to eq(0)
  end

  it "interleaves two tenants on the throughput lane without leaking slots" do
    batch = KafkaBatch::Batch.create(description: "tp two-tenant #{suffix}") do |b|
      b.push_job("integration.go_fair_throughput", { "tenant" => "A" }, tenant_id: "A")
      b.push_job("integration.go_fair_throughput", { "tenant" => "B" }, tenant_id: "B")
      b.push_job("integration.go_fair_throughput", { "tenant" => "A" }, tenant_id: "A")
    end

    wait_for_batch!(batch.id)

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("success")
    expect(reloaded[:completed_count]).to eq(3)
    expect(fair_inflight_total).to eq(0)
  end
end
