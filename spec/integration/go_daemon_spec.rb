# frozen_string_literal: true

require "socket"
require "yaml"
require "fileutils"

require_relative "../support/go_daemon_helper"

RSpec.describe "Go daemon (integration)", :integration do
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

  def base_handlers
    {
      "integration.go_daemon" => {
        "runtime" => "go",
        "topic" => @worker_topic,
        "apply_topic_prefix" => false,
        "max_retries" => 2
      },
      "integration.go_retry_once" => {
        "runtime" => "go",
        "topic" => @worker_topic,
        "apply_topic_prefix" => false,
        "max_retries" => 2
      },
      "integration.go_always_fail" => {
        "runtime" => "go",
        "topic" => @worker_topic,
        "apply_topic_prefix" => false,
        "max_retries" => 1,
        "complete_after_retries" => 1
      },
      "integration.go_multi" => {
        "runtime" => "go",
        "topic" => @worker_topic,
        "apply_topic_prefix" => false,
        "max_retries" => 1
      }
    }
  end

  before(:each) do
    skip "set KAFKA_BATCH_INTEGRATION=1 to run" unless opted_in?
    require "rdkafka"
    skip "no Kafka broker reachable at #{brokers}" unless broker_reachable?
    skip "Go daemon binary unavailable" unless go_available?

    @tmpdir = Dir.mktmpdir("kbatch-daemon-#{suffix}")
    @marker_path = File.join(@tmpdir, "marker")
    @worker_topic = "kb.daemon.worker.#{suffix}"
    @events_topic = "kb.daemon.events.#{suffix}"
    @callbacks_topic = "kb.daemon.callbacks.#{suffix}"
    @dlt_topic = "kb.daemon.dlt.#{suffix}"
    @retry_base = "kb.daemon.retry.#{suffix}"

    write_manifest!(base_handlers)
    write_daemon_config!

    [@worker_topic, @events_topic, @callbacks_topic, @dlt_topic,
     "#{@retry_base}.short", "#{@retry_base}.medium", "#{@retry_base}.large"].each do |t|
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
      "consumer_group" => "kb-daemon-#{suffix}",
      "jobs_topics" => [@worker_topic],
      "events_topic" => @events_topic,
      "callbacks_topic" => @callbacks_topic,
      "dead_letter_topic" => @dlt_topic,
      "retry_topic" => @retry_base,
      "redis_url" => KafkaBatchSpec::RedisHelper::TEST_URL,
      "handler_manifest" => @manifest_path,
      "max_retries" => 2,
      "complete_after_retries" => 1,
      "retry_tiers" => { "short" => 0, "medium" => 0, "large" => 0 }
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

  def create_topic!(name, partitions: 1)
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
      "KBATCH_DAEMON_ITEST_MARKER" => @marker_path,
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

  def wait_for_batch!(batch_id, status: %w[success complete], timeout: 45)
    deadline = Time.now + timeout
    loop do
      data = KafkaBatch.store.find_batch(batch_id)
      return data if data && status.include?(data[:status])
      raise "timeout waiting for batch #{batch_id} (last=#{data&.dig(:status)})" if Time.now >= deadline
      sleep 0.25
    end
  end

  def poll_topic(topic, timeout: 30, &match)
    KafkaBatchSpec::GoDaemonHelper.poll_topic(
      brokers: brokers, topic: topic, group_suffix: suffix, timeout: timeout, match: match
    )
  end

  it "completes a batch via Go daemon (push_job → daemon → Redis ledger)" do
    job_id = nil
    batch = KafkaBatch::Batch.create(description: "go daemon e2e #{suffix}") do |b|
      job_id = b.push_job("integration.go_daemon", { "ping" => 1 })
    end

    wait_for_batch!(batch.id)

    expect(File.exist?(@marker_path)).to be(true)
    expect(File.read(@marker_path)).to eq(job_id)

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("success")
    expect(reloaded[:completed_count]).to eq(1)
  end

  it "enqueue_job produces a consumable envelope on the worker topic" do
    job_id = KafkaBatch::Batch.enqueue_job("integration.go_daemon", { "k" => "v" })

    msg = poll_topic(@worker_topic, timeout: 20) { |m| m["job_id"] == job_id }
    expect(msg).not_to be_nil
    expect(msg["job_type"]).to eq("integration.go_daemon")
    expect(msg["worker_class"]).to eq("go:integration.go_daemon")
    expect(msg["payload"]).to eq("k" => "v")
  end

  it "finalizes a multi-job batch and emits a callback message" do
    batch = KafkaBatch::Batch.create(
      description: "multi #{suffix}",
      on_success: "TestCb",
      on_complete: "TestCb"
    ) do |b|
      3.times { |i| b.push_job("integration.go_multi", { "n" => i + 1 }) }
    end

    wait_for_batch!(batch.id)

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("success")
    expect(reloaded[:completed_count]).to eq(3)

    cb = poll_topic(@callbacks_topic, timeout: 20) { |m| m["batch_id"] == batch.id }
    expect(cb).not_to be_nil
    expect(cb["outcome"]).to eq("success")
    expect(cb["total_jobs"]).to eq(3)
    expect(cb["on_success"]).to eq("TestCb")
  end

  it "retries a failing job then completes the batch" do
    job_id = nil
    batch = KafkaBatch::Batch.create(description: "retry #{suffix}") do |b|
      job_id = b.push_job("integration.go_retry_once", { "ping" => 1 })
    end

    wait_for_batch!(batch.id)

    expect(File.read(@marker_path)).to eq(job_id)
    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("success")
    expect(reloaded[:completed_count]).to eq(1)
  end

  it "exhausts retries, publishes DLT, and finalizes batch as complete" do
    batch = KafkaBatch::Batch.create(description: "dlt #{suffix}") do |b|
      b.push_job("integration.go_always_fail", { "x" => 1 })
    end

    wait_for_batch!(batch.id, status: %w[complete])

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("complete")
    expect(reloaded[:failed_count]).to eq(1)

    dlt = poll_topic(@dlt_topic, timeout: 20) { |m| m["batch_id"] == batch.id }
    expect(dlt).not_to be_nil
    expect(dlt["dlt_type"]).to eq("job")
    expect(dlt["dlt_error_class"]).to eq("Permanent")
  end
end
