# frozen_string_literal: true

require "socket"
require "yaml"
require "fileutils"

require_relative "../support/go_daemon_helper"
require_relative "../support/ruby_callback_server"
require_relative "../support/callback_doubles"

RSpec.describe "Go daemon Ruby callbacks (integration)", :integration do
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

  before(:each) do
    skip "set KAFKA_BATCH_INTEGRATION=1 to run" unless opted_in?
    require "rdkafka"
    skip "no Kafka broker reachable at #{brokers}" unless broker_reachable?
    skip "Go daemon binary unavailable" unless go_available?

    KafkaBatchSpec::CallbackDoubles.reset!

    @tmpdir = Dir.mktmpdir("kbatch-cb-#{suffix}")
    @marker_path = File.join(@tmpdir, "marker")
    @callback_socket = File.join(Dir.tmpdir, "kb-cb-#{suffix}.sock")
    @worker_topic = "kb.cb.worker.#{suffix}"
    @events_topic = "kb.cb.events.#{suffix}"
    @callbacks_topic = "kb.cb.callbacks.#{suffix}"
    @dlt_topic = "kb.cb.dlt.#{suffix}"
    @retry_base = "kb.cb.retry.#{suffix}"

    write_manifest!
    write_daemon_config!

    [@worker_topic, @events_topic, @callbacks_topic, @dlt_topic,
     "#{@retry_base}.short", "#{@retry_base}.medium", "#{@retry_base}.large"].each do |t|
      create_topic!(t)
    end

    configure_kafka_batch!
    @callback_server = KafkaBatchSpec::RubyCallbackServer.new(socket_path: @callback_socket)
    @callback_server.start!
    start_daemon!
  end

  after(:each) do
    stop_daemon! if @daemon_pid
    @callback_server&.stop!
    FileUtils.rm_rf(@tmpdir) if @tmpdir
    KafkaBatch::Producer.reset! if opted_in?
  end

  def write_manifest!
    @manifest_path = File.join(@tmpdir, "handlers.yml")
    File.write(@manifest_path, {
      "handlers" => {
        "integration.go_daemon" => {
          "runtime" => "go",
          "topic" => @worker_topic,
          "apply_topic_prefix" => false,
          "max_retries" => 2
        }
      }
    }.to_yaml)
  end

  def write_daemon_config!
    @ready_path = File.join(@tmpdir, "ready")
    @config_path = File.join(@tmpdir, "daemon.yml")
    File.write(@config_path, {
      "brokers" => brokers.split(","),
      "consumer_group" => "kb-cb-#{suffix}",
      "jobs_topics" => [@worker_topic],
      "events_topic" => @events_topic,
      "callbacks_topic" => @callbacks_topic,
      "dead_letter_topic" => @dlt_topic,
      "retry_topic" => @retry_base,
      "redis_url" => KafkaBatchSpec::RedisHelper::TEST_URL,
      "handler_manifest" => @manifest_path,
      "ruby_callback_socket" => @callback_socket,
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

  def wait_for_callbacks!(batch_id, timeout: 45)
    deadline = Time.now + timeout
    loop do
      inv = KafkaBatchSpec::CallbackDoubles.invocations
      return inv if inv.any? { |i| i[:args]["batch_id"] == batch_id }
      raise "timeout waiting for Ruby callbacks batch=#{batch_id}" if Time.now >= deadline
      sleep 0.25
    end
  end

  it "invokes on_success and on_complete via the Ruby callback socket" do
    batch = KafkaBatch::Batch.create(
      description: "ruby cb #{suffix}",
      on_success: "RecordingCallback",
      on_complete: "RecordingCallback"
    ) do |b|
      b.push_job("integration.go_daemon", { "ping" => 1 })
    end

    wait_for_batch!(batch.id)
    invocations = wait_for_callbacks!(batch.id)

    methods = invocations.map { |i| i[:name] }
    expect(methods).to include(:on_success, :on_complete)

    success = invocations.find { |i| i[:name] == :on_success }
    expect(success[:args]["batch_id"]).to eq(batch.id)
    expect(success[:args]["outcome"]).to eq("success")

    expect(KafkaBatch.store.callback_dispatched?(batch.id)).to be(true)
  end
end
