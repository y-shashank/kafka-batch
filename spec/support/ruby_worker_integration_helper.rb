# frozen_string_literal: true

require "socket"
require "yaml"
require "fileutils"

require_relative "go_daemon_helper"

module KafkaBatchSpec
  # Shared setup for Go daemon + Ruby WorkerServer integration specs (Phase 4).
  module RubyWorkerIntegrationHelper
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

    def integration_preflight!
      skip "set KAFKA_BATCH_INTEGRATION=1 to run" unless opted_in?
      require "rdkafka"
      skip "no Kafka broker reachable at #{brokers}" unless broker_reachable?
      skip "Go daemon binary unavailable" unless go_available?
    end

    def start_ruby_worker_stack!(tmpdir_prefix: "kbatch-ruby")
      @tmpdir = Dir.mktmpdir("#{tmpdir_prefix}-#{suffix}")
      @marker_path = File.join(@tmpdir, "marker")
      @worker_socket = File.join(Dir.tmpdir, "kb-rw-#{suffix}.sock")
      @events_topic = "kb.ruby.events.#{suffix}"
      @callbacks_topic = "kb.ruby.callbacks.#{suffix}"
      @dlt_topic = "kb.ruby.dlt.#{suffix}"
      @retry_base = "kb.ruby.retry.#{suffix}"

      write_manifest!
      write_daemon_config!
      create_integration_topics!

      configure_kafka_batch!
      ENV["KBATCH_RUBY_WORKER_ITEST_MARKER"] = @marker_path
      KafkaBatch::WorkerServer.start!(socket_path: @worker_socket)
      start_daemon!
    end

    def stop_ruby_worker_stack!
      stop_daemon! if @daemon_pid
      KafkaBatch::WorkerServer.stop!
      ENV.delete("KBATCH_RUBY_WORKER_ITEST_MARKER")
      FileUtils.rm_rf(@tmpdir) if @tmpdir
      KafkaBatch::Producer.reset! if opted_in?
    end

    def write_manifest!
      raise NotImplementedError
    end

    def write_daemon_config!
      @ready_path = File.join(@tmpdir, "ready")
      @config_path = File.join(@tmpdir, "daemon.yml")
      cfg = base_daemon_config.merge(extra_daemon_config)
      File.write(@config_path, cfg.to_yaml)
    end

    def base_daemon_config
      {
        "brokers" => brokers.split(","),
        "consumer_group" => "kb-ruby-#{suffix}",
        "events_topic" => @events_topic,
        "callbacks_topic" => @callbacks_topic,
        "dead_letter_topic" => @dlt_topic,
        "retry_topic" => @retry_base,
        "redis_url" => KafkaBatchSpec::RedisHelper::TEST_URL,
        "handler_manifest" => @manifest_path,
        "ruby_worker_socket" => @worker_socket,
        "max_retries" => 2,
        "complete_after_retries" => 1,
        "retry_tiers" => { "short" => 0, "medium" => 0, "large" => 0 }
      }
    end

    def extra_daemon_config
      {}
    end

    def create_integration_topics!
      topics = integration_topics
      topics.each { |t| create_topic!(t) }
    end

    def integration_topics
      [@events_topic, @callbacks_topic, @dlt_topic,
       "#{@retry_base}.short", "#{@retry_base}.medium", "#{@retry_base}.large"]
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
        c.daemon_mode = true
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
        "KBATCH_DAEMON_READY_FILE" => @ready_path,
        "REDIS_URL" => KafkaBatchSpec::RedisHelper::TEST_URL,
        "KAFKA_PREFIX" => ""
      )
      cmd = if File.executable?(daemon_binary)
              [daemon_binary, "--config", @config_path, "--manifest", @manifest_path]
            else
              ["go", "run", "./cmd/kbatch-daemon-ittest", "--config", @config_path, "--manifest", @manifest_path]
            end
      @err_log = File.join(@tmpdir, "daemon.err")
      @daemon_pid = Process.spawn(env, *cmd, chdir: File.expand_path("../../go", __dir__),
                                  out: File::NULL, err: @err_log)
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

    def wait_for_marker!(timeout: 45)
      deadline = Time.now + timeout
      until File.exist?(@marker_path) || Time.now >= deadline
        sleep 0.25
      end
      expect(File.exist?(@marker_path)).to be(true), -> { "daemon stderr:\n#{daemon_stderr}" }
    end

    def daemon_stderr
      File.exist?(@err_log) ? File.read(@err_log) : ""
    end

    def poll_dlt!(batch_id: nil, job_id: nil, timeout: 30)
      msg = KafkaBatchSpec::GoDaemonHelper.poll_topic(
        brokers: brokers,
        topic: @dlt_topic,
        group_suffix: suffix,
        timeout: timeout,
        match: lambda { |m|
          (batch_id.nil? || m["batch_id"] == batch_id) &&
            (job_id.nil? || m["job_id"] == job_id)
        }
      )
      expect(msg).not_to be_nil, "no DLT message for batch_id=#{batch_id} job_id=#{job_id}"
      msg
    end
  end
end
