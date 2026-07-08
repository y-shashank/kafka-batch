# Phase 2 integration: real Kafka broker + real Go sidecar (kbatch-ittest).
#
#   Batch.push_job → JobConsumer → GoExecutor → Unix socket → Go handler
#
# Skipped when:
#   - no broker (same opt-in as other integration specs)
#   - Go toolchain / kbatch-ittest binary unavailable
#
# CI builds bin/kbatch-ittest before rspec. Locally:
#   cd go && go build -o ../bin/kbatch-ittest ./cmd/kbatch-ittest
require "securerandom"

require_relative "../support/go_sidecar_helper"

KMsg = Struct.new(:raw_payload, :topic, :partition, :offset, keyword_init: true)

RSpec.describe "Go sidecar (integration)", :integration do
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

  before(:each) do
    skip "set KAFKA_BATCH_INTEGRATION=1 to run" unless opted_in?
    require "rdkafka"
    skip "no Kafka broker reachable at #{brokers}" unless broker_reachable?

    skip "Go sidecar unavailable (install Go 1.24+ and build bin/kbatch-ittest)" \
      unless KafkaBatchSpec::GoSidecarHelper.available?

    @marker_path = File.join(Dir.tmpdir, "kbatch-marker-#{suffix}")
    File.delete(@marker_path) if File.exist?(@marker_path)

    @sidecar = KafkaBatchSpec::GoSidecarHelper.start!(marker_path: @marker_path)

    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.brokers             = brokers.split(",")
      c.logger              = Logger.new(File::NULL)
      c.redis_url           = KafkaBatchSpec::RedisHelper::TEST_URL
      c.go_executor_socket  = @sidecar[:socket_path]
      c.events_topic        = "kb.e2e.go.events.#{suffix}"
      c.callbacks_topic     = "kb.e2e.go.callbacks.#{suffix}"
    end

    KafkaBatch::HandlerManifest.load_from_hash(
      "handlers" => {
        KafkaBatchSpec::GoSidecarHelper::GO_JOB_TYPE => {
          "runtime"            => "go",
          "topic"              => @worker_topic = "kb.e2e.go.worker.#{suffix}",
          "apply_topic_prefix" => false,
          "max_retries"        => 1
        }
      }
    )

    KafkaBatchSpec::RedisHelper.flush!
    allow(KafkaBatch::Producer).to receive(:produce_sync).and_call_original
    allow(KafkaBatch::Producer).to receive(:produce_many_sync).and_call_original
    KafkaBatch::Producer.reset!
  end

  after(:each) do
    if defined?(@sidecar) && @sidecar
      KafkaBatchSpec::GoSidecarHelper.stop!(pid: @sidecar[:pid], socket_path: @sidecar[:socket_path])
    end
    File.delete(@marker_path) if @marker_path && File.exist?(@marker_path)
    KafkaBatch::Producer.reset! if opted_in?
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

  def drive_job_consumer(topic, job_id:, batch_id: nil, timeout: 30)
    rd = Rdkafka::Config.new(
      :"bootstrap.servers"  => brokers,
      :"group.id"           => "kb-go-e2e-#{suffix}",
      :"auto.offset.reset"  => "earliest",
      :"enable.auto.commit" => false
    ).consumer
    rd.subscribe(topic)

    deadline = Time.now + timeout
    loop do
      raw = rd.poll(1_000)
      break unless raw && Time.now < deadline

      decoded = Oj.load(raw.payload)
      next if decoded["job_id"] != job_id
      next if batch_id && decoded["batch_id"] != batch_id

      consumer  = build_consumer(KafkaBatch::Consumers::JobConsumer)
      committed = false
      allow(consumer).to receive(:mark_as_consumed!) { committed = true }
      wrapped = KMsg.new(raw_payload: raw.payload, topic: raw.topic, partition: raw.partition, offset: raw.offset)
      allow(consumer).to receive(:messages).and_return([wrapped])
      consumer.consume

      rd.commit if committed
      return committed
    end
    false
  ensure
    rd&.close
  end

  it "push_job → JobConsumer → Go sidecar executes over real Kafka" do
    create_topic!(@worker_topic)
    create_topic!(KafkaBatch.config.events_topic)

    job_id = nil
    batch = KafkaBatch::Batch.create do |b|
      job_id = b.push_job(KafkaBatchSpec::GoSidecarHelper::GO_JOB_TYPE, { "ping" => 1 })
    end

    expect(job_id).to be_a(String)
    expect(drive_job_consumer(@worker_topic, job_id: job_id, batch_id: batch.id)).to be(true)

    expect(File.exist?(@marker_path)).to be(true)
    expect(File.read(@marker_path)).to eq(job_id)
  end

  it "enqueue_job produces a consumable envelope with job_type and worker_class" do
    create_topic!(@worker_topic)

    job_id = KafkaBatch::Batch.enqueue_job(
      KafkaBatchSpec::GoSidecarHelper::GO_JOB_TYPE, { "k" => "v" }
    )
    expect(job_id).to be_a(String)

    cfg = Rdkafka::Config.new(
      :"bootstrap.servers"  => brokers,
      :"group.id"           => "kb-go-read-#{suffix}",
      :"auto.offset.reset"  => "earliest",
      :"enable.auto.commit" => false
    )
    consumer = cfg.consumer
    consumer.subscribe(@worker_topic)

    msg = nil
    deadline = Time.now + 20
    while msg.nil? && Time.now < deadline
      raw = consumer.poll(1_000)
      next unless raw

      decoded = Oj.load(raw.payload)
      msg = decoded if decoded["job_id"] == job_id
    end
    consumer.close

    expect(msg).not_to be_nil
    expect(msg["job_type"]).to eq(KafkaBatchSpec::GoSidecarHelper::GO_JOB_TYPE)
    expect(msg["worker_class"]).to eq("go:#{KafkaBatchSpec::GoSidecarHelper::GO_JOB_TYPE}")
    expect(msg["payload"]).to eq("k" => "v")
  end
end
