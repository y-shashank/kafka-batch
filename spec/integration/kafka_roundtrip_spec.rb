# Real-Kafka integration tests.
#
# These exercise the ACTUAL producer path (no FakeProducer) and real Kafka
# semantics — topic creation, murmur2 partitioning, delivery coordinates, and
# reading the produced bytes back off the broker. They are the piece the mocked
# unit suite can't cover: that the envelopes we serialize are really consumable
# and that produce_sync returns usable partition/offset coordinates.
#
# They run only when a broker is reachable at KAFKA_BATCH_TEST_BROKERS
# (default "localhost:9092" once opted in via KAFKA_BATCH_INTEGRATION=1). When
# no broker is present the whole group is skipped, so `bundle exec rspec` stays
# green on a laptop with no Kafka. CI sets both env vars and runs a KRaft broker.
require "securerandom"

RSpec.describe "Kafka round-trip (integration)", :integration do
  def configured_brokers
    ENV["KAFKA_BATCH_TEST_BROKERS"].to_s
  end

  def opted_in?
    ENV["KAFKA_BATCH_INTEGRATION"] == "1" || !configured_brokers.empty?
  end

  def brokers
    @brokers ||= configured_brokers.empty? ? "localhost:9092" : configured_brokers
  end

  before(:each) do
    skip "set KAFKA_BATCH_INTEGRATION=1 (and optionally KAFKA_BATCH_TEST_BROKERS) to run" unless opted_in?

    require "rdkafka"
    skip "no Kafka broker reachable at #{brokers}" unless broker_reachable?(brokers)

    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.brokers   = brokers.split(",")
      c.logger    = Logger.new(File::NULL)
      c.redis_url = KafkaBatchSpec::RedisHelper::TEST_URL if KafkaBatchSpec::RedisHelper.available?
    end
    # The shared spec_helper mocks the producer for the unit suite; here we want
    # the real WaterDrop producer, so call through to it.
    allow(KafkaBatch::Producer).to receive(:produce_sync).and_call_original
    allow(KafkaBatch::Producer).to receive(:produce_many_sync).and_call_original
    KafkaBatch::Producer.reset!
  end

  after(:each) { KafkaBatch::Producer.reset! if opted_in? }

  # ── helpers ────────────────────────────────────────────────────────────────

  def broker_reachable?(brokers)
    cfg = Rdkafka::Config.new(:"bootstrap.servers" => brokers)
    admin = cfg.admin
    # metadata fetch with a short timeout; any success means the broker answered
    admin.metadata(nil, 3_000)
    true
  rescue StandardError
    false
  ensure
    admin&.close
  end

  def unique_topic(prefix)
    "#{prefix}.itest.#{SecureRandom.hex(6)}"
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

  # Poll a topic from the beginning until +count+ messages arrive or timeout.
  # When +match+ is given, only messages for which match.call(decoded_hash) is
  # truthy are counted — use this on shared/live topics to ignore stale records.
  def consume(topic, count: 1, timeout: 20, match: nil)
    cfg = Rdkafka::Config.new(
      :"bootstrap.servers"  => brokers,
      :"group.id"           => "kb-itest-#{SecureRandom.hex(4)}",
      :"auto.offset.reset"  => "earliest",
      :"enable.auto.commit" => false
    )
    consumer = cfg.consumer
    consumer.subscribe(topic)

    out      = []
    deadline = Time.now + timeout
    while out.size < count && Time.now < deadline
      msg = consumer.poll(1_000)
      next unless msg

      decoded = Oj.load(msg.payload)
      next if match && !match.call(decoded)

      out << msg
    end
    out
  ensure
    consumer&.close
  end

  def integration_worker(topic, job_type:, &perform_block)
    worker_name = "IntegrationWorker#{SecureRandom.hex(4)}"
    klass = Class.new do
      define_singleton_method(:name) { worker_name }

      include KafkaBatch::Worker
      kafka_topic topic, apply_prefix: false
      job_type job_type

      define_method(:perform, &perform_block)
    end
    klass
  end

  # ── tests ────────────────────────────────────────────────────────────────

  it "produces a message and reads the exact bytes back from the broker" do
    topic = unique_topic("kb.roundtrip")
    create_topic!(topic)

    payload = { "job_id" => "j-#{SecureRandom.hex(4)}", "n" => 42 }
    report  = KafkaBatch::Producer.produce_sync(topic: topic, payload: payload, key: "k1")

    expect(report.partition).to be_a(Integer)
    expect(report.offset).to be_a(Integer)

    got = consume(topic, count: 1)
    expect(got.size).to eq(1)
    decoded = Oj.load(got.first.payload)
    expect(decoded).to eq(payload)
  end

  it "returns delivery coordinates usable by Batch.delivery_coords" do
    topic  = unique_topic("kb.coords")
    create_topic!(topic)

    report            = KafkaBatch::Producer.produce_sync(topic: topic, payload: { "x" => 1 }, key: "k")
    partition, offset = KafkaBatch::Batch.delivery_coords(report)

    expect(partition).to be_a(Integer)
    expect(offset).to be_a(Integer)
    expect(partition).to be >= 0
    expect(offset).to be >= 0
  end

  it "bulk-produces via produce_many_sync and all messages land on the broker" do
    topic = unique_topic("kb.bulk")
    create_topic!(topic)

    msgs = Array.new(5) { |i| { topic: topic, payload: { "i" => i }, key: "k#{i}" } }
    KafkaBatch::Producer.produce_many_sync(msgs)

    got = consume(topic, count: 5)
    expect(got.size).to eq(5)
    ints = got.map { |m| Oj.load(m.payload)["i"] }.sort
    expect(ints).to eq([0, 1, 2, 3, 4])
  end

  it "routes a standalone enqueue to a real topic with a decodable Worker envelope" do
    topic  = unique_topic("kb.enqueue")
    create_topic!(topic)
    worker = integration_worker(topic, job_type: "integration.enqueue") { |_payload| }

    job_id = KafkaBatch::Batch.enqueue(worker, { "order_id" => 7 })
    expect(job_id).to be_a(String)

    got = consume(topic, count: 1, match: ->(env) { env["job_id"] == job_id })
    expect(got.size).to eq(1)

    envelope = Oj.load(got.first.payload)
    expect(envelope["job_type"]).to eq("integration.enqueue")
    expect(envelope["worker_class"]).to eq(worker.name)
    expect(envelope["job_id"]).to eq(job_id)
    expect(envelope["payload"]).to eq({ "order_id" => 7 })

    # The JobConsumer's own decoder accepts the real on-wire envelope.
    consumer = build_consumer(KafkaBatch::Consumers::JobConsumer)
    expect(consumer.send(:decode, got.first.payload)).to include(
      "worker_class" => worker.name,
      "job_type"     => "integration.enqueue"
    )
  end
end
