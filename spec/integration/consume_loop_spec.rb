# Full consume-loop integration tests.
#
# Where kafka_roundtrip_spec proves produce → read-back, this proves the whole
# pipeline end-to-end against a REAL broker, driving the ACTUAL consumer classes
# over real fetched messages with REAL rdkafka offset commits:
#
#   Batch.push → (test.e2e)      → JobConsumer   → success events (events topic)
#              → EventConsumer   → batch finalizes → callback (callbacks topic)
#              → CallbackConsumer→ on_success/on_complete invoked
#
# The only thing not exercised is Karafka's own poll/rebalance orchestration; we
# stand in a minimal rdkafka poll loop and commit exactly when the consumer marks
# the message consumed — the same contract Karafka's mark_as_consumed! provides.
#
# Skipped unless a broker is reachable (see kafka_roundtrip_spec for the opt-in
# env vars). CI runs a KRaft broker and sets them.
require "securerandom"

# Minimal stand-in for a Karafka message wrapping a real rdkafka poll result.
KMsg = Struct.new(:raw_payload, :topic, :partition, :offset, keyword_init: true)

RSpec.describe "Full consume loop (integration)", :integration do
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
    skip "set KAFKA_BATCH_INTEGRATION=1 (and optionally KAFKA_BATCH_TEST_BROKERS) to run" unless opted_in?
    require "rdkafka"
    skip "no Kafka broker reachable at #{brokers}" unless broker_reachable?

    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.brokers        = brokers.split(",")
      c.logger         = Logger.new(File::NULL)
      c.redis_url      = KafkaBatchSpec::RedisHelper::TEST_URL
      # Unique events/callbacks topics per run so the downstream stages are
      # fully isolated from any other producer on the shared broker.
      c.events_topic   = "kb.e2e.events.#{suffix}"
      c.callbacks_topic = "kb.e2e.callbacks.#{suffix}"
    end
    KafkaBatchSpec::RedisHelper.flush!
    KafkaBatchSpec::WorkerRuns.reset!
    KafkaBatchSpec::CallbackDoubles.reset!

    # Use the real producer (spec_helper mocks it for the unit suite).
    allow(KafkaBatch::Producer).to receive(:produce_sync).and_call_original
    allow(KafkaBatch::Producer).to receive(:produce_many_sync).and_call_original
    KafkaBatch::Producer.reset!
  end

  after(:each) { KafkaBatch::Producer.reset! if opted_in? }

  # ── Kafka helpers ──────────────────────────────────────────────────────────

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

  def rd_consumer(group)
    Rdkafka::Config.new(
      :"bootstrap.servers"  => brokers,
      :"group.id"           => group,
      :"auto.offset.reset"  => "earliest",
      :"enable.auto.commit" => false
    ).consumer
  end

  # Drive the real consumer class over `topic`: poll real messages, run the true
  # consume path (gate + logic + store + downstream produce), and commit the
  # offset through rdkafka only when the consumer marked the message consumed.
  # Returns the number of messages the consumer committed.
  #
  # When +batch_id+ is set, only messages belonging to that batch are processed
  # (stale records on a reused broker are skipped but still committed).
  def drive(consumer_class, topic, expect:, timeout: 30, batch_id: nil)
    rd = rd_consumer("kb-e2e-#{consumer_class.name.split('::').last}-#{suffix}")
    rd.subscribe(topic)
    processed = 0
    deadline  = Time.now + timeout

    while processed < expect && Time.now < deadline
      raw = rd.poll(1_000)
      next unless raw

      decoded = Oj.load(raw.payload)
      if batch_id && decoded["batch_id"] != batch_id
        commit(rd)
        next
      end

      consumer  = build_consumer(consumer_class)
      committed = false
      allow(consumer).to receive(:mark_as_consumed!) { committed = true }
      # Skip the periodic reconciler so it can't independently re-fire callbacks
      # mid-test and confuse assertions.
      allow(consumer).to receive(:maybe_reconcile) if consumer.respond_to?(:maybe_reconcile, true)

      wrapped = KMsg.new(raw_payload: raw.payload, topic: raw.topic, partition: raw.partition, offset: raw.offset)
      allow(consumer).to receive(:messages).and_return([wrapped])

      consumer.consume
      # SuperFetch marks before #perform; wait for the pool so assertions see
      # events/retries produced by the async pipeline.
      KafkaBatch::SuperFetch.drain(timeout: 30) if defined?(KafkaBatch::SuperFetch)

      if committed
        commit(rd)
        processed += 1
      end
    end

    processed
  ensure
    rd&.close
  end

  def e2e_worker_for(topic)
    worker_name = "E2EWorker#{suffix}"
    Class.new do
      define_singleton_method(:name) { worker_name }

      include KafkaBatch::Worker
      kafka_topic topic, apply_prefix: false
      job_type "e2e"

      def perform(payload)
        KafkaBatchSpec::WorkerRuns.record(:e2e, payload)
      end
    end
  end

  def commit(rd)
    rd.commit
  rescue Rdkafka::RdkafkaError
    # "no_offset" when nothing new to commit — safe to ignore in this harness.
  end

  # ── tests ────────────────────────────────────────────────────────────────

  it "completes a batch end-to-end through real Kafka: job → event → callback" do
    worker_topic = "kb.e2e.worker.#{suffix}"
    worker       = e2e_worker_for(worker_topic)
    [worker_topic, KafkaBatch.config.events_topic, KafkaBatch.config.callbacks_topic].each { |t| create_topic!(t) }

    batch = KafkaBatch::Batch.create(on_success: "RecordingCallback", on_complete: "RecordingCallback") do |b|
      3.times { |i| b.push(worker, { "n" => i }) }
    end

    # Stage 1: JobConsumer runs each job and emits a success event.
    expect(drive(KafkaBatch::Consumers::JobConsumer, worker_topic, expect: 3, batch_id: batch.id)).to eq(3)
    expect(KafkaBatchSpec::WorkerRuns.runs.count { |r| r[:name] == :e2e }).to eq(3)

    # Stage 2: EventConsumer counts the 3 completions and finalizes the batch.
    drive(KafkaBatch::Consumers::EventConsumer, KafkaBatch.config.events_topic, expect: 3, batch_id: batch.id)

    reloaded = KafkaBatch.store.find_batch(batch.id)
    expect(reloaded[:status]).to eq("success")
    expect(reloaded[:completed_count]).to eq(3)

    # Stage 3: CallbackConsumer fires the batch callback.
    drive(KafkaBatch::Consumers::CallbackConsumer, KafkaBatch.config.callbacks_topic, expect: 1)

    names = KafkaBatchSpec::CallbackDoubles.invocations.map { |i| i[:name] }
    expect(names).to include(:on_success)
    summary = KafkaBatchSpec::CallbackDoubles.invocations.first[:args]
    expect(summary["batch_id"]).to eq(batch.id)
    expect(summary["total_jobs"]).to eq(3)
  end

  it "commits offsets for real: a restarted consumer group does not reprocess" do
    topic = "kb.commit.#{suffix}"
    create_topic!(topic)

    3.times { |i| KafkaBatch::Producer.produce_sync(topic: topic, payload: { "i" => i }, key: "k#{i}") }

    group = "kb-commit-group-#{suffix}"

    # First pass: consume all 3 and commit.
    first = rd_consumer(group)
    first.subscribe(topic)
    seen = 0
    deadline = Time.now + 20
    while seen < 3 && Time.now < deadline
      m = first.poll(1_000)
      next unless m
      seen += 1
      first.commit
    end
    first.close
    expect(seen).to eq(3)

    # Second pass: same group, committed offsets persist → nothing to reprocess.
    second = rd_consumer(group)
    second.subscribe(topic)
    again = 0
    deadline = Time.now + 8
    while Time.now < deadline
      m = second.poll(1_000)
      again += 1 if m
    end
    second.close
    expect(again).to eq(0)
  end
end
