# frozen_string_literal: true

require "spec_helper"

RSpec.describe KafkaBatch::Consumers::PriorityJobConsumer do
  let(:consumer) { described_class.new }
  let(:spec) do
    {
      rank:                1,
      mode:                mode,
      higher_topics:       %w[kafka_batch.jobs.p0],
      consumer_group:      "kafka-batch-jobs-fast",
      topic:               "kafka_batch.jobs.p1",
      weighted_interleave: 4
    }
  end
  let(:klass) { described_class.build(spec) }

  before do
    consumer.extend(KafkaBatch::Consumers::PriorityGate)
    allow(consumer).to receive(:pause)
    allow(KafkaBatch::Instrumentation).to receive(:consumer_priority_yielded)
  end

  describe "weighted mode" do
    let(:mode) { :weighted }

    it "interleaves lower-rank work while higher topics have lag" do
      inst = klass.new
      allow(inst).to receive(:higher_topics_have_lag?).and_return(true)
      allow(inst).to receive(:pause)
      allow(inst).to receive(:super) # won't be called

      yields = 0
      allows = 0
      8.times do
        if inst.send(:should_yield_to_higher?, spec)
          yields += 1
        else
          allows += 1
        end
      end
      expect(allows).to eq(2)  # 1-in-4 interleave
      expect(yields).to eq(6)
    end
  end

  describe "strict mode" do
    let(:mode) { :strict }

    it "always yields when higher topics have lag" do
      inst = klass.new
      allow(inst).to receive(:higher_topics_have_lag?).and_return(true)
      expect(inst.send(:should_yield_to_higher?, spec)).to be(true)
    end
  end

  describe "#consume" do
    let(:mode) { :strict }

    it "rank 0 does not check higher-topic lag" do
      rank0 = described_class.build(spec.merge(rank: 0, higher_topics: [], mode: :strict))
      inst  = build_consumer(rank0)
      allow(inst).to receive(:messages).and_return([])
      expect(inst).not_to receive(:higher_topics_have_lag?)
      inst.consume
    end

    it "rank 1 pauses at the batch offset and skips processing when higher topics have lag" do
      inst = build_consumer(klass)
      msg  = instance_double("Karafka::Messages::Message", offset: 42)
      allow(inst).to receive(:messages).and_return([msg])
      allow(inst).to receive(:higher_topics_have_lag?).and_return(true)
      expect(inst).to receive(:pause).with(42, 2_000)
      expect(inst).not_to receive(:process_message)
      inst.consume
    end

    it "rank 1 processes when higher topics have no lag" do
      inst = build_consumer(klass)
      msg  = instance_double("Karafka::Messages::Message")
      allow(inst).to receive(:messages).and_return([msg])
      allow(inst).to receive(:higher_topics_have_lag?).and_return(false)
      expect(inst).not_to receive(:pause)
      expect(inst).to receive(:process_message).with(msg)
      inst.consume
    end
    it "rank 1 processes when a higher topic is topic-paused via /lag" do
      inst = build_consumer(klass)
      msg  = instance_double("Karafka::Messages::Message")
      allow(inst).to receive(:messages).and_return([msg])
      allow(KafkaBatch::ConsumptionControl).to receive(:topic_level_paused?)
        .with(group: spec[:consumer_group], topic: "kafka_batch.jobs.p0").and_return(true)
      expect(inst).not_to receive(:pause)
      expect(inst).to receive(:process_message).with(msg)
      inst.consume
    end

    # R1: a priority consumer must flow through the prepended ConsumptionGate, so
    # pausing ITS OWN topic via /lag actually stops it (regression guard: the
    # per-message loop must live in #process_messages, not override #consume).
    it "honors its own /lag pause (does not bypass the ConsumptionGate)" do
      inst  = build_consumer(klass)
      msg   = instance_double("Karafka::Messages::Message", offset: 7)
      group = instance_double("Karafka::Routing::ConsumerGroup", id: spec[:consumer_group])
      topic = instance_double("Karafka::Routing::Topic", name: spec[:topic], consumer_group: group)
      allow(inst).to receive(:messages).and_return([msg])
      allow(inst).to receive(:topic).and_return(topic)
      allow(inst).to receive(:partition).and_return(0)
      allow(KafkaBatch::Liveness).to receive(:heartbeat)
      allow(KafkaBatch::ConsumptionControl).to receive(:available?).and_return(true)
      allow(KafkaBatch::ConsumptionControl).to receive(:paused?)
        .with(group: spec[:consumer_group], topic: spec[:topic], partition: 0).and_return(true)

      expect(inst).to receive(:pause)          # gate pauses this consumer
      expect(inst).not_to receive(:process_message)  # …and nothing is processed
      inst.consume
    end
  end

  # R2: yielding mid-batch must seek to the message being yielded on, never to
  # messages.first — otherwise already-committed messages get redelivered/re-run.
  describe "mid-batch yield offset (weighted)" do
    let(:mode) { :weighted }

    it "seeks to the current message, not the batch head" do
      inst = build_consumer(klass)
      m0   = instance_double("Karafka::Messages::Message", offset: 100)
      m1   = instance_double("Karafka::Messages::Message", offset: 101)
      allow(inst).to receive(:messages).and_return([m0, m1])
      # Process m0, then yield on m1.
      allow(inst).to receive(:should_yield_to_higher?).and_return(false, true)
      allow(KafkaBatch::Instrumentation).to receive(:consumer_priority_yielded)

      expect(inst).to receive(:process_message).with(m0).ordered
      expect(inst).not_to receive(:process_message).with(m1)
      expect(inst).to receive(:pause).with(101, anything)  # m1.offset, NOT m0.offset
      inst.send(:process_messages)
    end
  end
end
