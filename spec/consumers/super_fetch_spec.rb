# frozen_string_literal: true

require "spec_helper"

RSpec.describe "JobConsumer SuperFetch" do
  let(:consumer) { build_consumer(KafkaBatch::Consumers::JobConsumer) }

  def job_message(worker:, batch_id:, job_id:, attempt: 0)
    FakeMessage.new(
      topic: worker.kafka_topic,
      payload: {
        "job_id"       => job_id,
        "batch_id"     => batch_id,
        "job_type"     => worker.job_type,
        "worker_class" => worker.name,
        "payload"      => { "x" => 1 },
        "attempt"      => attempt,
        "max_retries"  => worker.max_retries
      }
    )
  end

  before do
    skip "Redis not available" unless KafkaBatchSpec::RedisHelper.available?
  end

  it "marks the offset before #perform starts" do
    msg = job_message(worker: SuccessfulWorker, batch_id: "b1", job_id: "sf-mark-1")
    order = []
    mutex = Mutex.new

    allow(consumer).to receive(:mark_as_consumed!) do
      mutex.synchronize { order << :mark }
    end
    allow_any_instance_of(SuccessfulWorker).to receive(:perform) do |_, payload|
      mutex.synchronize { order << :perform_start }
      KafkaBatchSpec::WorkerRuns.record(:success, payload)
    end

    allow(consumer).to receive(:messages).and_return([msg])
    consumer.send(:process_messages)
    KafkaBatch::SuperFetch.drain(timeout: 10)

    expect(order.first).to eq(:mark)
    expect(order).to include(:perform_start)
    expect(order.index(:mark)).to be < order.index(:perform_start)
  end

  it "acks a lost claim without running the worker" do
    msg = job_message(worker: SuccessfulWorker, batch_id: "b1", job_id: "sf-lost-1")

    # Another live owner already claimed this job_id.
    KafkaBatch::Workset.store.claim(
      job_id: "sf-lost-1", payload: msg.raw_payload, topic: msg.topic,
      partition: 0, offset: 0, consumer_id: "other-owner",
      lease_ttl: 60, steal_grace: -1
    )

    allow(consumer).to receive(:messages).and_return([msg])
    consumer.send(:process_messages)
    KafkaBatch::SuperFetch.drain(timeout: 5)

    expect(consumer).to have_received(:mark_as_consumed!).with(msg)
    expect(KafkaBatchSpec::WorkerRuns.runs).to be_empty
  end

  it "acks an in-flight redelivery without a second perform" do
    msg = job_message(worker: SuccessfulWorker, batch_id: "b1", job_id: "sf-inflight-1")
    gate = Queue.new
    started = Queue.new

    allow_any_instance_of(SuccessfulWorker).to receive(:perform) do |_, payload|
      started << true
      gate.pop
      KafkaBatchSpec::WorkerRuns.record(:success, payload)
    end

    allow(consumer).to receive(:messages).and_return([msg])
    t = Thread.new { consumer.send(:process_messages) }
    started.pop # first perform is running

    # Redelivery of the same job_id while still in-flight.
    consumer2 = build_consumer(KafkaBatch::Consumers::JobConsumer)
    allow(consumer2).to receive(:messages).and_return([msg])
    consumer2.send(:process_messages)

    expect(consumer2).to have_received(:mark_as_consumed!).with(msg)

    gate << true
    t.join
    KafkaBatch::SuperFetch.drain(timeout: 10)

    expect(KafkaBatchSpec::WorkerRuns.runs.map { |r| r[:name] }).to eq([:success])
  end

  it "Completes the workset after a successful SuperFetch perform" do
    msg = job_message(worker: SuccessfulWorker, batch_id: "b1", job_id: "sf-done-1")
    allow(consumer).to receive(:messages).and_return([msg])
    consumer.send(:process_messages)
    KafkaBatch::SuperFetch.drain(timeout: 10)

    expect(KafkaBatch::Workset.store.get_entry("sf-done-1")).to be_nil
    expect(KafkaBatchSpec::WorkerRuns.runs.map { |r| r[:name] }).to eq([:success])
  end

  it "clears fair-slot dedup on _reclaim so perform runs again" do
    slot_id = "slot-sf-reclaim"
    sched = instance_double(KafkaBatch::Fairness::Scheduler)
    allow(KafkaBatch).to receive(:scheduler).with(:time).and_return(sched)
    expect(sched).to receive(:clear_slot_execution!).with(slot_id).ordered
    expect(sched).to receive(:claim_slot_execution!).with(slot_id).and_return(true).ordered
    allow(sched).to receive(:complete)
    allow(sched).to receive(:renew_lease)
    allow(sched).to receive(:lease_ttl).and_return(1800)

    msg = FakeMessage.new(
      topic: KafkaBatch.config.fairness_ready_topic(:time),
      payload: {
        "job_id"        => "sf-fair-r1",
        "batch_id"      => "b1",
        "job_type"      => FairWorker.job_type,
        "worker_class"  => FairWorker.name,
        "payload"       => { "x" => 1 },
        "attempt"       => 0,
        "max_retries"   => 2,
        "tenant_id"     => "t1",
        "_fair_slot"    => true,
        "_fair_slot_id" => slot_id,
        "_fair_type"    => "time",
        "_reclaim"      => true
      }
    )

    allow(consumer).to receive(:messages).and_return([msg])
    consumer.send(:process_messages)
    KafkaBatch::SuperFetch.drain(timeout: 10)

    expect(KafkaBatchSpec::WorkerRuns.runs.map { |r| r[:name] }).to eq([:fair])
  end
end
