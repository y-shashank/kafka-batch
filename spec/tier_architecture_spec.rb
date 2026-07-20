# frozen_string_literal: true

require "spec_helper"
require_relative "support/route_capture"

RSpec.describe "Three-tier architecture (Ruby)" do
  let(:cg) { KafkaBatch.config.consumer_group }

  before do
    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.daemon_mode = false
      c.priority_config_paths = []
      # Ready topics are always runtime-split (.go / .ruby) — use the defaults.
    end
    [SuccessfulWorker, FairWorker].each { |w| KafkaBatch.register_worker(w) }
  end

  def capture_routes
    capture = KafkaBatchSpec::RouteCapture.new
    KafkaBatch.draw_routes(capture)
    capture
  end

  describe "tier 1 — client (produce only)" do
    it "skips all Karafka consumers when daemon_mode is enabled" do
      KafkaBatch.configure { |c| c.daemon_mode = true }
      builder = double("karafka_routes")
      expect(builder).not_to receive(:instance_eval)
      expect(KafkaBatch.logger).to receive(:warn).with(/daemon_mode enabled/)

      KafkaBatch.draw_routes(builder)
    end

    it "does not require workers to be registered for producer-only pods" do
      KafkaBatch.reset!
      KafkaBatch.configure { |c| c.daemon_mode = true }
      expect(KafkaBatch.draw_routes(double("routes"))).to be_nil
    end
  end

  describe "tier 2 — control (events, retry, callbacks, fair ingest)" do
    it "registers control topics in a dedicated consumer group" do
      capture = capture_routes
      control = capture.groups["#{cg}-control"]

      expect(control).to contain_exactly(
        KafkaBatch.config.events_topic,
        KafkaBatch.config.callbacks_topic,
        *KafkaBatch.config.retry_topics
      )
    end

    it "wires control consumers separately from job execution" do
      capture = capture_routes
      cfg = KafkaBatch.config

      expect(capture.consumers[["#{cg}-control", cfg.events_topic]])
        .to eq(KafkaBatch::Consumers::EventConsumer)
      expect(capture.consumers[["#{cg}-control", cfg.callbacks_topic]])
        .to eq(KafkaBatch::Consumers::CallbackConsumer)
      cfg.retry_topics.each do |rt|
        expect(capture.consumers[["#{cg}-control", rt]])
          .to eq(KafkaBatch::Consumers::RetryConsumer)
      end
    end

    it "registers fair ingest dispatch in its own group (not jobs-fair)" do
      capture = capture_routes
      ingest  = KafkaBatch.config.fairness_ingest_topic(:time)
      dispatch_group = "#{cg}-dispatch-time"
      jobs_fair_group = "#{cg}-jobs-fair-time"

      expect(capture.groups[dispatch_group]).to eq([ingest])
      expect(capture.consumers[[dispatch_group, ingest]])
        .to eq(KafkaBatch::Fairness::Dispatcher)

      ready = capture.groups[jobs_fair_group]
      expect(ready).not_to include(ingest)
    end

    it "lists control and dispatch groups separately from execution groups" do
      groups = KafkaBatch.consumer_groups

      expect(groups).to include("#{cg}-control", "#{cg}-dispatch-time", "#{cg}-jobs-fair-time", "#{cg}-jobs")
      expect(groups).not_to include("#{cg}-control-jobs") # no merged group
    end
  end

  describe "tier 3 — execution (plain + fair ready job topics)" do
    it "registers plain job topics only in the -jobs group" do
      capture = capture_routes

      expect(capture.groups["#{cg}-jobs"]).to eq([SuccessfulWorker.kafka_topic])
      expect(capture.groups["#{cg}-jobs"]).not_to include(KafkaBatch.config.events_topic)
    end

    it "registers fair ready topics with JobConsumer (not Dispatcher)" do
      capture = capture_routes
      ready_topic = KafkaBatch.config.fairness_ready_topic(:time, :ruby)
      group = "#{cg}-jobs-fair-time"

      expect(capture.groups[group]).to eq([ready_topic])
      expect(capture.consumers[[group, ready_topic]])
        .to eq(KafkaBatch::Consumers::JobConsumer)
    end

    it "can be deployed without the control group via Karafka include-consumer-groups" do
      execution_only = ["#{cg}-jobs", "#{cg}-jobs-fair-time"]
      control_only   = ["#{cg}-control", "#{cg}-dispatch-time"]

      expect(execution_only & control_only).to be_empty
      expect(KafkaBatch.consumer_groups).to include(*execution_only, *control_only)
    end
  end

  describe "consumer group name helpers (config-only, no worker boot)" do
    it "derives group names from config for deployment manifests" do
      expect(KafkaBatch.control_consumer_group).to eq("#{cg}-control")
      expect(KafkaBatch.jobs_consumer_group).to eq("#{cg}-jobs")
      expect(KafkaBatch.dispatch_consumer_group(:time)).to eq("#{cg}-dispatch-time")
      expect(KafkaBatch.jobs_fair_consumer_group(:time)).to eq("#{cg}-jobs-fair-time")
    end
  end

  describe "schedule poller (control-adjacent)" do
    it "does not start when schedule_poller_enabled is false" do
      KafkaBatch.configure { |c| c.schedule_poller_enabled = false }
      KafkaBatch::SchedulePoller.ensure_running!
      expect(KafkaBatch::SchedulePoller.running?).to be(false)
    end

    it "starts only when explicitly enabled (typical scheduler role)" do
      KafkaBatch.configure { |c| c.schedule_poller_enabled = true }
      KafkaBatch::SchedulePoller.ensure_running!
      wait_for { KafkaBatch::SchedulePoller.running? }
      expect(KafkaBatch::SchedulePoller.running?).to be(true)
    ensure
      KafkaBatch::SchedulePoller.stop!
    end
  end

  def wait_for(timeout: 2.0, interval: 0.01)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      return if yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep interval
    end
  end
end
