if ENV["COVERAGE"]
  require "simplecov"
  SimpleCov.start do
    add_filter "/spec/"
    track_files "lib/**/*.rb"
    enable_coverage :branch
  end
end

require "logger"
require "active_record"

# Quiet logging during the suite unless DEBUG is set.
ENV["KAFKA_BATCH_TEST_LOG"] ||= File::NULL

require "kafka_batch"

require_relative "support/active_record"
require_relative "support/fake_producer"
require_relative "support/fake_message"
require_relative "support/callback_doubles"
require_relative "support/redis_helper"
require_relative "support/test_workers"

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :expect }
  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand(config.seed)

  config.before(:each) do
    KafkaBatch.reset!
    KafkaBatch.configure do |c|
      c.brokers = ["localhost:9092"]
      c.logger  = Logger.new(ENV["KAFKA_BATCH_TEST_LOG"])
      # Liveness writes to Redis; keep it off for the bulk of specs and enable
      # explicitly in the liveness/web specs that exercise it.
      c.track_running_jobs = false
    end

    # Every spec gets a clean SQLite schema + a recording producer by default.
    KafkaBatchSpec::ActiveRecordSupport.truncate!
    FakeProducer.reset!
    allow(KafkaBatch::Producer).to receive(:produce_sync) do |topic:, payload:, key: nil, headers: {}|
      FakeProducer.record(topic: topic, payload: payload, key: key, headers: headers)
    end

    KafkaBatchSpec::CallbackDoubles.reset!
    KafkaBatchSpec::WorkerRuns.reset!
    allow(KafkaBatch::ConsumptionControl).to receive(:available?).and_return(false)
  end
end

# Build a Karafka consumer instance without booting Karafka. We allocate the
# object (skipping #initialize) and stub the runtime hooks the gem calls.
def build_consumer(klass)
  consumer = klass.allocate
  allow(consumer).to receive(:mark_as_consumed!)
  allow(consumer).to receive(:pause)
  consumer
end
