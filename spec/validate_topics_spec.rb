RSpec.describe "KafkaBatch.validate_topics!" do
  # Stub the rdkafka metadata path so we control which topics "exist".
  def stub_cluster_topics(names)
    md       = double("metadata", topics: names.map { |n| double(topic: n) })
    client   = double("rdkafka", metadata: md)
    producer = double("producer", client: client)
    allow(KafkaBatch::Producer).to receive(:instance).and_return(producer)
  end

  before { allow(KafkaBatch).to receive(:validate_fairness_partitions!) }

  it "derives required topics from Topics.specs (not config.jobs_topic)" do
    allow(KafkaBatch::Topics).to receive(:specs)
      .and_return([{ name: "app.events" }, { name: "app.dlt" }])
    stub_cluster_topics(%w[app.events app.dlt extra.topic])

    expect { KafkaBatch.validate_topics! }.not_to raise_error
  end

  it "raises listing the missing topics" do
    allow(KafkaBatch::Topics).to receive(:specs)
      .and_return([{ name: "app.events" }, { name: "app.missing" }])
    stub_cluster_topics(%w[app.events])

    expect { KafkaBatch.validate_topics! }
      .to raise_error(KafkaBatch::ConfigurationError, /app\.missing/)
  end

  it "does not require config.jobs_topic when worker topics are registered" do
    allow(KafkaBatch).to receive(:workers).and_return([SuccessfulWorker])
    # Cluster has the worker's topic + control plane + priority topics (always
    # provisioned by topics.rb), but NOT config.jobs_topic.
    existing = [SuccessfulWorker.kafka_topic,
                KafkaBatch.config.events_topic,
                KafkaBatch.config.callbacks_topic,
                KafkaBatch.config.dead_letter_topic,
                *KafkaBatch.config.retry_topics,
                KafkaBatch.config.fast_p0_topic,
                KafkaBatch.config.fast_p1_topic,
                KafkaBatch.config.slow_p0_topic,
                KafkaBatch.config.slow_p1_topic]
    stub_cluster_topics(existing)

    expect { KafkaBatch.validate_topics! }.not_to raise_error
  end
end
