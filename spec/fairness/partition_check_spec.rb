RSpec.describe "KafkaBatch.validate_fairness_partitions!" do
  before do
    KafkaBatch.config.fairness_enabled              = true
    KafkaBatch.config.fairness_min_ingest_partitions = 4
  end

  it "is a no-op when fairness is disabled (even with too few partitions)" do
    KafkaBatch.config.fairness_enabled = false
    allow(KafkaBatch).to receive(:fairness_ingest_partition_count).and_return(1)
    expect { KafkaBatch.validate_fairness_partitions!(strict: true) }.not_to raise_error
  end

  it "is a no-op when the partition count can't be determined" do
    allow(KafkaBatch).to receive(:fairness_ingest_partition_count).and_return(nil)
    expect { KafkaBatch.validate_fairness_partitions!(strict: true) }.not_to raise_error
  end

  it "raises (strict) when partitions are below the minimum" do
    allow(KafkaBatch).to receive(:fairness_ingest_partition_count).and_return(1)
    expect { KafkaBatch.validate_fairness_partitions!(strict: true) }
      .to raise_error(KafkaBatch::ConfigurationError, /partition/)
  end

  it "warns (no raise) when not strict and partitions are below the minimum" do
    allow(KafkaBatch).to receive(:fairness_ingest_partition_count).and_return(2)
    expect(KafkaBatch.logger).to receive(:warn).with(/partition/)
    expect { KafkaBatch.validate_fairness_partitions!(strict: false) }.not_to raise_error
  end

  it "always treats a single-partition topic as insufficient regardless of a low min" do
    KafkaBatch.config.fairness_min_ingest_partitions = 1
    allow(KafkaBatch).to receive(:fairness_ingest_partition_count).and_return(1)
    expect { KafkaBatch.validate_fairness_partitions!(strict: true) }
      .to raise_error(KafkaBatch::ConfigurationError)
  end

  it "passes when partitions meet the minimum" do
    allow(KafkaBatch).to receive(:fairness_ingest_partition_count).and_return(8)
    expect { KafkaBatch.validate_fairness_partitions!(strict: true) }.not_to raise_error
  end
end
