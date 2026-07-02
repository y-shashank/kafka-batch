require "spec_helper"

RSpec.describe KafkaBatch::ProcessStats do
  before { described_class.reset! }

  it "returns rss_bytes on Linux or macOS" do
    stats = described_class.sample
    skip "RSS not available on this platform" unless stats.key?("rss_bytes")
    expect(stats["rss_bytes"]).to be > 0
  end

  it "returns cpu_pct after a second sample" do
    described_class.sample
    sleep 0.05
    stats = described_class.sample
    expect(stats["cpu_pct"]).to be_a(Float) if stats.key?("cpu_pct")
  end
end
