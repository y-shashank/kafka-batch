require "spec_helper"

RSpec.describe KafkaBatch::WeightShares do
  describe ".compute" do
    it "returns an empty array for no tenants" do
      expect(described_class.compute([])).to eq([])
    end

    it "assigns 100% to a single tenant" do
      shares = described_class.compute([{ tenant_id: "solo", weight: 1.0 }])
      expect(shares.size).to eq(1)
      expect(shares.first.share_pct).to be_within(0.01).of(100.0)
    end

    it "splits evenly when weights are equal" do
      tenants = [
        { tenant_id: "a", weight: 1.0 },
        { tenant_id: "b", weight: 1.0 }
      ]
      shares = described_class.compute(tenants)
      expect(shares.map(&:share_pct)).to all(be_within(0.01).of(50.0))
      expect(shares.sum(&:share_pct)).to be_within(0.01).of(100.0)
    end

    it "increases one tenant's share when its weight rises" do
      base = described_class.compute([
        { tenant_id: "a", weight: 1.0 },
        { tenant_id: "b", weight: 1.0 }
      ])
      boosted = described_class.compute([
        { tenant_id: "a", weight: 3.0 },
        { tenant_id: "b", weight: 1.0 }
      ])

      a_base = base.find { |s| s.tenant_id == "a" }.share_pct
      a_boost = boosted.find { |s| s.tenant_id == "a" }.share_pct
      b_boost = boosted.find { |s| s.tenant_id == "b" }.share_pct

      expect(a_boost).to be > a_base
      expect(a_boost + b_boost).to be_within(0.01).of(100.0)
      expect(a_boost).to be_within(0.01).of(75.0)
      expect(b_boost).to be_within(0.01).of(25.0)
    end

    it "sorts by share descending" do
      shares = described_class.compute([
        { tenant_id: "small", weight: 1.0 },
        { tenant_id: "large", weight: 9.0 }
      ])
      expect(shares.first.tenant_id).to eq("large")
    end
  end
end
