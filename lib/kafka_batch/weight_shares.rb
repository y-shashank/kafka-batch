module KafkaBatch
  # Normalizes tenant weights into proportional capacity shares (always 100%).
  module WeightShares
    Share = Struct.new(:tenant_id, :weight, :share_pct, keyword_init: true)

    class << self
      # @param tenants [Array<Hash>] entries with :tenant_id and :weight
      # @return [Array<Share>] sorted by share descending, then tenant id
      def compute(tenants)
        list = Array(tenants)
        return [] if list.empty?

        total = list.sum { |t| t[:weight].to_f }
        if total <= 0
          return list.map do |t|
            Share.new(tenant_id: t[:tenant_id], weight: t[:weight], share_pct: 0.0)
          end
        end

        list.map do |t|
          Share.new(
            tenant_id: t[:tenant_id],
            weight:    t[:weight],
            share_pct: (t[:weight].to_f / total) * 100.0
          )
        end.sort_by { |s| [-s.share_pct, s.tenant_id.to_s] }
      end

      def format_pct(pct)
        format("%.1f%%", pct.to_f)
      end
    end
  end
end
