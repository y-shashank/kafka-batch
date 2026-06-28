module KafkaBatch
  # Kafka consumer-group lag (a.k.a. pending messages) per topic/partition.
  #
  # Backed by Karafka::Admin, which reads committed offsets and high-watermarks
  # straight from the cluster. Read-only and best-effort: if Karafka/Admin isn't
  # loaded (e.g. a pure web process) or the cluster can't be reached, the methods
  # degrade gracefully so the dashboard never breaks.
  #
  # "Lag" == messages produced to a partition that the consumer group hasn't
  # committed yet == pending work for that topic/partition.
  module Lag
    module_function

    def available?
      defined?(Karafka) && defined?(Karafka::Admin) && defined?(Karafka::App) &&
        Karafka::Admin.respond_to?(:read_lags_with_offsets)
    end

    # Per-partition lag/offset rows across all routed consumer groups.
    #
    # @return [Array<Hash>] each: {
    #   group:, topic:, partition:, committed:, end_offset:, lag:, never_consumed:
    # } sorted by group, topic, partition. `committed`/`end_offset` are nil for a
    # partition the group has never committed to (reported by Kafka as -1).
    def partitions
      return [] unless available?

      rows = []
      read.each do |group, topics|
        topics.each do |topic, parts|
          parts.each do |partition, info|
            committed = info[:offset].to_i
            lag       = info[:lag].to_i
            consumed  = committed >= 0
            rows << {
              group:          group.to_s,
              topic:          topic.to_s,
              partition:      partition.to_i,
              committed:      consumed ? committed : nil,
              lag:            lag.negative? ? 0 : lag,
              end_offset:     (consumed && !lag.negative?) ? committed + lag : nil,
              never_consumed: !consumed
            }
          end
        end
      end
      rows.sort_by { |r| [r[:group], r[:topic], r[:partition]] }
    end

    # Per-topic aggregation: total pending + partition count per (group, topic).
    # @return [Array<Hash>] each: { group:, topic:, partitions:, lag: }
    def topics(rows = partitions)
      rows
        .group_by { |r| [r[:group], r[:topic]] }
        .map do |(group, topic), prs|
          { group: group, topic: topic, partitions: prs.size, lag: prs.sum { |r| r[:lag] } }
        end
        .sort_by { |r| [r[:group], r[:topic]] }
    end

    # Total pending messages across everything.
    def total(rows = partitions)
      rows.sum { |r| r[:lag] }
    end

    # Read committed lag for a specific consumer group + topics.
    # @return [Hash] { group => { topic => { partition => { offset:, lag: } } } }
    def read_group(group, topics)
      return {} unless available?

      Karafka::Admin.read_lags_with_offsets({ group => topics })
    end

    # @api private
    # Read lags ONLY for this gem's consumer groups (control + jobs), so the
    # dashboard never reports on the host app's unrelated topics. Returns {} if
    # the gem's groups aren't present in the routing (nothing to show) rather
    # than falling back to Karafka's "all groups" behaviour.
    def read
      groups = gem_groups_with_topics
      return {} if groups.empty?

      Karafka::Admin.read_lags_with_offsets(groups)
    end

    # @api private
    # @return [Hash<String, Array<String>>] { gem_consumer_group => [topics] }
    def gem_groups_with_topics
      base   = KafkaBatch.config.consumer_group
      wanted = ["#{base}-control", "#{base}-jobs"]

      Karafka::App.routes
                  .select { |cg| wanted.include?(cg.id) }
                  .each_with_object({}) { |cg, h| h[cg.id] = cg.topics.map(&:name) }
    end
  end
end
