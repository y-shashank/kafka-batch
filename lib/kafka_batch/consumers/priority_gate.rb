module KafkaBatch
  module Consumers
    # Mixin for p1 consumers that adds a rate-limited lag check against the
    # corresponding p0 topic.  The check calls the Karafka Admin API at most
    # once per +priority_lag_check_interval+ seconds per consumer instance so
    # the cluster is not hammered on every consume call.
    #
    # On any error the check FAILS OPEN — the consumer processes its messages
    # rather than blocking indefinitely on an unreachable cluster.
    module PriorityGate
      # Returns true when the given p0 topic has any un-consumed lag in the
      # given consumer group.  Result is cached for priority_lag_check_interval
      # seconds (monotonic clock, per instance).
      #
      # @param p0_topic [String]
      # @param consumer_group [String]
      # @return [Boolean]
      def p0_has_lag?(p0_topic, consumer_group)
        now      = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        interval = KafkaBatch.config.priority_lag_check_interval.to_f

        if @priority_last_check && (now - @priority_last_check) < interval
          return @priority_last_result
        end

        # Cache the topic/group for yield_to_p0 instrumentation
        @priority_p0_topic      = p0_topic
        @priority_consumer_group = consumer_group

        @priority_last_check  = now
        @priority_last_result =
          begin
            data       = KafkaBatch::Lag.read_group(consumer_group, [p0_topic])
            partitions = (data[consumer_group] || {})[p0_topic] || {}
            partitions.values.any? { |info| info[:lag].to_i > 0 }
          rescue StandardError => e
            KafkaBatch.logger.debug(
              "[KafkaBatch][PriorityGate] lag check for #{p0_topic} failed – " \
              "failing open: #{e.message}"
            )
            false  # fail open: process the job rather than block indefinitely
          end
      end

      # Pause this partition for one lag-check interval so the p0 consumer
      # gets CPU time.  Messages remain uncommitted and will be redelivered.
      def yield_to_p0
        pause_ms = (KafkaBatch.config.priority_lag_check_interval * 1_000).to_i
        KafkaBatch.logger.debug(
          "[KafkaBatch][PriorityGate] #{self.class.name} p0 has lag – " \
          "pausing partition for #{pause_ms}ms"
        )
        KafkaBatch::Instrumentation.consumer_priority_yielded(
          consumer_class: self.class.name,
          p0_topic:       @priority_p0_topic,
          consumer_group: @priority_consumer_group,
          pause_ms:       pause_ms
        )
        pause(pause_ms)
      end
    end
  end
end
