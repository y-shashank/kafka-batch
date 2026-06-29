module KafkaBatch
  module Consumers
    # Prepended before #consume on gem consumers. When the Web UI marks a topic
    # or partition paused (Redis or MySQL), the consumer pauses indefinitely
    # until resume is requested. Pause state is refreshed at most every
    # config.consumption_control_refresh_interval seconds (default 60).
    module ConsumptionGate
      def consume
        return if apply_consumption_gate!

        super
      end

      private

      def apply_consumption_gate!
        return false unless KafkaBatch::ConsumptionControl.available?

        t = topic
        return false unless t&.consumer_group

        group = t.consumer_group.id
        if KafkaBatch::ConsumptionControl.paused?(
          group: group, topic: t.name, partition: partition
        )
          unless @consumption_gate_paused
            seek = messages.empty? ? :consecutive : messages.first.offset
            pause(seek, nil, true)
            @consumption_gate_paused = true
          end
          return true
        end

        if @consumption_gate_paused
          resume
          @consumption_gate_paused = false
        end

        false
      end
    end
  end
end
