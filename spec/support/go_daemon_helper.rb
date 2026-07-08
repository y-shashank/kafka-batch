# frozen_string_literal: true

module KafkaBatchSpec
  # Kafka poll helpers for Go daemon integration specs.
  module GoDaemonHelper
    class << self
      def poll_topic(brokers:, topic:, group_suffix:, timeout: 30, match: nil)
        require "rdkafka"
        cfg = Rdkafka::Config.new(
          :"bootstrap.servers"  => brokers,
          :"group.id"           => "kb-daemon-poll-#{group_suffix}-#{SecureRandom.hex(4)}",
          :"auto.offset.reset"  => "earliest",
          :"enable.auto.commit" => false
        )
        consumer = cfg.consumer
        consumer.subscribe(topic)

        deadline = Time.now + timeout
        while Time.now < deadline
          raw = consumer.poll(1_000)
          next unless raw

          decoded = Oj.load(raw.payload)
          return decoded if match.nil? || match.call(decoded)
        end
        nil
      ensure
        consumer&.close
      end
    end
  end
end
