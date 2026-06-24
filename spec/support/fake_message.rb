require "oj"

# Minimal stand-in for a Karafka message. Only the attributes the consumers
# actually read (raw_payload, topic, offset) are implemented.
class FakeMessage
  attr_reader :raw_payload, :topic, :offset, :partition

  def initialize(payload:, topic: "test.topic", offset: 0, partition: 0)
    @raw_payload = payload.is_a?(String) ? payload : Oj.dump(payload, mode: :compat)
    @topic       = topic
    @offset      = offset
    @partition   = partition
  end
end
