# frozen_string_literal: true

RSpec.describe "KafkaBatch.draw_routes daemon_mode" do
  it "skips Karafka consumers when daemon_mode is enabled" do
    KafkaBatch.configure { |c| c.daemon_mode = true }
    builder = double("karafka_routes")
    expect(builder).not_to receive(:instance_eval)
    expect(KafkaBatch.logger).to receive(:warn).with(/daemon_mode enabled/)

    KafkaBatch.draw_routes(builder)
  end
end
