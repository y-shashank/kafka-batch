# frozen_string_literal: true

require "socket"

RSpec.describe KafkaBatch::WorkerServer do
  let(:socket_path) { File.join(Dir.tmpdir, "kb-worker-server-#{SecureRandom.hex(4)}.sock") }

  before(:each) do
    KafkaBatchSpec::WorkerRuns.reset!
    KafkaBatch::HandlerRegistry.register_ruby(SuccessfulWorker)
  end
  after(:each) { described_class.stop! }

  it "runs a registered Ruby worker via POST /v1/execute" do
    described_class.start!(socket_path: socket_path)

    body = Oj.dump(
      "job_type" => SuccessfulWorker.job_type,
      "job_id" => SecureRandom.uuid,
      "attempt" => 0,
      "payload" => { "id" => 1 }
    )
    status, response = unix_post(socket_path, "/v1/execute", body)

    expect(status).to eq("200")
    expect(Oj.load(response)).to eq("ok" => true)
    expect(KafkaBatchSpec::WorkerRuns.runs.last).to include(name: :success, payload: { "id" => 1 })
  end

  it "returns error JSON for unknown handlers" do
    described_class.start!(socket_path: socket_path)

    body = Oj.dump("job_type" => "missing.handler", "job_id" => "j1", "attempt" => 0, "payload" => {})
    status, response = unix_post(socket_path, "/v1/execute", body)
    parsed = Oj.load(response)

    expect(status).to eq("422")
    expect(parsed["ok"]).to eq(false)
    expect(parsed["error_class"]).to eq("UnknownHandler")
  end

  def unix_post(socket_path, path, body)
    status = nil
    response = +""
    UNIXSocket.open(socket_path) do |sock|
      sock.write "POST #{path} HTTP/1.1\r\n"
      sock.write "Host: localhost\r\n"
      sock.write "Content-Type: application/json\r\n"
      sock.write "Content-Length: #{body.bytesize}\r\n"
      sock.write "Connection: close\r\n\r\n"
      sock.write body
      header = sock.readpartial(4096)
      while header !~ /\r\n\r\n/m
        header << sock.readpartial(4096)
      end
      headers, rest = header.split("\r\n\r\n", 2)
      status = headers.lines.first.split[1]
      length = headers.lines.map(&:strip).grep(/\AContent-Length:/i).first&.split(":", 2)&.last&.strip.to_i
      response << rest.to_s
      while length.positive? && response.bytesize < length
        response << sock.readpartial(length - response.bytesize)
      end
    end
    [status, response]
  end
end
