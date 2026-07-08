# frozen_string_literal: true

require "socket"

RSpec.describe KafkaBatch::HandlerManifest do
  after do
    described_class.reset!
    KafkaBatch::HandlerRegistry.reset!
  end

  it "loads Go handlers from YAML and registers them" do
    path = File.expand_path("../fixtures/handlers.yml", __dir__)
    described_class.load!(path)

    handler = KafkaBatch::HandlerRegistry.resolve!("job_type" => "segment.export")
    expect(handler.runtime).to eq(:go)
    expect(handler.executor).to be_a(KafkaBatch::Executors::Go)
    expect(handler.definition.kafka_topic).to eq("segment.exports")
    expect(handler.definition.max_retries).to eq(25)
  end
end

RSpec.describe KafkaBatch::Executors::Go do
  let(:socket_path) { File.join(Dir.tmpdir, "kbatch_test_#{Process.pid}_#{SecureRandom.hex(4)}.sock") }
  let(:server) { @test_server }
  let(:server_thread) { @test_thread }

  after do
    @test_server&.close
    @test_thread&.kill
    File.delete(socket_path) if File.exist?(socket_path)
  end

  def start_sidecar
    File.delete(socket_path) if File.exist?(socket_path)
    @test_server = UNIXServer.new(socket_path)
    @test_thread = Thread.new do
      loop do
        client = @test_server.accept
        Thread.new(client) { |sock| handle_request(sock) }
      rescue IOError, Errno::EBADF
        break
      end
    end
    wait_for_socket!
    yield if block_given?
  end

  def handle_request(sock)
    request_line = sock.gets
    return unless request_line

      len = 0
      while (line = sock.gets) && line != "\r\n"
        key, val = line.split(": ", 2)
        len = val.to_i if key && key.casecmp("content-length").zero?
      end
    body = len.positive? ? sock.read(len) : ""
    parsed = body.empty? ? {} : Oj.load(body, mode: :compat)
    response =
      if parsed.empty? && !@sidecar_handler
        { "ok" => true }
      else
        @sidecar_handler.call(parsed)
      end
    json = Oj.dump(response, mode: :compat)
    sock.write(
      "HTTP/1.1 200 OK\r\n" \
      "Content-Type: application/json\r\n" \
      "Content-Length: #{json.bytesize}\r\n" \
      "\r\n" \
      "#{json}"
    )
    sock.close
  rescue StandardError
    sock.close rescue nil
  end

  def wait_for_socket!
    50.times do
      UNIXSocket.new(socket_path).close
      return
    rescue Errno::ENOENT, Errno::ECONNREFUSED
      sleep 0.01
    end
    raise "sidecar socket not ready: #{socket_path}"
  end

  it "posts the execute protocol envelope and raises on handler failure" do
    KafkaBatch.configure { |c| c.go_executor_socket = socket_path }

    @sidecar_handler = lambda do |body|
      expect(body["job_type"]).to eq("segment.export")
      expect(body["payload"]).to eq("segment_id" => 42)
      { "ok" => false, "error_class" => "segment.NotFound", "error_message" => "missing" }
    end
    start_sidecar

    handler = KafkaBatch::HandlerRegistry::Handler.new(
      job_type: "segment.export", runtime: :go, worker_class: nil,
      executor: described_class.new,
      definition: KafkaBatch::HandlerDefinition.new(job_type: "segment.export", runtime: :go)
    )
    data = {
      "job_id" => "j1", "batch_id" => nil, "attempt" => 0,
      "job_type" => "segment.export", "payload" => { "segment_id" => 42 }
    }
    context = KafkaBatch::ExecutionContext.new(
      data: data, message: FakeMessage.new(topic: "t", payload: data), handler: handler
    )

    expect {
      handler.executor.call(context)
    }.to raise_error(KafkaBatch::GoExecutionError, /missing/) do |e|
      expect(e.error_class).to eq("segment.NotFound")
    end
  end

  it "returns cleanly on ok response" do
    KafkaBatch.configure { |c| c.go_executor_socket = socket_path }

    @sidecar_handler = ->(_body) { { "ok" => true } }
    start_sidecar

    handler = KafkaBatch::HandlerRegistry::Handler.new(
      job_type: "segment.export", runtime: :go, worker_class: nil,
      executor: described_class.new,
      definition: KafkaBatch::HandlerDefinition.new(job_type: "segment.export", runtime: :go)
    )
    data = {
      "job_id" => "j1", "batch_id" => nil, "attempt" => 0,
      "job_type" => "segment.export", "payload" => {}
    }
    context = KafkaBatch::ExecutionContext.new(
      data: data, message: FakeMessage.new(topic: "t", payload: data), handler: handler
    )

    expect { handler.executor.call(context) }.not_to raise_error
  end
end

RSpec.describe KafkaBatch::Batch do
  after do
    KafkaBatch::HandlerManifest.reset!
    KafkaBatch::HandlerRegistry.reset!
  end

  it "push_job enqueues by job_type from the manifest" do
    path = File.expand_path("../fixtures/handlers.yml", __dir__)
    KafkaBatch::HandlerManifest.load!(path)

    batch = described_class.create
    job_id = batch.push_job("segment.export", { "segment_id" => 7 })
    expect(job_id).to be_a(String)

    produced = FakeProducer.for_topic("segment.exports").last
    expect(produced.payload["job_type"]).to eq("segment.export")
    expect(produced.payload["worker_class"]).to eq("go:segment.export")
    expect(produced.payload["payload"]).to eq("segment_id" => 7)
  end
end
