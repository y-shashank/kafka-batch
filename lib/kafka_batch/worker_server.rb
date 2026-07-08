# frozen_string_literal: true

require "json"
require "socket"

module KafkaBatch
  # Unix-socket HTTP server that runs Ruby Worker#perform for the Go daemon (Phase 4).
  #
  # Protocol: same as kbatch serve — POST /v1/execute, GET /health.
  # See protocol/README.md and protocol/execute_request.json.
  class WorkerServer
    EXECUTE_PATH = "/v1/execute"
    MAX_BODY     = 8 * 1024 * 1024

    class << self
      def run!(socket_path: default_socket_path)
        new(socket_path: socket_path).run!
      end

      def start!(socket_path: default_socket_path)
        instance = new(socket_path: socket_path)
        instance.start!
        @instance = instance
      end

      def stop!
        @instance&.stop!
        @instance = nil
      end

      def default_socket_path
        path = ENV["KAFKA_BATCH_WORKER_SOCKET"].to_s.strip
        return path unless path.empty?

        KafkaBatch.config.ruby_worker_socket.to_s.strip
      end
    end

    attr_reader :socket_path

    def initialize(socket_path:)
      path = socket_path.to_s.strip
      raise ConfigurationError, "worker server socket path is required" if path.empty?

      @socket_path = path
      @server      = nil
      @thread      = nil
      @running     = false
      @max_inflight = Integer(ENV.fetch("KAFKA_BATCH_WORKER_MAX_THREADS", "64"))
      @inflight     = 0
      @inflight_mu  = Mutex.new
    end

    def run!
      start!
      @thread&.join
    end

    def start!
      return if @running

      File.unlink(@socket_path) if File.exist?(@socket_path)
      @server  = UNIXServer.new(@socket_path)
      @running = true
      @thread  = Thread.new { accept_loop }
      wait_until_listening!
      KafkaBatch.logger.info("[KafkaBatch::WorkerServer] listening on #{@socket_path}")
    end

    def stop!
      return unless @running

      @running = false
      @server&.close
      @thread&.join(5)
      File.unlink(@socket_path) if File.exist?(@socket_path)
    rescue StandardError
      nil
    end

    private

    def wait_until_listening!
      deadline = Time.now + 10
      until File.socket?(@socket_path) || Time.now >= deadline
        sleep 0.05
      end
      return if File.socket?(@socket_path)

      raise "WorkerServer did not bind #{@socket_path}"
    end

    def accept_loop
      loop do
        break unless @running

        client = @server.accept
        unless acquire_slot
          write_json(client, 503, "ok" => false, "error_class" => "Overloaded", "error_message" => "worker server at capacity")
          client.close
          next
        end
        Thread.new(client) do |c|
          handle_client(c)
        ensure
          release_slot
        end
      end
    rescue IOError, Errno::EBADF, Errno::ENOTSOCK
      nil
    end

    def acquire_slot
      @inflight_mu.synchronize do
        return false if @inflight >= @max_inflight

        @inflight += 1
        true
      end
    end

    def release_slot
      @inflight_mu.synchronize { @inflight -= 1 if @inflight.positive? }
    end

    def handle_client(client)
      method, path, _version = parse_request_line(client.gets)
      headers = read_headers(client)
      body    = read_body(client, headers["content-length"].to_i)

      case [method, path]
      when ["GET", "/health"]
        write_json(client, 200, "ok" => true)
      when ["POST", EXECUTE_PATH]
        write_json(client, *execute(body))
      else
        write_json(client, 404, "ok" => false, "error_class" => "NotFound", "error_message" => "not found")
      end
    rescue StandardError => e
      KafkaBatch.logger.error("[KafkaBatch::WorkerServer] #{e.class}: #{e.message}")
      write_json(client, 500, "ok" => false, "error_class" => e.class.name, "error_message" => e.message)
    ensure
      client.close
    end

    def parse_request_line(line)
      return ["", "", ""] unless line

      parts = line.strip.split
      [parts[0], parts[1], parts[2]]
    end

    def read_headers(client)
      headers = {}
      while (line = client.gets) && line != "\r\n"
        name, value = line.split(":", 2).map(&:strip)
        headers[name.downcase] = value if name && value
      end
      headers
    end

    def read_body(client, length)
      return "" unless length.positive?

      length = MAX_BODY if length > MAX_BODY
      body   = +""
      while body.bytesize < length
        chunk = client.read(length - body.bytesize)
        break unless chunk

        body << chunk
      end
      body
    end

    def execute(body)
      req = JSON.parse(body)
      data = build_job_data(req)
      handler = HandlerRegistry.resolve!(data)
      unless handler.runtime == :ruby
        return [422, error_response("NotRubyHandler", "handler runtime is #{handler.runtime}")]
      end

      context = ExecutionContext.new(data: data, message: nil, handler: handler)
      handler.executor.call(context)
      [200, { "ok" => true }]
    rescue HandlerRegistry::UnknownHandler => e
      [422, error_response("UnknownHandler", e.message)]
    rescue JSON::ParserError => e
      [400, error_response("JSON::ParserError", e.message)]
    rescue StandardError => e
      [422, error_response(e.class.name, e.message)]
    end

    def build_job_data(req)
      data = {
        "job_id"        => req["job_id"],
        "job_type"      => req["job_type"],
        "batch_id"      => req["batch_id"],
        "worker_class"  => req["worker_class"],
        "payload"       => req["payload"] || {},
        "attempt"       => req.fetch("attempt", 0).to_i,
        "tenant_id"     => req["tenant_id"],
        "enqueued_at"   => req["enqueued_at"],
        "batch_seq"     => req["batch_seq"],
        "max_retries"   => req["max_retries"],
        "retry_tier"    => req["retry_tier"],
        "valid_till"    => req["valid_till"],
        "_uniq_fp"      => req["_uniq_fp"]
      }
      data.compact!
      data
    end

    def error_response(error_class, error_message)
      {
        "ok"            => false,
        "error_class"   => error_class,
        "error_message" => error_message
      }
    end

    def write_json(client, status, payload)
      body = Oj.dump(payload, mode: :compat)
      client.print "HTTP/1.1 #{status} #{http_reason(status)}\r\n"
      client.print "Content-Type: application/json\r\n"
      client.print "Content-Length: #{body.bytesize}\r\n"
      client.print "Connection: close\r\n\r\n"
      client.print body
    end

    def http_reason(status)
      case status
      when 200 then "OK"
      when 400 then "Bad Request"
      when 404 then "Not Found"
      when 422 then "Unprocessable Entity"
      when 500 then "Internal Server Error"
      when 503 then "Service Unavailable"
      else "OK"
      end
    end
  end
end
