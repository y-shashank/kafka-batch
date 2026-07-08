# frozen_string_literal: true

require "json"
require "socket"

module KafkaBatchSpec
  # Minimal Unix-socket HTTP server for Go daemon RubySocketInvoker integration tests.
  class RubyCallbackServer
    attr_reader :socket_path

    def initialize(socket_path:)
      @socket_path = socket_path
      @server = nil
      @thread = nil
    end

    def start!
      File.unlink(@socket_path) if File.exist?(@socket_path)
      @server = UNIXServer.new(@socket_path)
      @thread = Thread.new { accept_loop }
      wait_until_listening!
    end

    def stop!
      @server&.close
      @thread&.join(2)
      File.unlink(@socket_path) if File.exist?(@socket_path)
    rescue StandardError
      nil
    end

    private

    def wait_until_listening!
      deadline = Time.now + 5
      until File.socket?(@socket_path) || Time.now >= deadline
        sleep 0.05
      end
      raise "callback server did not bind #{@socket_path}" unless File.socket?(@socket_path)
    end

    def accept_loop
      loop do
        client = @server.accept
        Thread.new(client) { |c| handle_client(c) }
      end
    rescue IOError, Errno::EBADF
      nil
    end

    def handle_client(client)
      headers = {}
      request_line = client.gets
      return unless request_line

      while (line = client.gets) && line != "\r\n"
        name, value = line.split(":", 2).map(&:strip)
        headers[name.downcase] = value if name && value
      end

      length = headers["content-length"].to_i
      body = length.positive? ? client.read(length) : ""
      req = JSON.parse(body)

      klass = Object.const_get(req.fetch("class_name"))
      unless klass.method_defined?(req.fetch("method_name").to_sym)
        respond(client, 422, "missing method")
        return
      end
      klass.new.public_send(req.fetch("method_name"), req.fetch("summary"))
      respond(client, 200, "ok")
    rescue NameError => e
      respond(client, 422, e.message)
    rescue StandardError => e
      respond(client, 500, e.message)
    ensure
      client.close
    end

    def respond(client, status, body)
      client.print "HTTP/1.1 #{status} OK\r\nContent-Length: #{body.bytesize}\r\n\r\n#{body}"
    end
  end
end
