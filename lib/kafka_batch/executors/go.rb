# frozen_string_literal: true

require "json"
require "socket"

module KafkaBatch
  module Executors
    # Invokes a Go handler via the kbatch sidecar (HTTP over a Unix socket).
    class Go
      EXECUTE_PATH = "/v1/execute"

      def call(context)
        body = build_request_body(context)
        response = post_execute(body)
        return if response["ok"]

        error_class = response["error_class"].to_s
        error_class = "GoExecutionError" if error_class.empty?
        message = response["error_message"].to_s
        message = "Go handler failed" if message.empty?

        raise GoExecutionError.new(message, error_class: error_class)
      end

      private

      def build_request_body(context)
        data = context.data
        {
          "job_type"    => context.job_type,
          "job_id"      => data["job_id"],
          "batch_id"    => data["batch_id"],
          "attempt"     => data["attempt"].to_i,
          "payload"     => data["payload"] || {},
          "tenant_id"   => data["tenant_id"],
          "enqueued_at" => data["enqueued_at"]
        }.compact
      end

      def post_execute(body)
        socket_path = KafkaBatch.config.go_executor_socket.to_s.strip
        raise ConfigurationError, "config.go_executor_socket is not set" if socket_path.empty?

        status, response_body = unix_post_json(socket_path, EXECUTE_PATH, body)
        unless status.to_i.between?(200, 299)
          raise GoExecutionError.new(
            "kbatch sidecar returned HTTP #{status}: #{response_body}",
            error_class: "GoSidecarError"
          )
        end

        Oj.load(response_body, mode: :compat)
      rescue Errno::ECONNREFUSED, Errno::ENOENT, Errno::ECONNRESET => e
        raise GoExecutionError.new(
          "kbatch sidecar unavailable at #{socket_path}: #{e.message}",
          error_class: "GoSidecarUnavailable"
        )
      end

      def unix_post_json(socket_path, path, body)
        json = Oj.dump(body, mode: :compat)
        UNIXSocket.open(socket_path) do |sock|
          sock.write "POST #{path} HTTP/1.1\r\n"
          sock.write "Host: localhost\r\n"
          sock.write "Content-Type: application/json\r\n"
          sock.write "Content-Length: #{json.bytesize}\r\n"
          sock.write "Connection: close\r\n\r\n"
          sock.write json
          read_http_response(sock)
        end
      end

      def read_http_response(sock)
        header = sock.readpartial(4096)
        while header !~ /\r\n\r\n/m
          header << sock.readpartial(4096)
        end
        headers, rest = header.split("\r\n\r\n", 2)
        status_line = headers.lines.first.to_s
        status = status_line.split[1]
        length = headers.lines.map(&:strip).grep(/\AContent-Length:/i).first&.split(":", 2)&.last&.strip.to_i
        body = rest.to_s
        while length.positive? && body.bytesize < length
          body << sock.readpartial(length - body.bytesize)
        end
        body = body.byteslice(0, length) if length.positive?
        [status, body]
      end
    end
  end
end
