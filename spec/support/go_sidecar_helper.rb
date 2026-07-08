# frozen_string_literal: true

require "socket"

module KafkaBatchSpec
  # Spawns the Go integration sidecar (kbatch-ittest) for real-broker specs.
  module GoSidecarHelper
    GO_JOB_TYPE = "integration.go_echo"

    class << self
      def available?
        return @available unless @available.nil?

        bin = itest_binary
        @available = File.executable?(bin) || system("which go >/dev/null 2>&1")
      end

      def itest_binary
        ENV.fetch("KBATCH_ITEST_BIN") do
          File.expand_path("../../bin/kbatch-ittest", __dir__)
        end
      end

      def go_module_root
        File.expand_path("../../go", __dir__)
      end

      # @return [Hash] :socket_path, :marker_path, :pid
      def start!(marker_path:)
        socket_path = File.join(Dir.tmpdir, "kbatch-ittest-#{Process.pid}-#{SecureRandom.hex(4)}.sock")
        File.delete(socket_path) if File.exist?(socket_path)

        env = ENV.to_h.merge("KBATCH_ITEST_MARKER" => marker_path)
        pid =
          if File.executable?(itest_binary)
            Process.spawn(env, itest_binary, "serve", "--socket", socket_path,
                          out: File::NULL, err: File::NULL)
          else
            Process.spawn(env, "go", "run", "./cmd/kbatch-ittest", "serve", "--socket", socket_path,
                          chdir: go_module_root, out: File::NULL, err: File::NULL)
          end

        wait_for_socket!(socket_path)
        { socket_path: socket_path, marker_path: marker_path, pid: pid }
      end

      def stop!(pid:, socket_path: nil)
        return unless pid

        Process.kill("TERM", pid)
        Timeout.timeout(5) { Process.wait(pid) }
      rescue Errno::ESRCH, Timeout::Error
        Process.kill("KILL", pid) rescue nil
      ensure
        File.delete(socket_path) if socket_path && File.exist?(socket_path)
      end

      def wait_for_socket!(path, timeout: 15)
        deadline = Time.now + timeout
        while Time.now < deadline
          begin
            UNIXSocket.new(path).close
            return true
          rescue Errno::ENOENT, Errno::ECONNREFUSED
            sleep 0.05
          end
        end
        raise "Go sidecar socket not ready: #{path}"
      end
    end
  end
end
