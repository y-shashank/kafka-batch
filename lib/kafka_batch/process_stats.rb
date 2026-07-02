module KafkaBatch
  # Best-effort RSS + CPU sampling for the current Ruby process. Used by
  # Liveness heartbeats (throttled via config.liveness_stats_interval).
  module ProcessStats
    class << self
      # @return [Hash] keys "rss_bytes" (Integer), "cpu_pct" (Float, nil on first sample)
      def sample
        rss = read_rss_bytes
        cpu = read_cpu_percent
        out = {}
        out["rss_bytes"] = rss if rss&.positive?
        out["cpu_pct"]   = cpu.round(1) if cpu
        out
      end

      def reset!
        @cpu_mutex      = nil
        @last_cpu_wall  = nil
        @last_cpu_time  = nil
      end

      private

      def read_rss_bytes
        if linux?
          line = File.foreach("/proc/self/status").find { |l| l.start_with?("VmRSS:") }
          return line.split[1].to_i * 1024 if line
        elsif darwin?
          # Throttled by Liveness — occasional `ps` is acceptable on macOS.
          kb = `ps -o rss= -p #{Process.pid}`.strip.to_i
          return kb * 1024 if kb.positive?
        end
        nil
      rescue StandardError
        nil
      end

      # Approximate % of one CPU core used since the previous #sample call.
      def read_cpu_percent
        wall = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        cpu  = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID)

        cpu_mutex.synchronize do
          prev_wall = @last_cpu_wall
          prev_cpu  = @last_cpu_time
          @last_cpu_wall = wall
          @last_cpu_time  = cpu

          return nil unless prev_wall && prev_cpu && (wall - prev_wall).positive?

          ((cpu - prev_cpu) / (wall - prev_wall)) * 100.0
        end
      rescue StandardError
        nil
      end

      def cpu_mutex
        @cpu_mutex ||= Mutex.new
      end

      def linux?
        RUBY_PLATFORM.include?("linux")
      end

      def darwin?
        RUBY_PLATFORM.include?("darwin")
      end
    end
  end
end
