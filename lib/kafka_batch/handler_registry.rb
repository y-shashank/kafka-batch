# frozen_string_literal: true

module KafkaBatch
  # Maps stable job_type identifiers to execution handlers.
  # Phase 1: Ruby workers only; future phases add :go executors for hybrid hosts.
  class HandlerRegistry
    class UnknownHandler < Error; end

    Handler = Struct.new(:job_type, :runtime, :worker_class, :executor, keyword_init: true)

    @mutex           = Mutex.new
    @by_job_type     = {}
    @by_worker_class = {}

    class << self
      def register_ruby(worker_class)
        unless worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
          raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
        end

        runtime = worker_class.executor
        unless runtime == :ruby
          raise ArgumentError, "unsupported executor #{runtime.inspect} for #{worker_class} (Phase 1: :ruby only)"
        end

        job_type = worker_class.job_type
        handler  = Handler.new(
          job_type:      job_type,
          runtime:       :ruby,
          worker_class:  worker_class,
          executor:      Executors::Ruby.new(worker_class)
        )

        @mutex.synchronize do
          existing = @by_job_type[job_type]
          if existing && existing.worker_class != worker_class
            raise ArgumentError,
                  "job_type #{job_type.inspect} already registered to #{existing.worker_class}"
          end

          @by_job_type[job_type]             = handler
          @by_worker_class[worker_class.name] = handler
        end

        handler
      end

      # @return [Handler]
      # @raise [UnknownHandler]
      def resolve!(data)
        job_type    = data["job_type"]
        worker_name = data["worker_class"]

        if job_type && !job_type.to_s.empty?
          handler = @mutex.synchronize { @by_job_type[job_type.to_s] }
          return handler if handler
        end

        if worker_name && !worker_name.to_s.empty?
          handler = resolve_by_worker_class!(worker_name.to_s)
          if job_type && !job_type.to_s.empty? && handler.job_type != job_type.to_s
            raise UnknownHandler,
                  "job_type #{job_type.inspect} does not match #{handler.job_type.inspect} " \
                  "for #{worker_name}"
          end
          return handler
        end

        raise UnknownHandler, "Unknown job_type: #{job_type}" if job_type && !job_type.to_s.empty?

        raise UnknownHandler, "Missing job_type and worker_class"
      end

      def resolve_by_worker_class!(worker_name)
        handler = @mutex.synchronize { @by_worker_class[worker_name] }
        return handler if handler

        klass = Object.const_get(worker_name)
        raise UnknownHandler, "#{worker_name} does not include KafkaBatch::Worker" \
          unless klass.include?(KafkaBatch::Worker)

        register_ruby(klass)
      rescue NameError
        raise UnknownHandler, "Unknown worker class: #{worker_name}"
      end
      private :resolve_by_worker_class!

      def registered?(job_type)
        @mutex.synchronize { @by_job_type.key?(job_type.to_s) }
      end

      def reset!
        @mutex.synchronize do
          @by_job_type     = {}
          @by_worker_class = {}
        end
      end
    end
  end
end
