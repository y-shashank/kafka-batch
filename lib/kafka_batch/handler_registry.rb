# frozen_string_literal: true

module KafkaBatch
# Maps stable job_type identifiers to execution handlers (:ruby in-process, :go sidecar).
  class HandlerRegistry
    class UnknownHandler < Error; end

    Handler = Struct.new(:job_type, :runtime, :worker_class, :executor, :definition, keyword_init: true) do
      def worker_class_name
        definition&.worker_class_name || worker_class&.name.to_s
      end
    end

    @mutex           = Mutex.new
    @by_job_type     = {}
    @by_worker_class = {}

    class << self
      def register_ruby(worker_class)
        unless worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)
          raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker"
        end

        runtime = worker_class.executor
        if runtime == :go
          return register_go(worker_class)
        end
        unless runtime == :ruby
          raise ArgumentError, "unsupported executor #{runtime.inspect} for #{worker_class}"
        end

        register_definition(HandlerDefinition.from_worker(worker_class))
      end

      def register_go(worker_class = nil, definition: nil)
        definition ||= HandlerDefinition.from_worker(worker_class)
        unless definition.runtime == :go
          raise ArgumentError, "register_go requires runtime :go (got #{definition.runtime.inspect})"
        end

        register_definition(definition, executor: Executors::Go.new)
      end

      def register_definition(definition, executor: nil)
        job_type = definition.job_type
        worker_class = definition.worker_class
        runtime = definition.runtime

        exec =
          case runtime
          when :ruby
            raise ArgumentError, "ruby handler missing worker_class for #{job_type}" unless worker_class
            executor || Executors::Ruby.new(worker_class)
          when :go
            executor || Executors::Go.new
          else
            raise ArgumentError, "unsupported runtime #{runtime.inspect} for #{job_type}"
          end

        handler = Handler.new(
          job_type:      job_type,
          runtime:       runtime,
          worker_class:  worker_class,
          executor:      exec,
          definition:    definition
        )

        @mutex.synchronize do
          existing = @by_job_type[job_type]
          if existing
            same_worker = existing.worker_class == worker_class
            same_worker ||= worker_class.nil? && existing.definition&.worker_class_name == definition.worker_class_name
            unless same_worker
              raise ArgumentError,
                    "job_type #{job_type.inspect} already registered to #{existing.worker_class || existing.definition&.worker_class_name}"
            end
          end

          @by_job_type[job_type] = handler
          if worker_class&.name && !worker_class.name.to_s.empty?
            @by_worker_class[worker_class.name] = handler
          end
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

      def definition!(job_type)
        handler = @mutex.synchronize { @by_job_type[job_type.to_s] }
        raise UnknownHandler, "Unknown job_type: #{job_type}" unless handler

        handler.definition
      end

      def lookup_by_job_type(job_type)
        @mutex.synchronize { @by_job_type[job_type.to_s] }
      end
      private :lookup_by_job_type

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
