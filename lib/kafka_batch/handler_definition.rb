# frozen_string_literal: true

module KafkaBatch
  # Routing and retry metadata for a job handler — from a Ruby Worker class or a
  # manifest entry (Go-only handlers with no Worker class in the host app).
  class HandlerDefinition
    attr_reader :job_type, :runtime, :worker_class, :topic_base, :apply_topic_prefix,
                :fairness_type, :retry_tier

    def initialize(job_type:, runtime:, worker_class: nil, topic: nil, apply_topic_prefix: true,
                   max_retries: nil, complete_after_retries: nil, fairness_type: nil,
                   retry_tier: nil)
      @job_type                 = job_type.to_s
      @runtime                  = runtime.to_sym
      @worker_class             = worker_class
      @topic_base               = topic&.to_s
      @apply_topic_prefix       = apply_topic_prefix ? true : false
      @max_retries              = max_retries
      @complete_after_retries   = complete_after_retries
      @fairness_type            = fairness_type&.to_sym
      @retry_tier               = retry_tier&.to_sym
    end

    def self.from_worker(worker_class)
      raise ArgumentError, "#{worker_class} must include KafkaBatch::Worker" \
        unless worker_class.is_a?(Class) && worker_class.include?(KafkaBatch::Worker)

      topic_base = worker_class.instance_variable_get(:@kafka_topic_base)
      apply_prefix = worker_class.instance_variable_get(:@kafka_topic_apply_prefix)
      apply_prefix = true if apply_prefix.nil?

      new(
        job_type:               worker_class.job_type,
        runtime:                worker_class.executor,
        worker_class:           worker_class,
        topic:                  topic_base,
        apply_topic_prefix:     apply_prefix,
        max_retries:            worker_class.instance_variable_get(:@max_retries),
        complete_after_retries: worker_class.instance_variable_get(:@complete_after_retries),
        fairness_type:          worker_class.fairness_type,
        retry_tier:             worker_class.retry_tier
      )
    end

    def self.from_manifest_entry(job_type, entry)
      entry = entry.transform_keys(&:to_s)
      runtime = (entry["runtime"] || "go").to_sym
      fairness = entry["fairness_type"] || entry["fairness"]
      fairness = fairness.to_sym if fairness && !fairness.is_a?(Symbol)

      worker_class = resolve_manifest_worker_class(entry["worker_class"], job_type, runtime)

      new(
        job_type:               job_type,
        runtime:                runtime,
        worker_class:           worker_class,
        topic:                  entry["topic"],
        apply_topic_prefix:     entry.fetch("apply_topic_prefix", true),
        max_retries:            entry["max_retries"],
        complete_after_retries: entry["complete_after_retries"],
        fairness_type:          fairness,
        retry_tier:             entry["retry_tier"]
      )
    end

    def self.resolve_manifest_worker_class(name, job_type, runtime)
      return nil unless runtime == :ruby

      if name && !name.to_s.empty?
        return Object.const_get(name.to_s)
      end

      handler = HandlerRegistry.send(:lookup_by_job_type, job_type)
      handler&.worker_class
    rescue NameError => e
      raise ArgumentError, "worker_class #{name.inspect} not found: #{e.message}"
    end
    private_class_method :resolve_manifest_worker_class

    def kafka_topic
      if @topic_base && !@topic_base.empty?
        return @topic_base unless @apply_topic_prefix

        KafkaBatch.config.resolve_topic(@topic_base)
      else
        KafkaBatch.config.jobs_topic
      end
    end

    def fairness?
      !@fairness_type.nil?
    end

    def max_retries
      @max_retries.nil? ? KafkaBatch.config.max_retries : @max_retries.to_i
    end

    def complete_after_retries
      if @complete_after_retries.nil?
        KafkaBatch.config.complete_after_retries
      else
        @complete_after_retries.to_i
      end
    end

    # Wire +worker_class+ field for events, failures, and legacy tooling.
    def worker_class_name
      if @worker_class&.name && !@worker_class.name.to_s.empty?
        @worker_class.name
      else
        "go:#{@job_type}"
      end
    end
  end
end
