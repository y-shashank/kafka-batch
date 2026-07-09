# frozen_string_literal: true

require "yaml"

module KafkaBatch
  # Loads handler definitions from YAML and registers them in HandlerRegistry
  # (routing for ruby and go runtimes; go execution is in kafka-batch-go).
  #
  # Example config/kafka_batch_handlers.yml:
  #
  #   handlers:
  #     segment.export:
  #       runtime: go
  #       topic: segment.exports
  #       max_retries: 25
  class HandlerManifest
    class << self
      def load!(path)
        path = path.to_s.strip
        return reset! if path.empty?

        expanded = File.expand_path(path)
        unless File.file?(expanded)
          raise ConfigurationError, "handler manifest not found: #{expanded}"
        end

        data = YAML.safe_load(File.read(expanded), permitted_classes: [], aliases: true)
        load_from_hash(data || {})
      end

      def load_from_hash(data)
        reset!
        handlers = data["handlers"] || data[:handlers] || {}
        handlers.each do |job_type, entry|
          definition = HandlerDefinition.from_manifest_entry(job_type, entry || {})
          register_definition!(definition)
        end
        @loaded = true
        definitions
      end

      def definitions
        @definitions ||= {}
      end

      def [](job_type)
        definitions[job_type.to_s]
      end

      def topics
        definitions.values.map(&:kafka_topic).uniq
      end

      # Plain (non-fair) topics for runtime :go handlers — kbatch worker -go-worker-jobs group.
      def go_plain_topics
        definitions.values
                   .select { |d| d.runtime == :go && !d.fairness? }
                   .map(&:kafka_topic)
                   .compact
                   .uniq
      end

      # Priority YAML topics that have at least one runtime :go handler on that topic.
      def go_priority_topics_by_group(registry)
        return {} unless registry

        out = {}
        registry.configs.each do |prio|
          go_topics = prio.topics.select do |topic|
            definitions.values.any? do |d|
              d.runtime == :go && !d.fairness? && d.kafka_topic == topic
            end
          end
          next if go_topics.empty?

          group = KafkaBatch.go_worker_priority_consumer_group(prio.consumer_group_suffix)
          out[group] = go_topics
        end
        out
      end

      def reset!
        @definitions = {}
        @loaded      = false
      end

      def loaded?
        @loaded == true
      end

      private

      def register_definition!(definition)
        job_type = definition.job_type
        if definitions.key?(job_type)
          raise ConfigurationError, "duplicate handler in manifest: #{job_type.inspect}"
        end

        definitions[job_type] = definition
        HandlerRegistry.register_definition(definition)
      end
    end
  end
end
