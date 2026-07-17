# frozen_string_literal: true

require "yaml"
require "time"

module KafkaBatch
  module Ai
    # Sanitized routing topology for the AI live config snapshot.
    #
    # Sources (host-app YAML + in-process registry — never operational Redis):
    #   - kafka_batch_handlers.yml (config.handler_manifest_path)
    #   - priority queue YAML files (config.priority_config_paths)
    #   - extra_job_topics / jobs_topics
    #   - registered Worker classes when available (fill gaps not in the manifest)
    #
    # Read-only: does not call HandlerManifest.load! (avoids resetting a live registry).
    module RoutingSnapshot
      module_function

      # @return [Hash]
      def build(cfg = KafkaBatch.config)
        handlers, handlers_source = collect_handlers(cfg)
        groups, groups_error = collect_priority_groups(cfg)

        {
          "refreshed_at" => Time.now.utc.iso8601,
          "handler_manifest_path" => cfg.handler_manifest_path.to_s,
          "handlers_source" => handlers_source,
          "handler_count" => handlers.size,
          "handlers" => handlers,
          "priority_config_paths" => Array(cfg.resolved_priority_config_paths),
          "priority_group_count" => groups.size,
          "priority_groups" => groups,
          "priority_error" => groups_error,
          "extra_job_topics" => Array(cfg.extra_job_topics).map(&:to_s).reject(&:empty?),
          "jobs_topics" => Array(cfg.jobs_topics).map(&:to_s).reject(&:empty?)
        }.compact
      end

      # @api private
      def collect_handlers(cfg)
        by_type = {}

        if HandlerManifest.loaded?
          HandlerManifest.definitions.each_value do |definition|
            row = serialize_definition(definition)
            by_type[row["job_type"]] = row
          end
          source = "registry"
        else
          path = resolved_manifest_path(cfg)
          if path && File.file?(path)
            parse_manifest_file(path).each { |row| by_type[row["job_type"]] = row }
            source = "yaml"
          else
            source = path.to_s.empty? ? "none" : "missing"
          end
        end

        worker_handlers.each do |row|
          by_type[row["job_type"]] ||= row.merge("source" => "worker")
        end

        [by_type.values.sort_by { |h| h["job_type"].to_s }, source]
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Ai::RoutingSnapshot] handlers failed: #{e.message}")
        [[], "error:#{e.class}"]
      end
      module_function :collect_handlers

      # @api private
      def collect_priority_groups(cfg)
        paths = Array(cfg.resolved_priority_config_paths)
        return [[], nil] if paths.empty?

        registry = KafkaBatch::Priority::Registry.load(paths, cfg: cfg)
        rows = registry.configs.map do |prio|
          {
            "path" => prio.path.to_s,
            "consumer_group_suffix" => prio.consumer_group_suffix.to_s,
            "consumer_group" => prio.consumer_group.to_s,
            "mode" => prio.mode.to_s,
            "weighted_interleave" => prio.weighted_interleave,
            "topics" => Array(prio.topics).map(&:to_s),
            "topic_count" => Array(prio.topics).size
          }
        end
        [rows, nil]
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][Ai::RoutingSnapshot] priority failed: #{e.message}")
        [[], e.message]
      end
      module_function :collect_priority_groups

      # @api private
      def parse_manifest_file(path)
        data = YAML.safe_load(File.read(path), permitted_classes: [], aliases: true) || {}
        handlers = data["handlers"] || data[:handlers] || {}
        handlers.map do |job_type, entry|
          definition = HandlerDefinition.from_manifest_entry(job_type, entry || {})
          serialize_definition(definition).merge("source" => "yaml")
        end
      end
      module_function :parse_manifest_file

      # @api private
      def worker_handlers
        return [] unless KafkaBatch.respond_to?(:workers)

        KafkaBatch.workers.filter_map do |worker|
          next unless worker.is_a?(Class) && worker.include?(KafkaBatch::Worker)

          serialize_definition(HandlerDefinition.from_worker(worker))
        rescue StandardError
          nil
        end
      rescue StandardError
        []
      end
      module_function :worker_handlers

      # @api private
      def serialize_definition(definition)
        row = {
          "job_type" => definition.job_type.to_s,
          "runtime" => definition.runtime.to_s,
          "topic_base" => definition.topic_base.to_s,
          "apply_topic_prefix" => definition.apply_topic_prefix,
          "worker_class" => definition.worker_class_name.to_s,
          "max_retries" => definition.max_retries,
          "retry_tier" => definition.retry_tier&.to_s
        }
        if definition.fairness?
          ft = definition.fairness_type
          cfg = KafkaBatch.config
          row["fairness"] = true
          row["fairness_type"] = ft.to_s
          row["ingest_topic"] = cfg.fairness_ingest_topic(ft).to_s
          row["ready_topic_ruby"] = cfg.fairness_ready_topic(ft, :ruby).to_s
          row["ready_topic_go"] = cfg.fairness_ready_topic(ft, :go).to_s
          row["topic"] = row["ingest_topic"]
        else
          row["fairness"] = false
          row["topic"] = definition.kafka_topic.to_s
        end
        row.compact
      end
      module_function :serialize_definition

      # @api private
      def resolved_manifest_path(cfg)
        if cfg.respond_to?(:resolved_handler_manifest_path)
          p = cfg.resolved_handler_manifest_path
          return p if p && !p.to_s.empty?
        end
        path = cfg.handler_manifest_path.to_s.strip
        return nil if path.empty?

        File.expand_path(path)
      end
      module_function :resolved_manifest_path
    end
  end
end
