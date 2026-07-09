# frozen_string_literal: true

require "active_record"
require "oj"
require_relative "database_connection"

module KafkaBatch
  # Optional durable audit trail for KafkaBatch::Web mutating actions.
  #
  # Enable with:
  #   config.audit_enabled = true
  #   config.audit_database_connection = :kafka_batch_audit   # database.yml name
  #   # or an AR model class that already connects_to the audit DB:
  #   config.audit_database_connection = MyApp::KafkaBatchAuditRecord
  #
  # The table is created by the kafka_batch audit migration (see install_migrations).
  module AuditLog
    SENSITIVE_KEYS = %w[
      _csrf password secret token api_key authorization
    ].freeze

    class << self
      def record(action:, path:, method: "POST", actor: nil, status: "ok", metadata: {})
        return unless enabled?

        row = {
          action:     action.to_s,
          path:       path.to_s,
          method:     method.to_s,
          actor:      actor,
          node_id:    KafkaBatch.node_id,
          status:     status.to_s,
          metadata:   serialize_metadata(metadata),
          created_at: Time.now.utc
        }
        model.create!(row)
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][AuditLog] write failed: #{e.message}")
        nil
      end

      # Record a mutating Web UI request after CSRF validation.
      def record_web_action(env:, path:, params: {}, status: "ok", error: nil)
        meta = {
          params: scrub_params(params),
          remote_ip: env["REMOTE_ADDR"],
          user_agent: env["HTTP_USER_AGENT"]
        }
        meta[:error] = error.to_s if error

        record(
          action:   web_action_name(path),
          path:     path,
          actor:    resolve_actor(env),
          status:   status,
          metadata: meta
        )
      end

      def enabled?
        KafkaBatch.config.audit_enabled
      end

      # Public helper for instrumentation and tests.
      def action_name_for(path)
        web_action_name(path)
      end

      def resolve_actor(env)
        custom = KafkaBatch.config.audit_actor
        case custom
        when Proc
          custom.call(env)
        when String
          custom
        else
          first_present(
            env["HTTP_X_KAFKA_BATCH_ACTOR"],
            env["HTTP_X_FORWARDED_USER"],
            env["REMOTE_USER"]
          )
        end
      end

      # Newest-first page of audit rows for the dashboard. Fetches +limit + 1+ so
      # the caller can tell whether a next page exists. Each row is a plain Hash
      # with a parsed +metadata+. Returns [] when auditing is off or the table is
      # unavailable (best-effort — the dashboard never 500s on a read).
      # @return [Array<Hash>]
      def list(limit: 25, offset: 0, action: nil)
        return [] unless enabled?

        lim = limit.to_i.clamp(1, 500)
        off = [offset.to_i, 0].max
        scope = model.order(created_at: :desc, id: :desc)
        scope = scope.where(action: action.to_s) if action && !action.to_s.empty?
        scope.limit(lim).offset(off).map { |r| row_to_h(r) }
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][AuditLog] read failed: #{e.message}")
        []
      end

      # Distinct action names present in the table (for the filter chips), capped.
      # @return [Array<String>]
      def actions(limit: 50)
        return [] unless enabled?

        model.distinct.order(:action).limit(limit.to_i).pluck(:action)
      rescue StandardError => e
        KafkaBatch.logger.warn("[KafkaBatch][AuditLog] actions read failed: #{e.message}")
        []
      end

      def reset!
        @model = nil
      end

      private

      def model
        @model ||= DatabaseConnection.bind(audit_model_class, connection: connection_config)
      end

      def audit_model_class
        klass = Class.new(ActiveRecord::Base)
        klass.table_name         = "kafka_batch_audit_logs"
        klass.inheritance_column = nil
        klass
      end

      def connection_config
        KafkaBatch.config.audit_database_connection
      end

      def first_present(*values)
        values.each do |v|
          s = v.to_s.strip
          return s unless s.empty?
        end
        nil
      end

      def web_action_name(path)
        case path
        when %r{\A/lag/pause\z} then "lag.pause"
        when %r{\A/lag/resume\z} then "lag.resume"
        when %r{\A/weights/throughput/reset\z} then "weights.throughput.reset"
        when %r{\A/weights/(?:time/)?reset\z} then "weights.reset"
        when %r{\A/weights/throughput\z} then "weights.throughput.set"
        when %r{\A/weights} then "weights.set"
        when %r{\A/batches/bulk\z} then "batches.bulk"
        when %r{\A/batches/[^/]+/cancel\z} then "batches.cancel"
        when %r{\A/batches/[^/]+/delete\z} then "batches.delete"
        else "web.#{path.delete_prefix('/').tr('/', '.')}"
        end
      end

      def scrub_params(params)
        flat = params.is_a?(Hash) ? params : {}
        flat.each_with_object({}) do |(k, v), h|
          key = k.to_s
          next if SENSITIVE_KEYS.any? { |s| key.downcase.include?(s) }

          h[key] = v.is_a?(String) && v.bytesize > 512 ? "#{v.byteslice(0, 512)}…" : v
        end
      end

      def serialize_metadata(metadata)
        Oj.dump(metadata || {}, mode: :compat)
      rescue StandardError
        metadata.to_s
      end

      # Normalize an AR audit row into a plain Hash with parsed metadata so the
      # web layer never touches ActiveRecord objects directly.
      def row_to_h(record)
        {
          id:         record.id,
          action:     record.action,
          path:       record.path,
          method:     record.method,
          actor:      record.actor,
          node_id:    record.node_id,
          status:     record.status,
          metadata:   parse_metadata(record.metadata),
          created_at: record.created_at
        }
      end

      def parse_metadata(raw)
        return {} if raw.nil? || raw == ""
        return raw if raw.is_a?(Hash)

        Oj.load(raw.to_s)
      rescue StandardError
        { "raw" => raw.to_s }
      end
    end
  end
end
