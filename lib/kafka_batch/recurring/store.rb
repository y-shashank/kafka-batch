# frozen_string_literal: true

require "json"
require_relative "reader"

module KafkaBatch
  module Recurring
    # Write side for recurring schedules — the dashboard's register / pause /
    # resume / delete actions. Shares the AR model (and connection binding) with
    # Reader. The Go daemon owns firing; these calls only mutate rows, and the
    # daemon picks changes up on its next tick (≤ one resolution window).
    module Store
      module_function

      MISFIRE_POLICIES = %w[skip fire_once backfill].freeze
      NAME_RE = /\A[a-zA-Z0-9_.:-]{1,191}\z/

      Result = Struct.new(:ok, :error, :schedule, keyword_init: true)

      # canonical_job_type maps a user-entered handler identifier to the canonical
      # manifest job_type that enqueue_job / the daemon expect. It accepts either:
      #   - a job_type already in the manifest (e.g. "hello.ruby", "hello.go") → as-is
      #   - a Ruby worker CLASS name (e.g. "HelloRubyWorker")                  → its job_type
      # Returns nil when the value resolves to neither (unknown). This is why a
      # schedule registered with a worker-class name failed with
      # "Unknown job_type: HelloRubyWorker": enqueue_job resolves job_types only.
      def canonical_job_type(value)
        v = value.to_s.strip
        return nil if v.empty?
        return v unless defined?(KafkaBatch::HandlerRegistry)

        reg = KafkaBatch::HandlerRegistry
        # Already a known manifest job_type? (non-binding existence check)
        return v if reg.respond_to?(:registered?) && reg.registered?(v)

        # Otherwise try to resolve it as a Ruby worker CLASS name → its job_type.
        begin
          handler = reg.resolve!("worker_class" => v)
          return handler.job_type if handler&.job_type
        rescue StandardError
          # not a known worker class either
        end
        nil
      end

      # registry_populated? reports whether we can trust canonical_job_type to
      # reject unknowns (the manifest/workers are loaded in this process). When
      # false (e.g. a bare producer that never loaded the manifest) we store the
      # value as given rather than risk a false-negative rejection.
      def registry_populated?
        defined?(KafkaBatch::HandlerManifest) && KafkaBatch::HandlerManifest.respond_to?(:loaded?) &&
          KafkaBatch::HandlerManifest.loaded?
      end

      # upsert creates or updates a schedule by name, recomputing next_run_at from
      # the cron expression so an edited cron takes effect immediately. Returns a
      # Result; ok=false carries a validation error message for a 400.
      def upsert(name:, cron:, job_type:, timezone: "UTC", args: {}, tenant_id: nil,
                 misfire_policy: "fire_once", enabled: true)
        name = name.to_s.strip
        cron = cron.to_s.strip
        job_type = job_type.to_s.strip
        timezone = timezone.to_s.strip
        timezone = "UTC" if timezone.empty?
        misfire_policy = misfire_policy.to_s.strip
        misfire_policy = "fire_once" if misfire_policy.empty?

        return err("name is required") if name.empty?
        return err("name must match #{NAME_RE.source}") unless name.match?(NAME_RE)
        return err("cron is required") if cron.empty?
        return err("job_type is required") if job_type.empty?
        unless MISFIRE_POLICIES.include?(misfire_policy)
          return err("misfire_policy must be one of #{MISFIRE_POLICIES.join(', ')}")
        end

        # Normalize a worker-class name (HelloRubyWorker) to its manifest job_type
        # (hello.ruby) and reject a truly-unknown identifier early, so the schedule
        # can actually be enqueued by run-now AND fired by the daemon (both resolve
        # job_types, not class names) instead of failing later with
        # "Unknown job_type: ...".
        if (canonical = canonical_job_type(job_type))
          job_type = canonical
        elsif registry_populated?
          return err("unknown job_type or worker class #{job_type.inspect} " \
                     "(not found in the handler manifest)")
        end

        args = coerce_args(args)
        return err("args must be a JSON object") if args.nil?

        begin
          next_run = Reader.next_run_at(cron, timezone)
        rescue ArgumentError => e
          return err(e.message)
        end

        now = Time.now.utc
        rec = model.find_or_initialize_by(name: name)
        rec.cron_expr      = cron
        rec.timezone       = timezone
        rec.job_type       = job_type
        # Assign the Hash directly: the args_json column is MySQL JSON, so AR
        # encodes it to a proper JSON *object* — matching what the Go daemon
        # writes (json.Marshal) and reads. Assigning a pre-encoded String would
        # double-encode it and break Go's json.Unmarshal.
        rec.args_json      = args
        rec.tenant_id      = (tenant_id.to_s.strip.empty? ? nil : tenant_id.to_s.strip)
        rec.enabled        = enabled ? 1 : 0
        rec.misfire_policy = misfire_policy
        rec.next_run_at    = next_run
        rec.created_at     ||= now
        rec.updated_at     = now
        rec.save!

        Result.new(ok: true, schedule: Reader.serialize(rec, stale_factor: Reader::DEFAULT_STALE_FACTOR, now: now))
      rescue StandardError => e
        err(e.message)
      end

      # set_enabled pauses (false) or resumes (true) a schedule. Returns false
      # when no schedule by that name exists.
      def set_enabled(name, enabled)
        rec = model.find_by(name: name.to_s)
        return false unless rec

        rec.update!(enabled: enabled ? 1 : 0, updated_at: Time.now.utc)
        true
      end

      # delete removes a schedule. Returns the number of rows deleted (0 or 1).
      def delete(name)
        model.where(name: name.to_s).delete_all
      end

      def find(name)
        model.find_by(name: name.to_s)
      end

      def coerce_args(args)
        case args
        when nil then {}
        when Hash then args
        when String
          return {} if args.strip.empty?

          parsed = JSON.parse(args)
          parsed.is_a?(Hash) ? parsed : nil
        else
          nil
        end
      rescue JSON::ParserError
        nil
      end

      def err(message)
        Result.new(ok: false, error: message)
      end

      def model
        Reader.model
      end
    end
  end
end
