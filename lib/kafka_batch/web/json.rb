# frozen_string_literal: true

require "oj"

module KafkaBatch
  class Web
    # JSON response helpers for the dashboard API.
    module Json
      module_function

      def headers
        { "content-type" => "application/json; charset=utf-8", "cache-control" => "no-store" }
      end

      # Prefer Json.ok(ok: true, status: "running") — HTTP code uses http_status:
      # so a JSON "status" field never collides with the Rack status code.
      def ok(payload = nil, http_status: 200, **fields)
        body = payload.is_a?(Hash) ? payload.merge(fields) : fields
        [http_status, headers, [Oj.dump(body, mode: :compat)]]
      end

      def error(http_status, message, extra = {})
        ok({ ok: false, error: message }.merge(extra), http_status: http_status)
      end
    end
  end
end
