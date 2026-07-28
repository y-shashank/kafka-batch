# frozen_string_literal: true

require_relative "rules/base"
require_relative "rules/lag_stuck_growing"
require_relative "rules/redis_rtt_high"
require_relative "rules/no_live_consumers"
require_relative "rules/reconciler_stale"
require_relative "rules/fairness_ingest_backed_up"
require_relative "rules/dlt_rate_high"
require_relative "rules/schedule_depth_high"
require_relative "rules/cron_stale"
require_relative "rules/tenant_error_rate_high"
