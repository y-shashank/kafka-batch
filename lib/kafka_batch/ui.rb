# frozen_string_literal: true

# kafka_batch/ui — lightweight entry point for web/API processes that mount
# the KafkaBatch dashboard but do NOT run Karafka consumers or produce jobs.
#
# Loads only what the dashboard needs:
#   - Configuration + store (Redis or MySQL reads/writes)
#   - Lag (Karafka::Admin reads — gracefully absent when Karafka isn't loaded)
#   - Liveness (consumer heartbeats + running-job tracking)
#   - ConsumptionControl (pause/resume state)
#   - Partition (tenant ingest-partition lookup)
#   - CancellationCache (batch cancel action)
#   - Web (the Rack dashboard app)
#
# Does NOT load: Worker DSL, Batch (creation/push), Producer, consumers,
# Reconciler, Topics provisioning, Fairness dispatcher/scheduler.
#
# Usage in Gemfile (web service):
#   gem "kafka-batch", require: "kafka_batch/ui"
#
# Usage in Gemfile (worker service — full backend):
#   gem "kafka-batch", require: "kafka_batch"
#
# In the web service initializer, configure just the connection details:
#
#   KafkaBatch.configure do |c|
#     c.store     = :redis
#     c.redis_url = ENV["REDIS_URL"]
#     c.brokers   = ENV["KAFKA_BROKERS"].split(",")
#     c.logger    = Rails.logger
#   end
#
# Then mount the dashboard as usual:
#   mount KafkaBatch::Web => "/kafka_batch"

require "logger"

# NOTE: require_relative paths here are relative to lib/kafka_batch/ (not lib/)
# because this file lives at lib/kafka_batch/ui.rb.
require_relative "version"
require_relative "errors"
require_relative "configuration"
require_relative "instrumentation"
require_relative "core"
require_relative "stores/base"
require_relative "stores/mysql_store"
require_relative "stores/redis_store"
require_relative "liveness"
require_relative "lag"
require_relative "partition"
require_relative "consumption_control"
require_relative "cancellation_cache"
require_relative "web"

require_relative "railtie" if defined?(Rails::Railtie)
