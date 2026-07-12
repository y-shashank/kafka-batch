# frozen_string_literal: true

# Dash-named shim so Bundler's default auto-require for `gem "kafka-batch"`
# (which tries `require "kafka-batch"`) loads the gem. Without this, a Gemfile
# entry without an explicit `require:` never loads the gem — the railtie and its
# rake tasks (kafka_batch:create_topics, etc.) silently fail to register.
#
# Loads the full backend by default. Web/dashboard-only processes that want the
# lighter surface can still opt in with `gem "kafka-batch", require: "kafka_batch/ui"`.
require_relative "kafka_batch"
