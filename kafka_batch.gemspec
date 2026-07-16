require_relative "lib/kafka_batch/version"

Gem::Specification.new do |spec|
  spec.name          = "kafka-batch"
  spec.version       = KafkaBatch::VERSION
  spec.authors       = ["Shashank Yadav"]
  spec.email         = ["shashank.yadav@partech.com"]
  spec.summary       = "Sidekiq-Pro-compatible batch semantics on top of Kafka (via Karafka)"
  spec.description   = <<~DESC
    kafka-batch provides Sidekiq-Pro-style batch management (on_success / on_complete
    callbacks, per-job retry, idempotent completion tracking) using Apache Kafka as
    the transport layer via the Karafka ecosystem. State is stored in either MySQL
    (via ActiveRecord) or Redis.
  DESC
  spec.homepage      = "https://github.com/partech/kafka-batch"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 2.7.0"

  spec.files         = Dir["lib/**/*", "bin/*", "db/**/*", "*.gemspec", "*.md"] -
                       Dir["lib/kafka_batch/web/public/**/*.map"]
  spec.bindir        = "bin"
  spec.executables   = []
  spec.require_paths = ["lib"]

  # ── Kafka (Karafka ecosystem) ─────────────────────────────────────────────
  # WaterDrop is Karafka's official producer gem (standalone, no Karafka required)
  spec.add_dependency "waterdrop",       ">= 2.4"
  # Karafka for consumer classes and routing DSL
  spec.add_dependency "karafka",         ">= 2.0"

  # ── JSON ──────────────────────────────────────────────────────────────────
  spec.add_dependency "oj",              ">= 3.0"

  # ── Redis store (loaded only when store: :redis) ─────────────────────────
  spec.add_dependency "redis",           ">= 4.0"
  spec.add_dependency "connection_pool", ">= 2.2"
  spec.add_dependency "xxhash",          ">= 0.4"

  # ── Development ───────────────────────────────────────────────────────────
  spec.add_development_dependency "activerecord", ">= 6.0"
  spec.add_development_dependency "railties",     ">= 6.0"
  spec.add_development_dependency "rspec",        "~> 3.0"
  spec.add_development_dependency "sqlite3"
end
