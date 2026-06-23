KafkaBatch.configure do |config|
  # ── State store ────────────────────────────────────────────────────────────
  # :mysql  – persistent, queryable, survives Redis restarts
  #           requires running: rails g kafka_batch:install --store mysql
  # :redis  – lower latency, no schema migration needed
  config.store = :mysql

  # ── Kafka brokers ──────────────────────────────────────────────────────────
  config.brokers = (ENV["KAFKA_BROKERS"] || "localhost:9092").split(",")

  # ── Topic names ────────────────────────────────────────────────────────────
  # Change these if you want a namespace prefix, e.g. "myapp.kafka_batch.jobs"
  config.jobs_topic        = "kafka_batch.jobs"
  config.events_topic      = "kafka_batch.events"
  config.callbacks_topic   = "kafka_batch.callbacks"
  config.dead_letter_topic = "kafka_batch.dead_letter"
  config.retry_topic       = "kafka_batch.jobs.retry"

  # ── Consumer group ─────────────────────────────────────────────────────────
  config.consumer_group = "kafka-batch"

  # ── Retry behaviour ────────────────────────────────────────────────────────
  config.max_retries   = 3    # global default; override per Worker class
  config.retry_backoff = 5    # seconds; linear: attempt * retry_backoff

  # ── Redis (only used when store: :redis) ──────────────────────────────────
  config.redis_url       = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
  config.redis_pool_size = 5
  config.batch_ttl       = 7 * 24 * 3600  # 7 days; set nil to never expire

  # ── Reconciliation ─────────────────────────────────────────────────────────
  # Batches stuck in "running" older than this threshold are re-evaluated.
  # Trigger via: rake kafka_batch:reconcile (or a cron job)
  config.reconciliation_interval = 300  # seconds

  # ── Advanced: raw rdkafka / WaterDrop config overrides ────────────────────
  # config.producer_config = { "compression.type" => "snappy" }
  # config.consumer_config = { "fetch.min.bytes"  => "1024"   }

  # ── Logging ────────────────────────────────────────────────────────────────
  # Defaults to Rails.logger when running inside Rails
  # config.logger = Logger.new($stdout)
end
