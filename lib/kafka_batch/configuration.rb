module KafkaBatch
  class Configuration
    # ── Store ────────────────────────────────────────────────────────────────
    # :mysql  – uses ActiveRecord (requires kafka_batch migrations)
    # :redis  – uses Redis (no migrations needed)
    attr_accessor :store

    # ── Kafka connection ─────────────────────────────────────────────────────
    attr_accessor :brokers          # Array<String>  e.g. ["localhost:9092"]

    # ── Topic names ──────────────────────────────────────────────────────────
    attr_accessor :jobs_topic       # String  default: "kafka_batch.jobs"
    attr_accessor :events_topic     # String  default: "kafka_batch.events"
    attr_accessor :callbacks_topic  # String  default: "kafka_batch.callbacks"
    attr_accessor :dead_letter_topic # String  default: "kafka_batch.dead_letter"

    # ── Retry topic ──────────────────────────────────────────────────────────
    # Failed jobs are forwarded here with a retry_after timestamp instead of
    # sleeping inside the job consumer (which would block the Kafka partition).
    # The RetryConsumer waits via Karafka pause() then re-enqueues to the
    # original topic.
    attr_accessor :retry_topic       # String  default: "kafka_batch.jobs.retry"

    # ── Consumer ─────────────────────────────────────────────────────────────
    attr_accessor :consumer_group   # String

    # ── Retry behaviour ──────────────────────────────────────────────────────
    attr_accessor :max_retries      # Integer – default per worker (worker can override)
    attr_accessor :retry_backoff    # Integer – seconds; linear: attempt * retry_backoff

    # ── Redis (only when store: :redis) ─────────────────────────────────────
    attr_accessor :redis_url        # String  e.g. "redis://localhost:6379/0"
    attr_accessor :redis_pool_size  # Integer

    # ── TTL for batch metadata in Redis ─────────────────────────────────────
    attr_accessor :batch_ttl        # Integer – seconds; default 7 days

    # ── Reconciliation ───────────────────────────────────────────────────────
    # A periodic sweep that re-checks "running" batches that look stuck.
    attr_accessor :reconciliation_interval  # Integer – seconds; default 300

    # ── Passthrough rdkafka config ───────────────────────────────────────────
    # Merged on top of defaults for the producer.
    attr_accessor :producer_config  # Hash<String, Object>

    # Merged on top of defaults for every consumer.
    attr_accessor :consumer_config  # Hash<String, Object>

    # ── Topic validation ─────────────────────────────────────────────────────
    # When true, KafkaBatch verifies that all configured topics exist in Kafka
    # during Rails boot (requires a working broker connection at startup).
    # Disabled by default to avoid blocking startup in test/CI environments.
    attr_accessor :validate_topics_on_boot  # Boolean  default: false

    # ── Logging ──────────────────────────────────────────────────────────────
    attr_accessor :logger

    def initialize
      @store                    = :mysql
      @brokers                  = ["localhost:9092"]
      @jobs_topic               = "kafka_batch.jobs"
      @events_topic             = "kafka_batch.events"
      @callbacks_topic          = "kafka_batch.callbacks"
      @dead_letter_topic        = "kafka_batch.dead_letter"
      @retry_topic              = "kafka_batch.jobs.retry"
      @consumer_group           = "kafka-batch"
      @max_retries              = 3
      @retry_backoff            = 5
      @redis_url                = "redis://localhost:6379/0"
      @redis_pool_size          = 5
      @batch_ttl                = 7 * 24 * 3600  # 7 days
      @reconciliation_interval  = 300
      @producer_config          = {}
      @consumer_config          = {}
      @validate_topics_on_boot  = false
      @logger                   = Logger.new($stdout).tap { |l| l.progname = "KafkaBatch" }
    end

    def validate!
      raise ConfigurationError, "store must be :mysql or :redis" unless %i[mysql redis].include?(@store)
      raise ConfigurationError, "brokers must not be empty"       if Array(@brokers).empty?

      if @store == :redis
        raise ConfigurationError, "redis_url must be set for :redis store" if @redis_url.nil? || @redis_url.empty?
      end
    end
  end
end
