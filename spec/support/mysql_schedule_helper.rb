# frozen_string_literal: true

begin
  require "mysql2"
rescue LoadError
  # optional — MySQL schedule integration specs skip when unavailable
end

module KafkaBatchSpec
  module MysqlScheduleHelper
    SCHEMA_SQL = <<~SQL.squish
      CREATE TABLE IF NOT EXISTS kafka_batch_scheduled_jobs (
        job_id       VARCHAR(36)  NOT NULL,
        run_at       DATETIME(6)  NOT NULL,
        partition_id INT          NOT NULL,
        kafka_offset BIGINT       NOT NULL,
        batch_id     VARCHAR(36)  NULL,
        lease_until  DATETIME(6)  NULL,
        created_at   DATETIME(6)  NOT NULL,
        PRIMARY KEY (job_id),
        KEY idx_kb_scheduled_due (run_at, lease_until),
        KEY idx_kb_scheduled_batch_id (batch_id)
      )
    SQL

    module_function

    def dsn
      ENV["KAFKA_BATCH_TEST_MYSQL_DSN"].to_s.strip
    end

    def available?
      return false unless defined?(Mysql2)
      return false if dsn.empty?

      with_client { |c| c.query("SELECT 1") }
      true
    rescue StandardError
      false
    end

    def prepare!
      with_client do |c|
        c.query(SCHEMA_SQL)
        c.query("TRUNCATE TABLE kafka_batch_scheduled_jobs")
      end
    end

    def truncate!
      with_client { |c| c.query("TRUNCATE TABLE kafka_batch_scheduled_jobs") }
    end

    def with_client
      client = Mysql2::Client.new(dsn)
      yield client
    ensure
      client&.close
    end
  end
end
