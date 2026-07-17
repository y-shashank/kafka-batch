# frozen_string_literal: true

require "connection_pool"
require "json"
require "oj"
require "securerandom"
require "socket"
require "time"
require_relative "../redis_client"
require_relative "../system_info"

module KafkaBatch
  module Ai
    # Loads prebuilt knowledge chunks (from knowledge_chunks.json) into Redis
    # together with a live config snapshot. Designed for many UI pods:
    #
    #   - Heavy markdown chunking is done offline (Chunker → packaged JSON).
    #   - At boot each pod calls sync! (best-effort, never raises).
    #   - Only one pod wins a short NX lock and performs the Redis write.
    #   - Knowledge chunks are rewritten only when packaged corpus_version
    #     differs from Redis meta (no time-based TTL for docs).
    #   - Config snapshot (+ config:live chunk) refreshes at most every
    #     CONFIG_REFRESH_SECONDS (24h) so operator knob changes propagate
    #     without republishing the knowledge base.
    #
    # Redis keys (assistant namespace — never operational ledger keys):
    #   kafka_batch:ai:knowledge:lock
    #   kafka_batch:ai:knowledge:meta
    #   kafka_batch:ai:knowledge:chunks   (HASH id → JSON)
    #   kafka_batch:ai:knowledge:config   (JSON snapshot)
    #   kafka_batch:ai:knowledge:ids      (JSON array of chunk ids)
    #
    # Rebuild / release instructions: see lib/kafka_batch/ai/README.md
    module KnowledgeIndex
      LOCK_KEY   = "kafka_batch:ai:knowledge:lock"
      META_KEY   = "kafka_batch:ai:knowledge:meta"
      CHUNKS_KEY = "kafka_batch:ai:knowledge:chunks"
      CONFIG_KEY = "kafka_batch:ai:knowledge:config"
      IDS_KEY    = "kafka_batch:ai:knowledge:ids"

      LOCK_TTL_SECONDS = 120
      # How often boot may refresh the live config snapshot (including broker
      # topic partition inventory) when the knowledge corpus version is unchanged.
      # Not a Redis key TTL — compared to meta["config_refreshed_at"].
      # Only the pod that wins the NX lock performs the write.
      CONFIG_REFRESH_SECONDS = 24 * 3600
      LIVE_CONFIG_CHUNK_ID = "config:live"

      RELEASE_LOCK_LUA = <<~LUA.freeze
        if redis.call('GET', KEYS[1]) == ARGV[1] then
          return redis.call('DEL', KEYS[1])
        end
        return 0
      LUA

      class << self
        # Best-effort boot sync. Safe to call from every UI/worker pod.
        # @return [Symbol] :disabled | :skipped_locked | :skipped_fresh |
        #   :synced_corpus | :synced_config | :error
        def sync!
          return :disabled unless enabled?
          return :disabled unless KafkaBatch.config.redis_configured?

          packaged = load_packaged!
          return :error if packaged.nil?

          redis_with do |r|
            token = SecureRandom.hex(16)
            acquired = r.set(LOCK_KEY, token, nx: true, ex: LOCK_TTL_SECONDS)
            return :skipped_locked unless acquired

            begin
              meta = parse_hash(r.hgetall(META_KEY))
              need_corpus = corpus_stale?(meta, packaged["corpus_version"])
              need_config = config_stale?(meta)

              if !need_corpus && !need_config
                return :skipped_fresh
              end

              if need_corpus
                write_corpus!(r, packaged)
                :synced_corpus
              else
                write_config_only!(r, meta)
                :synced_config
              end
            ensure
              r.eval(RELEASE_LOCK_LUA, keys: [LOCK_KEY], argv: [token])
            end
          end
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch][Ai::KnowledgeIndex] sync skipped: #{e.class}: #{e.message}")
          :error
        end

        def enabled?
          KafkaBatch.config.ai_knowledge_enabled
        end

        def packaged_path
          File.expand_path("knowledge_chunks.json", __dir__)
        end

        def load_packaged!
          path = packaged_path
          unless File.file?(path)
            KafkaBatch.logger.warn("[KafkaBatch][Ai::KnowledgeIndex] missing #{path}")
            return nil
          end

          Oj.load(File.read(path))
        rescue StandardError => e
          KafkaBatch.logger.warn("[KafkaBatch][Ai::KnowledgeIndex] load failed: #{e.message}")
          nil
        end

        # Read helpers for future RAG (docs-only — no operational keys).
        def meta
          redis_with { |r| parse_hash(r.hgetall(META_KEY)) } || {}
        end

        def config_snapshot
          raw = redis_with { |r| r.get(CONFIG_KEY) }
          return {} if raw.nil? || raw.empty?

          Oj.load(raw)
        rescue StandardError
          {}
        end

        def chunk_ids
          raw = redis_with { |r| r.get(IDS_KEY) }
          return [] if raw.nil? || raw.empty?

          Oj.load(raw)
        rescue StandardError
          []
        end

        def fetch_chunk(id)
          raw = redis_with { |r| r.hget(CHUNKS_KEY, id.to_s) }
          return nil if raw.nil? || raw.empty?

          Oj.load(raw)
        rescue StandardError
          nil
        end

        def reset_pool!
          @pool&.shutdown(&:close) rescue nil
          @pool = nil
        end

        private

        def corpus_stale?(meta, corpus_version)
          return true if meta.nil? || meta.empty?

          meta["corpus_version"].to_s != corpus_version.to_s
        end

        def config_stale?(meta)
          return true if meta.nil? || meta.empty?
          # One-shot after deploy of topic inventory: older meta has no stamp.
          return true if meta["topics_refreshed_at"].to_s.empty?

          stamp = meta["config_refreshed_at"].to_s
          stamp = meta["refreshed_at"].to_s if stamp.empty?
          refreshed_at = Time.parse(stamp.to_s).to_i rescue 0
          return true if refreshed_at <= 0

          (Time.now.to_i - refreshed_at) >= CONFIG_REFRESH_SECONDS
        end

        def write_corpus!(r, packaged)
          now = Time.now.utc.iso8601
          snapshot = build_config_snapshot
          live_chunk = build_live_config_chunk(snapshot)
          chunks = Array(packaged["chunks"]).map { |c| c.dup }
          chunks << live_chunk

          r.del(CHUNKS_KEY)
          chunks.each_slice(100) do |slice|
            fields = {}
            slice.each { |c| fields[c["id"]] = Oj.dump(c) }
            r.hset(CHUNKS_KEY, fields) unless fields.empty?
          end

          r.set(CONFIG_KEY, Oj.dump(snapshot))
          r.set(IDS_KEY, Oj.dump(chunks.map { |c| c["id"] }))
          r.hset(
            META_KEY,
            "corpus_version"       => packaged["corpus_version"].to_s,
            "chunk_count"          => chunks.size.to_s,
            "refreshed_at"         => now,
            "config_refreshed_at"  => now,
            "topics_refreshed_at"  => (snapshot.dig("topic_inventory", "refreshed_at").to_s.empty? ? now : snapshot.dig("topic_inventory", "refreshed_at").to_s),
            "refreshed_by"         => pod_id,
            "packaged_built"       => packaged["built_at"].to_s
          )

          KafkaBatch.logger.info(
            "[KafkaBatch][Ai::KnowledgeIndex] synced corpus chunks=#{chunks.size} " \
            "corpus=#{packaged['corpus_version']} by=#{pod_id}"
          )
        end

        # Knowledge unchanged — refresh masked live config only.
        def write_config_only!(r, meta)
          now = Time.now.utc.iso8601
          snapshot = build_config_snapshot
          live_chunk = build_live_config_chunk(snapshot)

          r.set(CONFIG_KEY, Oj.dump(snapshot))
          r.hset(CHUNKS_KEY, LIVE_CONFIG_CHUNK_ID, Oj.dump(live_chunk))
          r.hset(
            META_KEY,
            "config_refreshed_at" => now,
            "topics_refreshed_at" => (snapshot.dig("topic_inventory", "refreshed_at").to_s.empty? ? now : snapshot.dig("topic_inventory", "refreshed_at").to_s),
            "config_refreshed_by" => pod_id,
            # Preserve corpus fields; touch refreshed_at only for observability of last writer.
            "refreshed_by" => pod_id
          )

          KafkaBatch.logger.info(
            "[KafkaBatch][Ai::KnowledgeIndex] refreshed config snapshot " \
            "corpus=#{meta['corpus_version']} by=#{pod_id}"
          )
        end

        def build_live_config_chunk(snapshot)
          lines = ["Live configuration snapshot (this deploy)", ""]
          inventory = snapshot["topic_inventory"]
          snapshot.each do |key, value|
            next if key == "topic_inventory"

            lines << "#{key}: #{value}"
          end
          if inventory.is_a?(Hash)
            lines << ""
            lines << "AUTHORITATIVE LIVE TOPIC PARTITIONS (from Kafka broker)"
            lines << "Rule: broker_partitions = actual cluster count. " \
                     "configured_partitions = create_topics DEFAULT only — never report it as live."
            lines << "topic_inventory_available: #{inventory['available']}"
            lines << "topic_count: #{inventory['topic_count']}"
            lines << "broker_known_count: #{inventory['broker_known_count']}"
            lines << "topics_refreshed_at: #{inventory['refreshed_at']}"
            Array(inventory["topics"]).each do |t|
              next unless t.is_a?(Hash)

              broker = t["broker_partitions"].nil? ? "n/a" : t["broker_partitions"]
              lines << "- #{t['name']}: live_broker_partitions=#{broker} " \
                       "create_default_partitions=#{t['configured_partitions']} " \
                       "category=#{t['category']} status=#{t['status']} rf=#{t['replication_factor']}"
            end
          end
          text = lines.join("\n")
          {
            "id"           => LIVE_CONFIG_CHUNK_ID,
            "source"       => "config",
            "title"        => "Live configuration snapshot",
            "heading_path" => ["Live configuration"],
            "section"      => "Live configuration",
            "text"         => text,
            "char_count"   => text.length
          }
        end

        # Masked, flat snapshot of operator-facing knobs for RAG.
        def build_config_snapshot
          c = KafkaBatch.config
          inventory =
            begin
              KafkaBatch::Topics.inventory
            rescue StandardError => e
              KafkaBatch.logger.warn("[KafkaBatch][Ai::KnowledgeIndex] topic inventory failed: #{e.message}")
              {
                "available" => false,
                "refreshed_at" => Time.now.utc.iso8601,
                "topic_count" => 0,
                "broker_known_count" => 0,
                "topics" => [],
                "error" => e.message
              }
            end

          {
            "brokers" => Array(c.brokers).join(","),
            "redis_url" => SystemInfo.mask_redis_url(c.redis_url),
            "topic_prefix" => c.topic_prefix.to_s,
            "consumer_group" => c.consumer_group.to_s,
            "store" => c.store.to_s,
            "schedule_store" => c.schedule_store.to_s,
            "schedule_poller_enabled" => c.schedule_poller_enabled,
            "jobs_topic" => c.jobs_topic.to_s,
            "events_topic" => c.events_topic.to_s,
            "callbacks_topic" => c.callbacks_topic.to_s,
            "dead_letter_topic" => c.dead_letter_topic.to_s,
            "retry_topic" => c.retry_topic.to_s,
            "scheduled_topic" => c.scheduled_topic.to_s,
            "fair_time_ingest_topic" => c.fair_time_ingest_topic.to_s,
            "fair_time_ready_ruby_topic" => c.fair_time_ready_ruby_topic.to_s,
            "fair_time_ready_go_topic" => c.fair_time_ready_go_topic.to_s,
            "fair_throughput_ingest_topic" => c.fair_throughput_ingest_topic.to_s,
            "max_retries" => c.max_retries,
            "retry_tiers" => c.retry_tiers,
            "retry_max_pause_seconds" => c.retry_max_pause_seconds,
            "super_fetch_concurrency" => c.super_fetch_concurrency,
            "super_fetch_claim_window" => c.super_fetch_claim_window,
            "super_fetch_lease_ttl" => c.super_fetch_lease_ttl,
            "super_fetch_orphan_grace" => c.super_fetch_orphan_grace,
            "super_fetch_reclaim_enabled" => c.super_fetch_reclaim_enabled,
            "super_fetch_reclaim_interval" => c.super_fetch_reclaim_interval,
            "super_fetch_reclaim_limit" => c.super_fetch_reclaim_limit,
            "super_fetch_drain_timeout" => c.super_fetch_drain_timeout,
            "redis_pool_size" => c.redis_pool_size,
            "fairness_global_concurrency" => c.fairness_global_concurrency,
            "fairness_ready_window" => c.fairness_ready_window,
            "fairness_lease_ttl" => c.fairness_lease_ttl,
            "fairness_weighted_concurrency" => c.fairness_weighted_concurrency,
            "fairness_max_inflight_per_tenant" => c.fairness_max_inflight_per_tenant,
            "fairness_dynamic_tenant_partitions" => c.fairness_dynamic_tenant_partitions,
            "fairness_min_ingest_partitions" => c.fairness_min_ingest_partitions,
            "fairness_default_weight" => c.fairness_default_weight,
            "uniq_enabled" => c.uniq_enabled,
            "uniq_lock_ttl" => c.uniq_lock_ttl,
            "uniq_on_duplicate" => c.uniq_on_duplicate.to_s,
            "skip_cancelled_jobs" => c.skip_cancelled_jobs,
            "cancellation_cache_ttl" => c.cancellation_cache_ttl,
            "liveness_backend" => c.liveness_backend.to_s,
            "liveness_ttl" => c.liveness_ttl,
            "liveness_heartbeat_interval" => c.liveness_heartbeat_interval,
            "track_running_jobs" => c.track_running_jobs,
            "schedule_poll_interval" => c.schedule_poll_interval,
            "schedule_poll_max_interval" => c.schedule_poll_max_interval,
            "schedule_batch_size" => c.schedule_batch_size,
            "schedule_lease_seconds" => c.schedule_lease_seconds,
            "max_schedule_horizon" => c.max_schedule_horizon,
            "batch_ttl" => c.batch_ttl,
            "reconciliation_interval" => c.reconciliation_interval,
            "consumption_control_refresh_interval" => c.consumption_control_refresh_interval,
            "daemon_mode" => c.daemon_mode,
            "handler_manifest_path" => c.handler_manifest_path.to_s,
            "performance_metrics_enabled" => c.performance_metrics_enabled,
            "ai_knowledge_enabled" => c.ai_knowledge_enabled,
            "topics_replication_factor" => c.topics_replication_factor,
            "topic_inventory" => inventory
          }
        end

        def pod_id
          host = Socket.gethostname rescue "unknown"
          pid  = Process.pid
          "#{host}:#{pid}"
        end

        def parse_hash(h)
          return {} if h.nil? || h.empty?

          h.transform_keys(&:to_s)
        end

        def redis_with
          return nil unless KafkaBatch.config.redis_configured?

          pool.with { |r| yield r }
        end

        def pool
          @pool ||= ConnectionPool.new(size: 1, timeout: 3) do
            client = RedisClient.new(KafkaBatch.config)
            raise "Redis not configured" unless client

            client
          end
        end
      end
    end
  end
end
