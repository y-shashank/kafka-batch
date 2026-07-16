# frozen_string_literal: true

require "base64"
require "connection_pool"
require "oj"
require "securerandom"
require "stringio"
require "time"
require "zlib"
require_relative "workset/reclaim_scheduler"

module KafkaBatch
  # Redis working-set ledger shared with kafka-batch-go SuperFetch reclaim.
  # Keys and Lua match pkg/workset in the Go repo — do not fork the contract.
  module Workset
    JOB_KEY_PREFIX       = "kafka_batch:work:job:"
    BY_CONSUMER_PREFIX   = "kafka_batch:work:by_consumer:"
    INDEX_KEY            = "kafka_batch:work:index"
    LIVE_CONSUMER_PREFIX = "kafka_batch:live:consumer:"
    RECLAIMING_PREFIX    = "kafka_batch:work:reclaiming:"
    PRODUCED_PREFIX      = "kafka_batch:work:produced:"

    DEFAULT_ORPHAN_GRACE   = 40
    DEFAULT_LEASE_TTL      = 120
    DEFAULT_HEARTBEAT_TTL  = 180
    PRODUCED_MARKER_TTL    = 3600
    DEFAULT_RECLAIM_LOCK   = 30

    ReclaimResult = Struct.new(:checked, :reclaimed, :failed, :skipped, keyword_init: true)

    CLAIM_LUA = <<~LUA
      local jobKey = KEYS[1]
      local byCons = KEYS[2]
      local index = KEYS[3]
      local livePrefix = KEYS[4]
      local jobID = ARGV[1]
      local consumerID = ARGV[2]
      local fence = ARGV[3]
      local payload = ARGV[4]
      local ttl = tonumber(ARGV[5])
      local now = tonumber(ARGV[6])
      local grace = tonumber(ARGV[7]) or 0
      local hbTTL = tonumber(ARGV[8])
      if not hbTTL or hbTTL < 1 then hbTTL = ttl end

      local cur = redis.call('GET', jobKey)
      if cur then
        local ok, obj = pcall(cjson.decode, cur)
        if ok and type(obj) == 'table' then
          local owner = obj['consumer_id']
          if owner and owner ~= '' and owner ~= consumerID then
            local alive = redis.call('EXISTS', livePrefix .. owner)
            if alive == 1 then
              return 0
            end
            local claimedUnix = tonumber(obj['claimed_at_unix'] or 0) or 0
            if grace > 0 and claimedUnix > 0 and (now - claimedUnix) < grace then
              return 0
            end
            redis.call('SREM', 'kafka_batch:work:by_consumer:' .. owner, jobID)
          elseif owner == consumerID then
            redis.call('EXPIRE', jobKey, ttl)
            redis.call('SET', livePrefix .. consumerID, '1', 'EX', hbTTL)
            local claimedUnix = tonumber(obj['claimed_at_unix'] or 0) or now
            redis.call('ZADD', index, claimedUnix, jobID)
            return 2
          end
        end
      end

      redis.call('SET', jobKey, payload, 'EX', ttl)
      redis.call('SADD', byCons, jobID)
      redis.call('ZADD', index, now, jobID)
      redis.call('SET', livePrefix .. consumerID, '1', 'EX', hbTTL)
      return 1
    LUA

    RENEW_LUA = <<~LUA
      local cur = redis.call('GET', KEYS[1])
      if not cur then return 0 end
      local ok, obj = pcall(cjson.decode, cur)
      if not ok or type(obj) ~= 'table' then return 0 end
      if obj['consumer_id'] ~= ARGV[1] or obj['fence'] ~= ARGV[2] then return 0 end
      redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
      return 1
    LUA

    COMPLETE_LUA = <<~LUA
      local cur = redis.call('GET', KEYS[1])
      if not cur then
        redis.call('SREM', KEYS[2], ARGV[1])
        redis.call('ZREM', KEYS[3], ARGV[1])
        return 0
      end
      local ok, obj = pcall(cjson.decode, cur)
      if not ok or type(obj) ~= 'table' then return 0 end
      if obj['consumer_id'] ~= ARGV[2] or obj['fence'] ~= ARGV[3] then return 0 end
      redis.call('DEL', KEYS[1])
      redis.call('SREM', KEYS[2], ARGV[1])
      redis.call('ZREM', KEYS[3], ARGV[1])
      return 1
    LUA

    # KEYS[1]=job KEYS[2]=by_consumer KEYS[3]=zindex KEYS[4]=reclaiming KEYS[5]=produced
    # ARGV[1]=job_id ARGV[2]=fence
    FINISH_RECLAIM_LUA = <<~LUA
      local cur = redis.call('GET', KEYS[1])
      if cur then
        local ok, obj = pcall(cjson.decode, cur)
        if ok and type(obj) == 'table' then
          if obj['fence'] ~= ARGV[2] then
            redis.call('DEL', KEYS[4])
            return 0
          end
          local owner = obj['consumer_id']
          if owner and owner ~= '' then
            redis.call('SREM', 'kafka_batch:work:by_consumer:' .. owner, ARGV[1])
          end
        end
        redis.call('DEL', KEYS[1])
      end
      redis.call('SREM', KEYS[2], ARGV[1])
      redis.call('ZREM', KEYS[3], ARGV[1])
      redis.call('DEL', KEYS[4])
      redis.call('DEL', KEYS[5])
      return 1
    LUA

    Entry = Struct.new(
      :job_id, :payload, :encoding, :topic, :partition, :offset,
      :consumer_id, :fence, :claimed_at, :claimed_at_unix, :runtime,
      keyword_init: true
    )

    ENCODING_GZIP = "gzip"
    COMPRESS_MIN_BYTES = 256

    ClaimResult = Struct.new(:won, :fence, :entry, keyword_init: true)

    class Store
      def initialize(pool: nil)
        @pool = pool
      end

      # @return [ClaimResult]
      def claim(job_id:, payload:, topic:, partition:, offset:, consumer_id:,
                lease_ttl: nil, heartbeat_ttl: nil, steal_grace: nil)
        raise ArgumentError, "workset: empty job_id" if job_id.to_s.empty?

        ttl   = positive_or(lease_ttl, DEFAULT_LEASE_TTL)
        hb    = positive_or(heartbeat_ttl, DEFAULT_HEARTBEAT_TTL)
        grace = resolve_grace(steal_grace)
        fence = SecureRandom.uuid
        now   = Time.now.utc
        entry = Entry.new(
          job_id:          job_id.to_s,
          payload:         payload.to_s.b,
          topic:           topic.to_s,
          partition:       partition.to_i,
          offset:          offset.to_i,
          consumer_id:     consumer_id.to_s,
          fence:           fence,
          claimed_at:      now.iso8601(9),
          claimed_at_unix: now.to_i,
          runtime:         "ruby"
        )
        raw = dump_entry(entry)

        res = redis_with do |r|
          r.eval(
            CLAIM_LUA,
            keys: [job_key(job_id), by_consumer_key(consumer_id), INDEX_KEY, LIVE_CONSUMER_PREFIX],
            argv: [
              job_id.to_s, consumer_id.to_s, fence, raw,
              ttl, now.to_i, grace, hb
            ]
          )
        end

        case res.to_i
        when 1
          ClaimResult.new(won: true, fence: fence, entry: entry)
        when 2
          existing = get_entry(job_id)
          return ClaimResult.new(won: false) unless existing

          ClaimResult.new(won: true, fence: existing.fence, entry: existing)
        else
          ClaimResult.new(won: false)
        end
      end

      def renew(job_id, consumer_id, fence, ttl: nil)
        return false if job_id.to_s.empty?

        lease = positive_or(ttl, DEFAULT_LEASE_TTL)
        n = redis_with do |r|
          r.eval(
            RENEW_LUA,
            keys: [job_key(job_id)],
            argv: [consumer_id.to_s, fence.to_s, lease]
          )
        end
        n.to_i == 1
      end

      def still_owned?(job_id, consumer_id, fence)
        return false if job_id.to_s.empty?

        entry = get_entry(job_id)
        return false unless entry

        entry.consumer_id == consumer_id.to_s && entry.fence == fence.to_s
      end

      def complete(job_id, consumer_id, fence)
        return if job_id.to_s.empty?

        redis_with do |r|
          r.eval(
            COMPLETE_LUA,
            keys: [job_key(job_id), by_consumer_key(consumer_id), INDEX_KEY],
            argv: [job_id.to_s, consumer_id.to_s, fence.to_s]
          )
        end
        nil
      end

      def touch_consumer(consumer_id, ttl: nil)
        return if consumer_id.to_s.empty?

        hb = positive_or(ttl, DEFAULT_HEARTBEAT_TTL)
        redis_with { |r| r.set(live_key(consumer_id), "1", ex: hb) }
        nil
      end

      def get_entry(job_id)
        raw = redis_with { |r| r.get(job_key(job_id)) }
        return nil if raw.nil? || raw.empty?

        parse_entry(raw)
      end

      # Working-set entries older than grace whose consumer heartbeat is missing.
      # Pipelines GET + EXISTS (deduped by consumer_id) to keep reclaim storms cheap.
      def list_orphans(limit: 100, grace: nil)
        lim = limit.to_i
        lim = 100 if lim < 1
        grace_sec = resolve_grace(grace)
        max_score = Time.now.to_i - grace_sec

        ids = redis_with do |r|
          r.zrangebyscore(INDEX_KEY, "-inf", max_score, limit: [0, lim * 3])
        end
        return [] if ids.nil? || ids.empty?

        raws = redis_with do |r|
          r.pipelined { |pipe| ids.each { |id| pipe.get(job_key(id)) } }
        end

        candidates = []
        missing = []
        ids.zip(Array(raws)).each do |id, raw|
          if raw.nil? || raw.empty?
            missing << id
            next
          end
          entry = parse_entry(raw)
          candidates << entry if entry
        end

        if missing.any?
          redis_with { |r| r.pipelined { |pipe| missing.each { |id| pipe.zrem(INDEX_KEY, id) } } }
        end
        return [] if candidates.empty?

        unique_cids = candidates.map(&:consumer_id).uniq
        alive_flags = redis_with do |r|
          r.pipelined { |pipe| unique_cids.each { |cid| pipe.exists(live_key(cid)) } }
        end
        alive_by = {}
        unique_cids.zip(Array(alive_flags)).each do |cid, flag|
          alive_by[cid] = flag.to_i > 0
        end

        out = []
        candidates.each do |entry|
          break if out.size >= lim
          out << entry unless alive_by[entry.consumer_id]
        end
        out
      end

      def begin_reclaim(job_id, lock_ttl: DEFAULT_RECLAIM_LOCK)
        return false if job_id.to_s.empty?

        ttl = positive_or(lock_ttl, DEFAULT_RECLAIM_LOCK)
        redis_with { |r| r.set(reclaiming_key(job_id), "1", nx: true, ex: ttl) }
      end

      def mark_produced(job_id, fence, ttl: PRODUCED_MARKER_TTL)
        return if job_id.to_s.empty?

        ex = positive_or(ttl, PRODUCED_MARKER_TTL)
        redis_with { |r| r.set(produced_key(job_id), fence.to_s, ex: ex) }
        nil
      end

      def produced_fence(job_id)
        return "" if job_id.to_s.empty?

        redis_with { |r| r.get(produced_key(job_id)).to_s }
      end

      # @return [Integer] 1 on success, 0 if fence mismatch / stolen
      def finish_reclaim(entry)
        return 1 unless entry

        n = redis_with do |r|
          r.eval(
            FINISH_RECLAIM_LUA,
            keys: [
              job_key(entry.job_id),
              by_consumer_key(entry.consumer_id),
              INDEX_KEY,
              reclaiming_key(entry.job_id),
              produced_key(entry.job_id)
            ],
            argv: [entry.job_id, entry.fence]
          )
        end
        n.to_i
      end

      def abort_reclaim(job_id)
        return if job_id.to_s.empty?

        redis_with { |r| r.del(reclaiming_key(job_id)) }
        nil
      end

      # Re-produce orphaned workset jobs to their original topic with `_reclaim: true`.
      # +producer+ is a callable of signature `(topic, key, body_string) -> void`.
      def reclaim_orphans(producer:, limit: 100, lock_ttl: DEFAULT_RECLAIM_LOCK, grace: nil)
        out = ReclaimResult.new(checked: 0, reclaimed: 0, failed: 0, skipped: 0)
        return out unless producer

        orphans = list_orphans(limit: limit, grace: grace)
        out.checked = orphans.size
        orphans.each do |entry|
          begin
            won = begin_reclaim(entry.job_id, lock_ttl: lock_ttl)
            unless won
              out.skipped += 1
              next
            end
            reclaim_one(producer, entry)
            out.reclaimed += 1
            KafkaBatch.logger.info(
              "[KafkaBatch][Workset] reclaimed job_id=#{entry.job_id} → topic=#{entry.topic} " \
              "(dead consumer=#{entry.consumer_id})"
            )
          rescue StandardError => e
            out.failed += 1
            KafkaBatch.logger.warn(
              "[KafkaBatch][Workset] reclaim failed job_id=#{entry.job_id}: #{e.class}: #{e.message}"
            )
          end
        end
        out
      end

      def close
        @pool&.shutdown(&:close)
        @pool = nil
      end

      private

      def reclaim_one(producer, entry)
        already = produced_fence(entry.job_id)
        if !already.empty? && already != entry.fence
          redis_with { |r| r.del(produced_key(entry.job_id)) }
          already = ""
        end
        if !already.empty?
          finish_reclaim_checked!(entry)
          return
        end

        body = Workset.mark_reclaim_payload(Workset.payload_for_reclaim(entry))
        if entry.topic.to_s.empty?
          abort_reclaim(entry.job_id)
          raise ArgumentError, "workset: reclaim missing topic job_id=#{entry.job_id}"
        end

        begin
          producer.call(entry.topic, entry.job_id, body)
        rescue StandardError
          abort_reclaim(entry.job_id)
          raise
        end

        begin
          mark_produced_retry(entry.job_id, entry.fence)
        rescue StandardError => e
          # Produce already happened — never Abort. Prefer finish; rare double-produce
          # only if the marker could not be written.
          begin
            finish_reclaim_checked!(entry)
            return
          rescue StandardError
            raise e
          end
        end
        finish_reclaim_checked!(entry)
      end

      def mark_produced_retry(job_id, fence)
        err = nil
        5.times do |i|
          begin
            mark_produced(job_id, fence)
            return
          rescue StandardError => e
            err = e
            sleep((i + 1) * 0.02)
          end
        end
        raise err if err
      end

      def finish_reclaim_checked!(entry)
        n = finish_reclaim(entry)
        return if n == 1

        raise "workset: finish reclaim noop job_id=#{entry.job_id} (fence mismatch or gone)"
      end

      def job_key(job_id)
        "#{JOB_KEY_PREFIX}#{job_id}"
      end

      def by_consumer_key(id)
        "#{BY_CONSUMER_PREFIX}#{id}"
      end

      def live_key(id)
        "#{LIVE_CONSUMER_PREFIX}#{id}"
      end

      def reclaiming_key(job_id)
        "#{RECLAIMING_PREFIX}#{job_id}"
      end

      def produced_key(job_id)
        "#{PRODUCED_PREFIX}#{job_id}"
      end

      def resolve_grace(seconds)
        return DEFAULT_ORPHAN_GRACE if seconds.nil?

        s = seconds.to_i
        return 0 if s.negative?
        return DEFAULT_ORPHAN_GRACE if s.zero?

        s
      end

      def positive_or(value, default)
        n = value.to_i
        n.positive? ? n : default
      end

      def dump_entry(entry)
        payload = entry.payload.to_s.b
        encoding = entry.encoding.to_s
        if encoding.empty? && payload.bytesize >= COMPRESS_MIN_BYTES
          payload = gzip_deflate(payload)
          encoding = ENCODING_GZIP
        end
        h = {
          "job_id"          => entry.job_id,
          # Match Go encoding/json []byte → base64 so daemon reclaim can decode.
          "payload"         => Base64.strict_encode64(payload),
          "topic"           => entry.topic,
          "partition"       => entry.partition,
          "offset"          => entry.offset,
          "consumer_id"     => entry.consumer_id,
          "fence"           => entry.fence,
          "claimed_at"      => entry.claimed_at,
          "claimed_at_unix" => entry.claimed_at_unix,
          "runtime"         => entry.runtime
        }
        h["encoding"] = encoding unless encoding.empty?
        Oj.dump(h, mode: :compat)
      end

      def parse_entry(raw)
        h = Oj.load(raw)
        return nil unless h.is_a?(Hash)

        payload =
          begin
            Base64.strict_decode64(h["payload"].to_s)
          rescue ArgumentError
            h["payload"].to_s.b
          end

        Entry.new(
          job_id:          h["job_id"].to_s,
          payload:         payload,
          encoding:        h["encoding"].to_s,
          topic:           h["topic"].to_s,
          partition:       h["partition"].to_i,
          offset:          h["offset"].to_i,
          consumer_id:     h["consumer_id"].to_s,
          fence:           h["fence"].to_s,
          claimed_at:      h["claimed_at"].to_s,
          claimed_at_unix: h["claimed_at_unix"].to_i,
          runtime:         h["runtime"].to_s
        )
      end

      def gzip_deflate(bytes)
        io = StringIO.new
        gz = Zlib::GzipWriter.new(io)
        gz.write(bytes)
        gz.close
        io.string.b
      end

      def redis_with
        unless KafkaBatch.config.redis_configured?
          raise KafkaBatch::ConfigurationError, "Redis is not configured (required for SuperFetch workset)"
        end

        pool.with { |conn| yield conn }
      end

      def pool
        @pool ||= ConnectionPool.new(size: KafkaBatch.config.redis_pool_size, timeout: 5) do
          KafkaBatch::RedisClient.new(KafkaBatch.config, timeout: 2, reconnect_attempts: 1) ||
            raise(KafkaBatch::ConfigurationError, "Redis is not configured")
        end
      end
    end

    class << self
      def store
        @mutex ||= Mutex.new
        @mutex.synchronize { @store ||= Store.new }
      end

      def payload_for_reclaim(entry)
        raw = entry.payload.to_s.b
        return raw unless entry.encoding.to_s == ENCODING_GZIP

        gz = Zlib::GzipReader.new(StringIO.new(raw))
        begin
          gz.read.b
        ensure
          gz.close
        end
      end

      def mark_reclaim_payload(raw)
        data = Oj.load(raw)
        raise ArgumentError, "workset: reclaim payload is not a JSON object" unless data.is_a?(Hash)

        data["_reclaim"] = true
        Oj.dump(data, mode: :compat)
      end

      def reset!
        ReclaimScheduler.stop! if defined?(ReclaimScheduler)
        @mutex ||= Mutex.new
        @mutex.synchronize do
          @store&.close
          @store = nil
        end
      end
    end
  end
end
