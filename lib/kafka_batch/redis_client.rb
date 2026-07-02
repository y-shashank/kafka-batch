require "redis"

module KafkaBatch
  # Normalizes config.redis_url (String) or config.redis (Hash) into redis-rb
  # client options. Accepts Rails-style hashes, e.g.
  #   { host: "localhost", port: 6379, db: 0, namespace: "development" }
  module RedisClient
    CLIENT_OPTION_KEYS = %i[
      host port path db password username
      timeout connect_timeout read_timeout write_timeout
      ssl ssl_params driver id
    ].freeze

    # Rails / cache-store metadata — not passed to redis-rb directly.
    RAILS_META_KEYS = %i[namespace location].freeze

    class << self
      # @return [Hash] options suitable for Redis.new (may be empty)
      def connection_options(config)
        hash = config.redis
        if hash.is_a?(Hash) && !hash.empty?
          from_hash(hash)[:options]
        elsif (url = config.redis_url_raw) && !url.to_s.empty?
          { url: url.to_s }
        else
          {}
        end
      end

      # @return [Redis, nil]
      def new(config, **extra)
        parsed = config.redis.is_a?(Hash) && !config.redis.empty? ? from_hash(config.redis) : nil
        opts   = parsed ? parsed[:options] : connection_options(config)
        return nil if opts.empty?

        client = Redis.new(**opts, **extra)
        apply_namespace(client, parsed&.dig(:namespace))
      end

      # Canonical URL for logging / the dashboard (no password).
      def url_for(hash)
        h = hash.transform_keys(&:to_sym)
        explicit = h[:url]
        return explicit.to_s if explicit && !explicit.to_s.empty?

        id = h[:id]
        return id.to_s if id.to_s.start_with?("redis://")

        host = h[:host] || "localhost"
        port = h[:port] || 6379
        db   = h.key?(:db) ? h[:db] : 0
        "redis://#{host}:#{port}/#{db}"
      end

      private

      def from_hash(hash)
        h = hash.transform_keys(&:to_sym)
        namespace = h[:namespace]

        url = h[:url]
        url = h[:id] if url.to_s.empty? && h[:id].to_s.start_with?("redis://")

        options = {}
        if url && !url.to_s.empty?
          options[:url] = url.to_s
          %i[password username timeout connect_timeout read_timeout write_timeout ssl ssl_params driver].each do |key|
            options[key] = h[key] if h.key?(key)
          end
        else
          options[:host] = (h[:host] || "localhost").to_s
          options[:port] = h[:port] || 6379
          options[:db]   = h.fetch(:db, 0)
          options[:password] = h[:password] if h[:password]
          options[:username] = h[:username] if h[:username]

          CLIENT_OPTION_KEYS.each do |key|
            next if key == :id && h[key].to_s.start_with?("redis://")
            options[key] = h[key] if h.key?(key)
          end
        end

        { options: options, namespace: namespace }
      end

      def apply_namespace(client, namespace)
        ns = namespace.to_s
        return client if ns.empty?

        if defined?(Redis::Namespace)
          Redis::Namespace.new(ns, redis: client)
        else
          KafkaBatch.logger&.warn(
            "[KafkaBatch] config.redis namespace=#{ns.inspect} ignored — " \
            "add the redis-namespace gem to enable key prefixing"
          )
          client
        end
      end
    end
  end
end
