package client

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
)

func (c *Client) validateManifest() error {
	if len(c.manifest.Handlers) == 0 && c.cfg.ManifestPath != "" {
		return ConfigurationError{Message: "handler manifest has no handlers"}
	}
	defaultTopic := c.cfg.defaultJobsTopic()
	if err := c.manifest.Validate(defaultTopic); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if err := c.manifest.ValidateTopicRuntimeExclusivity(defaultTopic); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	return nil
}

func pingRedis(ctx context.Context, rdb *redis.Client) error {
	if rdb == nil {
		return ConfigurationError{Message: "redis client not configured"}
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

// LookupHandler resolves a manifest entry (exported for callers building custom flows).
func (c *Client) LookupHandler(jobType string) (config.HandlerEntry, error) {
	return c.lookupHandler(jobType)
}
