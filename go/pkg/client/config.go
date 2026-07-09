package client

import (
	"time"
)

// Config holds producer-client settings (mirrors Ruby KafkaBatch.config produce surface).
type Config struct {
	Brokers      []string
	TopicPrefix  string
	RedisURL     string
	ManifestPath string

	JobsTopic      string
	ScheduledTopic string
	CallbacksTopic string

	BatchTTL time.Duration

	MaxRetries           int
	CompleteAfterRetries int

	UniqEnabled     bool
	UniqLockTTL     time.Duration
	UniqOnDuplicate string // "skip" or "raise"

	ScheduleIndexWriteRetries int
	ScheduleIndexWriteBackoff time.Duration
	MaxScheduleHorizon        time.Duration
	ProduceChunkSize          int
	AllIndexMaxSize           int

	FairnessTimeIngest       string
	FairnessThroughputIngest string
}

// DefaultConfig returns sensible local defaults.
func DefaultConfig() Config {
	return Config{
		Brokers:                   []string{"localhost:9092"},
		RedisURL:                  "redis://localhost:6379/0",
		JobsTopic:                 "kafka_batch.jobs",
		ScheduledTopic:            "kafka_batch.scheduled",
		CallbacksTopic:            "kafka_batch.callbacks",
		BatchTTL:                  7 * 24 * time.Hour,
		MaxRetries:                25,
		CompleteAfterRetries:      3,
		UniqEnabled:               true,
		UniqLockTTL:               7 * 24 * time.Hour,
		UniqOnDuplicate:           "skip",
		ScheduleIndexWriteRetries: 3,
		ScheduleIndexWriteBackoff: time.Second,
		MaxScheduleHorizon:        30 * 24 * time.Hour,
		ProduceChunkSize:          500,
		FairnessTimeIngest:        "kafka_batch.fair_time_ingest",
		FairnessThroughputIngest:  "kafka_batch.fair_throughput_ingest",
	}
}

func (c Config) resolveTopic(base string) string {
	if c.TopicPrefix == "" || base == "" {
		return base
	}
	prefix := c.TopicPrefix + "."
	if len(base) >= len(prefix) && base[:len(prefix)] == prefix {
		return base
	}
	return prefix + base
}

func (c Config) defaultJobsTopic() string {
	return c.resolveTopic(c.JobsTopic)
}
