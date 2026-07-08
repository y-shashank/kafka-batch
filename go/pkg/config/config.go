package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Daemon holds runtime configuration for kbatch daemon.
type Daemon struct {
	Brokers            []string
	TopicPrefix        string
	ConsumerGroup      string
	JobsTopics         []string
	EventsTopic        string
	CallbacksTopic     string
	DeadLetterTopic    string
	RetryTopicBase     string
	RetryTiers         map[string]int // seconds
	RetryProgression   []string
	RetryJitter        float64
	RetryMaxPause      time.Duration
	MaxRetries         int
	CompleteAfter      int
	EventEmitRetries   int
	EventEmitBackoff   time.Duration
	RedisURL           string
	BatchTTL           time.Duration
	HandlerManifest    string
	SkipCancelledJobs  bool
	NodeID             string
}

func DefaultDaemon() Daemon {
	return Daemon{
		Brokers:           []string{"localhost:9092"},
		ConsumerGroup:     "kafka-batch",
		EventsTopic:       "kafka_batch.events",
		CallbacksTopic:    "kafka_batch.callbacks",
		DeadLetterTopic:   "kafka_batch.dead_letter",
		RetryTopicBase:    "kafka_batch.jobs.retry",
		RetryTiers:        map[string]int{"short": 30, "medium": 420, "large": 1200},
		RetryProgression:  []string{"short", "medium", "large"},
		RetryJitter:       0.1,
		RetryMaxPause:     30 * time.Second,
		MaxRetries:        3,
		CompleteAfter:     3,
		EventEmitRetries:  3,
		EventEmitBackoff:  time.Second,
		RedisURL:          "redis://localhost:6379/0",
		BatchTTL:          7 * 24 * time.Hour,
		SkipCancelledJobs: true,
		NodeID:            hostname(),
	}
}

func LoadDaemon(path string) (Daemon, error) {
	cfg := DefaultDaemon()
	if path == "" {
		applyEnv(&cfg)
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	var doc struct {
		Brokers         []string          `yaml:"brokers"`
		TopicPrefix     string            `yaml:"topic_prefix"`
		ConsumerGroup   string            `yaml:"consumer_group"`
		JobsTopics      []string          `yaml:"jobs_topics"`
		EventsTopic     string            `yaml:"events_topic"`
		CallbacksTopic  string            `yaml:"callbacks_topic"`
		DeadLetterTopic string            `yaml:"dead_letter_topic"`
		RetryTopic      string            `yaml:"retry_topic"`
		RetryTiers      map[string]int    `yaml:"retry_tiers"`
		RedisURL        string            `yaml:"redis_url"`
		HandlerManifest string            `yaml:"handler_manifest"`
		MaxRetries         int            `yaml:"max_retries"`
		CompleteAfter      int            `yaml:"complete_after_retries"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return cfg, err
	}
	if len(doc.Brokers) > 0 {
		cfg.Brokers = doc.Brokers
	}
	if doc.TopicPrefix != "" {
		cfg.TopicPrefix = doc.TopicPrefix
	}
	if doc.ConsumerGroup != "" {
		cfg.ConsumerGroup = doc.ConsumerGroup
	}
	if len(doc.JobsTopics) > 0 {
		cfg.JobsTopics = doc.JobsTopics
	}
	if doc.EventsTopic != "" {
		cfg.EventsTopic = doc.EventsTopic
	}
	if doc.CallbacksTopic != "" {
		cfg.CallbacksTopic = doc.CallbacksTopic
	}
	if doc.DeadLetterTopic != "" {
		cfg.DeadLetterTopic = doc.DeadLetterTopic
	}
	if doc.RetryTopic != "" {
		cfg.RetryTopicBase = doc.RetryTopic
	}
	if doc.RetryTiers != nil {
		cfg.RetryTiers = doc.RetryTiers
	}
	if doc.RedisURL != "" {
		cfg.RedisURL = doc.RedisURL
	}
	if doc.HandlerManifest != "" {
		cfg.HandlerManifest = doc.HandlerManifest
	}
	if doc.MaxRetries > 0 {
		cfg.MaxRetries = doc.MaxRetries
	}
	if doc.CompleteAfter > 0 {
		cfg.CompleteAfter = doc.CompleteAfter
	}
	applyEnv(&cfg)
	cfg.prefixTopics()
	return cfg, nil
}

func applyEnv(cfg *Daemon) {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKA_PREFIX"); v != "" {
		cfg.TopicPrefix = strings.TrimSpace(v)
	}
	if v := os.Getenv("REDIS_URL"); v != "" {
		cfg.RedisURL = v
	}
	if v := os.Getenv("KAFKA_BATCH_HANDLER_MANIFEST"); v != "" {
		cfg.HandlerManifest = v
	}
	cfg.prefixTopics()
}

func (c *Daemon) prefixTopics() {
	if c.TopicPrefix == "" {
		return
	}
	p := c.TopicPrefix + "."
	c.EventsTopic = prefixName(p, c.EventsTopic)
	c.CallbacksTopic = prefixName(p, c.CallbacksTopic)
	c.DeadLetterTopic = prefixName(p, c.DeadLetterTopic)
	c.RetryTopicBase = prefixName(p, c.RetryTopicBase)
	for i, t := range c.JobsTopics {
		c.JobsTopics[i] = prefixName(p, t)
	}
	if !strings.HasPrefix(c.ConsumerGroup, c.TopicPrefix) {
		c.ConsumerGroup = c.TopicPrefix + "." + c.ConsumerGroup
	}
}

func prefixName(prefix, name string) string {
	if strings.HasPrefix(name, prefix) {
		return name
	}
	return prefix + name
}

func (c Daemon) RetryTopic(tier string) string {
	return c.RetryTopicBase + "." + tier
}

func (c Daemon) RetryTopics() []string {
	out := make([]string, 0, len(c.RetryTiers))
	for tier := range c.RetryTiers {
		out = append(out, c.RetryTopic(tier))
	}
	return out
}

func (c Daemon) RetryTierFor(nextAttempt int, workerTier string) string {
	if workerTier != "" {
		if _, ok := c.RetryTiers[workerTier]; ok {
			return workerTier
		}
	}
	idx := nextAttempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(c.RetryProgression) {
		idx = len(c.RetryProgression) - 1
	}
	return c.RetryProgression[idx]
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "kbatch-daemon"
	}
	return h
}

// Manifest loads handler definitions (topic routing for Go handlers).
type Manifest struct {
	Handlers map[string]HandlerEntry `yaml:"handlers"`
}

type HandlerEntry struct {
	Runtime          string `yaml:"runtime"`
	Topic            string `yaml:"topic"`
	ApplyTopicPrefix bool   `yaml:"apply_topic_prefix"`
	MaxRetries       int    `yaml:"max_retries"`
}

func LoadManifest(path, topicPrefix string) (Manifest, error) {
	var m Manifest
	if path == "" {
		return m, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err := yaml.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	for name, h := range m.Handlers {
		if h.Topic != "" && h.ApplyTopicPrefix && topicPrefix != "" && !strings.HasPrefix(h.Topic, topicPrefix+".") {
			entry := m.Handlers[name]
			entry.Topic = topicPrefix + "." + h.Topic
			m.Handlers[name] = entry
		}
	}
	return m, nil
}

func (m Manifest) JobTopics(defaultTopic string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, h := range m.Handlers {
		if h.Runtime != "go" {
			continue
		}
		t := h.Topic
		if t == "" {
			t = defaultTopic
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func (m Manifest) Validate() error {
	for jobType, h := range m.Handlers {
		if h.Runtime == "go" {
			if _, ok := lookupRegistered(jobType); !ok {
				return fmt.Errorf("handler %q not registered in Go (missing kbatch.Register)", jobType)
			}
		}
	}
	return nil
}

// lookupRegistered is set by manifest package init from kbatch package.
var lookupRegistered = func(string) (struct{}, bool) { return struct{}{}, true }

func SetHandlerLookup(fn func(string) bool) {
	lookupRegistered = func(s string) (struct{}, bool) {
		if fn(s) {
			return struct{}{}, true
		}
		return struct{}{}, false
	}
}
