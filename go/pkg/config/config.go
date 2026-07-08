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
	SchedulePollerEnabled bool
	ScheduledTopic        string
	SchedulePollInterval  time.Duration
	ScheduleLeaseSeconds  int
	ScheduleBatchSize     int
	ScheduleReclaimEvery  time.Duration
	SchedulePollMaxInterval time.Duration
	SchedulePollJitter    float64
	ScheduleStore         string
	ScheduleMySQLDSN      string
	PriorityConfigPaths   []string
	PriorityLagCheckInterval time.Duration
	PriorityWeightedInterleave int
	ConsumptionControlRefreshInterval time.Duration
	RubyCallbackSocket    string
	RubyWorkerSocket      string
	RubyWorkerTimeout     time.Duration
	FairnessEnabled       bool
	FairnessTimeIngest    string
	FairnessTimeReady     string
	FairnessThroughputIngest string
	FairnessThroughputReady  string
	FairnessReadyWindow   int
	FairnessGlobalConcurrency int
	FairnessMaxInflightPerTenant int
	FairnessLeaseTTL          float64
	FairnessDefaultWeight     float64
	FairnessWeightedConcurrency bool
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
		ScheduledTopic:    "kafka_batch.scheduled",
		SchedulePollInterval: 5 * time.Second,
		ScheduleLeaseSeconds: 60,
		ScheduleBatchSize:    100,
		ScheduleReclaimEvery: 30 * time.Second,
		SchedulePollMaxInterval: 60 * time.Second,
		PriorityLagCheckInterval: 2 * time.Second,
		PriorityWeightedInterleave: 4,
		ConsumptionControlRefreshInterval: 30 * time.Second,
		FairnessTimeIngest:   "kafka_batch.fair_time_ingest",
		FairnessTimeReady:    "kafka_batch.fair_time_ready",
		FairnessThroughputIngest: "kafka_batch.fair_throughput_ingest",
		FairnessThroughputReady:  "kafka_batch.fair_throughput_ready",
		FairnessReadyWindow:  100,
		FairnessGlobalConcurrency: 50,
		FairnessLeaseTTL:          1800,
		FairnessDefaultWeight:     1.0,
		FairnessWeightedConcurrency: true,
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
		SchedulePollerEnabled bool          `yaml:"schedule_poller_enabled"`
		ScheduledTopic        string        `yaml:"scheduled_topic"`
		ScheduleLeaseSeconds  int           `yaml:"schedule_lease_seconds"`
		ScheduleBatchSize     int           `yaml:"schedule_batch_size"`
		SchedulePollIntervalSec float64     `yaml:"schedule_poll_interval"`
		ScheduleReclaimIntervalSec float64  `yaml:"schedule_reclaim_interval"`
		SchedulePollMaxIntervalSec float64  `yaml:"schedule_poll_max_interval"`
		SchedulePollJitter    float64       `yaml:"schedule_poll_jitter"`
		ScheduleStore         string        `yaml:"schedule_store"`
		ScheduleMySQLDSN      string        `yaml:"schedule_mysql_dsn"`
		PriorityConfigPaths   []string      `yaml:"priority_config_paths"`
		PriorityLagCheckIntervalSec float64 `yaml:"priority_lag_check_interval"`
		PriorityWeightedInterleave int      `yaml:"priority_weighted_interleave"`
		ConsumptionControlRefreshIntervalSec float64 `yaml:"consumption_control_refresh_interval"`
		RubyCallbackSocket    string        `yaml:"ruby_callback_socket"`
		RubyWorkerSocket      string        `yaml:"ruby_worker_socket"`
		RubyWorkerTimeoutSec  float64       `yaml:"ruby_worker_timeout"`
		FairnessEnabled       bool          `yaml:"fairness_enabled"`
		FairnessTimeIngest    string        `yaml:"fairness_time_ingest"`
		FairnessTimeReady     string        `yaml:"fairness_time_ready"`
		FairnessThroughputIngest string     `yaml:"fairness_throughput_ingest"`
		FairnessThroughputReady  string     `yaml:"fairness_throughput_ready"`
		FairnessReadyWindow   int           `yaml:"fairness_ready_window"`
		FairnessGlobalConcurrency int       `yaml:"fairness_global_concurrency"`
		FairnessMaxInflightPerTenant int    `yaml:"fairness_max_inflight_per_tenant"`
		FairnessLeaseTTL          float64    `yaml:"fairness_lease_ttl"`
		FairnessDefaultWeight     float64    `yaml:"fairness_default_weight"`
		FairnessWeightedConcurrency bool     `yaml:"fairness_weighted_concurrency"`
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
	if doc.SchedulePollerEnabled {
		cfg.SchedulePollerEnabled = true
	}
	if doc.ScheduledTopic != "" {
		cfg.ScheduledTopic = doc.ScheduledTopic
	}
	if doc.ScheduleLeaseSeconds > 0 {
		cfg.ScheduleLeaseSeconds = doc.ScheduleLeaseSeconds
	}
	if doc.ScheduleBatchSize > 0 {
		cfg.ScheduleBatchSize = doc.ScheduleBatchSize
	}
	if doc.SchedulePollIntervalSec > 0 {
		cfg.SchedulePollInterval = time.Duration(doc.SchedulePollIntervalSec * float64(time.Second))
	}
	if doc.ScheduleReclaimIntervalSec > 0 {
		cfg.ScheduleReclaimEvery = time.Duration(doc.ScheduleReclaimIntervalSec * float64(time.Second))
	}
	if doc.SchedulePollMaxIntervalSec > 0 {
		cfg.SchedulePollMaxInterval = time.Duration(doc.SchedulePollMaxIntervalSec * float64(time.Second))
	}
	if doc.SchedulePollJitter > 0 {
		cfg.SchedulePollJitter = doc.SchedulePollJitter
	}
	if doc.ScheduleStore != "" {
		cfg.ScheduleStore = doc.ScheduleStore
	}
	if doc.ScheduleMySQLDSN != "" {
		cfg.ScheduleMySQLDSN = doc.ScheduleMySQLDSN
	}
	if len(doc.PriorityConfigPaths) > 0 {
		cfg.PriorityConfigPaths = doc.PriorityConfigPaths
	}
	if doc.PriorityLagCheckIntervalSec > 0 {
		cfg.PriorityLagCheckInterval = time.Duration(doc.PriorityLagCheckIntervalSec * float64(time.Second))
	}
	if doc.PriorityWeightedInterleave > 0 {
		cfg.PriorityWeightedInterleave = doc.PriorityWeightedInterleave
	}
	if doc.ConsumptionControlRefreshIntervalSec > 0 {
		cfg.ConsumptionControlRefreshInterval = time.Duration(doc.ConsumptionControlRefreshIntervalSec * float64(time.Second))
	}
	if doc.RubyCallbackSocket != "" {
		cfg.RubyCallbackSocket = doc.RubyCallbackSocket
	}
	if doc.RubyWorkerSocket != "" {
		cfg.RubyWorkerSocket = doc.RubyWorkerSocket
	}
	if doc.RubyWorkerTimeoutSec > 0 {
		cfg.RubyWorkerTimeout = time.Duration(doc.RubyWorkerTimeoutSec * float64(time.Second))
	}
	if doc.FairnessEnabled {
		cfg.FairnessEnabled = true
	}
	if doc.FairnessTimeIngest != "" {
		cfg.FairnessTimeIngest = doc.FairnessTimeIngest
	}
	if doc.FairnessTimeReady != "" {
		cfg.FairnessTimeReady = doc.FairnessTimeReady
	}
	if doc.FairnessThroughputIngest != "" {
		cfg.FairnessThroughputIngest = doc.FairnessThroughputIngest
	}
	if doc.FairnessThroughputReady != "" {
		cfg.FairnessThroughputReady = doc.FairnessThroughputReady
	}
	if doc.FairnessReadyWindow > 0 {
		cfg.FairnessReadyWindow = doc.FairnessReadyWindow
	}
	if doc.FairnessGlobalConcurrency > 0 {
		cfg.FairnessGlobalConcurrency = doc.FairnessGlobalConcurrency
	}
	if doc.FairnessMaxInflightPerTenant > 0 {
		cfg.FairnessMaxInflightPerTenant = doc.FairnessMaxInflightPerTenant
	}
	if doc.FairnessLeaseTTL > 0 {
		cfg.FairnessLeaseTTL = doc.FairnessLeaseTTL
	}
	if doc.FairnessDefaultWeight > 0 {
		cfg.FairnessDefaultWeight = doc.FairnessDefaultWeight
	}
	if doc.FairnessWeightedConcurrency {
		cfg.FairnessWeightedConcurrency = true
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
	if v := os.Getenv("KAFKA_BATCH_SCHEDULE_MYSQL_DSN"); v != "" {
		cfg.ScheduleMySQLDSN = v
	}
	if v := os.Getenv("KAFKA_BATCH_RUBY_CALLBACK_SOCKET"); v != "" {
		cfg.RubyCallbackSocket = v
	}
	if v := os.Getenv("KAFKA_BATCH_RUBY_WORKER_SOCKET"); v != "" {
		cfg.RubyWorkerSocket = v
	}
	if v := os.Getenv("KAFKA_BATCH_PRIORITY_CONFIG"); v != "" {
		cfg.PriorityConfigPaths = append(cfg.PriorityConfigPaths, strings.TrimSpace(v))
	}
	if v := os.Getenv("KAFKA_BATCH_PRIORITY_CONFIGS"); v != "" {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				cfg.PriorityConfigPaths = append(cfg.PriorityConfigPaths, p)
			}
		}
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
	c.ScheduledTopic = prefixName(p, c.ScheduledTopic)
	c.FairnessTimeIngest = prefixName(p, c.FairnessTimeIngest)
	c.FairnessTimeReady = prefixName(p, c.FairnessTimeReady)
	c.FairnessThroughputIngest = prefixName(p, c.FairnessThroughputIngest)
	c.FairnessThroughputReady = prefixName(p, c.FairnessThroughputReady)
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
	FairnessType     string `yaml:"fairness_type"`
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

func (m Manifest) JobTopics(defaultTopic string, includeRuby bool) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, h := range m.Handlers {
		rt := strings.ToLower(strings.TrimSpace(h.Runtime))
		if rt == "go" || (includeRuby && rt == "ruby") {
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
	}
	return out
}

func (m Manifest) HasRubyHandlers() bool {
	for _, h := range m.Handlers {
		if strings.EqualFold(h.Runtime, "ruby") {
			return true
		}
	}
	return false
}

func (m Manifest) Validate() error {
	for jobType, h := range m.Handlers {
		switch strings.ToLower(strings.TrimSpace(h.Runtime)) {
		case "go":
			if _, ok := lookupRegistered(jobType); !ok {
				return fmt.Errorf("handler %q not registered in Go (missing kbatch.Register)", jobType)
			}
		case "ruby":
			// Ruby handlers execute via worker-server socket (Phase 4).
		case "":
			return fmt.Errorf("handler %q missing runtime", jobType)
		default:
			return fmt.Errorf("handler %q has unsupported runtime %q", jobType, h.Runtime)
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
