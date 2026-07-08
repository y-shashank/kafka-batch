package priority

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
)

// Mode is strict or weighted interleave.
type Mode string

const (
	ModeStrict   Mode = "strict"
	ModeWeighted Mode = "weighted"
)

// Config is one priority YAML group (mirrors Ruby Priority::Config).
type Config struct {
	Path                 string
	ConsumerGroupSuffix  string
	ConsumerGroup        string
	Mode                 Mode
	Topics               []string
	WeightedInterleave   int
}

// TopicSpec is runtime metadata for one topic inside a priority group.
type TopicSpec struct {
	Topic              string
	Rank               int
	Mode               Mode
	HigherTopics       []string
	ConsumerGroup      string
	WeightedInterleave int
}

// Load reads one priority YAML file.
func Load(path string, cfg config.Daemon, defaultJobsTopic string) (Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return Config{}, fmt.Errorf("priority config path is empty")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("priority config not found: %s: %w", path, err)
	}
	var doc struct {
		ConsumerGroupSuffix string   `yaml:"consumer_group_suffix"`
		Mode                string   `yaml:"mode"`
		Topics              []string `yaml:"topics"`
		WeightedInterleave  int      `yaml:"weighted_interleave"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return Config{}, fmt.Errorf("priority config %s: %w", path, err)
	}
	suffix := strings.TrimSpace(doc.ConsumerGroupSuffix)
	if suffix == "" {
		return Config{}, fmt.Errorf("priority config %s requires consumer_group_suffix", path)
	}
	mode := Mode(strings.ToLower(strings.TrimSpace(doc.Mode)))
	if mode == "" {
		mode = ModeWeighted
	}
	if mode != ModeStrict && mode != ModeWeighted {
		return Config{}, fmt.Errorf("priority config %s mode must be strict or weighted (got %q)", path, doc.Mode)
	}
	topics := make([]string, 0, len(doc.Topics))
	seen := map[string]struct{}{}
	for _, t := range doc.Topics {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		t = resolveTopic(cfg, t)
		if _, dup := seen[t]; dup {
			return Config{}, fmt.Errorf("priority config %s lists duplicate topic %q", path, t)
		}
		seen[t] = struct{}{}
		topics = append(topics, t)
	}
	if len(topics) == 0 {
		return Config{}, fmt.Errorf("priority config %s requires a non-empty topics list", path)
	}
	jobsTopic := defaultJobsTopic
	if jobsTopic == "" {
		jobsTopic = resolveTopic(cfg, "kafka_batch.jobs")
	}
	for _, t := range topics {
		if t == jobsTopic {
			return Config{}, fmt.Errorf("priority config %s must not include default jobs topic %q", path, jobsTopic)
		}
	}
	interleave := doc.WeightedInterleave
	if interleave < 1 {
		interleave = cfg.PriorityWeightedInterleave
	}
	if interleave < 1 {
		interleave = 4
	}
	return Config{
		Path:                path,
		ConsumerGroupSuffix: suffix,
		ConsumerGroup:       cfg.ConsumerGroup + "-" + suffix,
		Mode:                mode,
		Topics:              topics,
		WeightedInterleave:  interleave,
	}, nil
}

func (c Config) TopicSpecs() []TopicSpec {
	specs := make([]TopicSpec, len(c.Topics))
	for i, topic := range c.Topics {
		higher := append([]string(nil), c.Topics[:i]...)
		specs[i] = TopicSpec{
			Topic:              topic,
			Rank:               i,
			Mode:               c.Mode,
			HigherTopics:       higher,
			ConsumerGroup:      c.ConsumerGroup,
			WeightedInterleave: c.WeightedInterleave,
		}
	}
	return specs
}

func resolveTopic(cfg config.Daemon, base string) string {
	if cfg.TopicPrefix == "" {
		return base
	}
	prefix := cfg.TopicPrefix + "."
	if strings.HasPrefix(base, prefix) {
		return base
	}
	return prefix + base
}
