package priority

import (
	"fmt"
	"strings"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
)

// Registry holds loaded priority groups.
type Registry struct {
	Configs []Config
}

// LoadRegistry loads all priority YAML paths. explicitFlatTopics is jobs_topics
// from daemon YAML only (not manifest-derived); overlap with those is rejected.
func LoadRegistry(paths []string, cfg config.Daemon, explicitFlatTopics []string) (Registry, error) {
	flat := map[string]struct{}{}
	defaultJobs := resolveTopic(cfg, "kafka_batch.jobs")
	flat[defaultJobs] = struct{}{}
	for _, t := range explicitFlatTopics {
		flat[t] = struct{}{}
	}

	var out Registry
	reserved := map[string]struct{}{}
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		pc, err := Load(path, cfg, defaultJobs)
		if err != nil {
			return Registry{}, err
		}
		for _, t := range pc.Topics {
			if _, ok := flat[t]; ok {
				return Registry{}, fmt.Errorf(
					"priority topic %q is also in flat jobs_topics — would double-process", t)
			}
			if _, dup := reserved[t]; dup {
				return Registry{}, fmt.Errorf("priority topic %q appears in multiple priority YAML files", t)
			}
			reserved[t] = struct{}{}
		}
		out.Configs = append(out.Configs, pc)
	}
	return out, nil
}

// AllTopics returns every topic declared across groups.
func (r Registry) AllTopics() []string {
	seen := map[string]struct{}{}
	var out []string
	for _, c := range r.Configs {
		for _, t := range c.Topics {
			if _, ok := seen[t]; ok {
				continue
			}
			seen[t] = struct{}{}
			out = append(out, t)
		}
	}
	return out
}
