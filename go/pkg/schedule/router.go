package schedule

import (
	"fmt"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
)

// Route describes where a scheduled job should be re-produced.
type Route struct {
	Topic     string
	Key       string
	Partition *int32 // nil = broker assigns by key
}

// DaemonRouter routes scheduled jobs (mirrors Batch.route_for_definition).
type DaemonRouter struct {
	Manifest config.Manifest
	Cfg      config.Daemon
	Default  string
}

func (r DaemonRouter) Route(payload map[string]interface{}) (Route, error) {
	jobType, _ := payload["job_type"].(string)
	jobID, _ := payload["job_id"].(string)
	tenantID, _ := payload["tenant_id"].(string)
	batchID, _ := payload["batch_id"].(string)
	worker, _ := payload["worker_class"].(string)

	var entry config.HandlerEntry
	var ok bool
	if jobType != "" {
		entry, ok = r.Manifest.Handlers[jobType]
	}
	if !ok && worker != "" {
		for jt, h := range r.Manifest.Handlers {
			if h.Runtime == "go" && ("go:"+jt) == worker {
				entry = h
				ok = true
				jobType = jt
				break
			}
		}
	}

	if ok && entry.FairnessType != "" {
		return r.fairRoute(entry.FairnessType, jobID, tenantID, batchID)
	}

	if ok && entry.Topic != "" {
		return Route{Topic: entry.Topic, Key: jobID}, nil
	}
	if r.Default != "" {
		return Route{Topic: r.Default, Key: jobID}, nil
	}
	return Route{}, fmt.Errorf("no route for job_type=%q worker_class=%q", jobType, worker)
}

func (r DaemonRouter) fairRoute(fairnessType, jobID, tenantID, batchID string) (Route, error) {
	switch fairnessType {
	case "time":
		key := tenantID
		if key == "" {
			key = batchID
		}
		if key == "" {
			key = jobID
		}
		return Route{Topic: r.Cfg.FairnessTimeIngest, Key: key}, nil
	case "throughput":
		return Route{}, fmt.Errorf("throughput fairness lane not implemented in Go daemon")
	default:
		return Route{}, fmt.Errorf("unknown fairness_type %q", fairnessType)
	}
}
