package metrics

import (
	"log"
	"sync"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
)

// Config wires instrumentation events to StatsD/Datadog or a custom handler.
type Config struct {
	Enabled    bool
	Prefix     string
	StatsDAddr string
	// Handler is used when StatsDAddr is empty (Ruby :proc adapter).
	Handler func(event string, payload map[string]interface{}, durationMs float64)
}

var (
	mu        sync.Mutex
	installed bool
)

// FromDaemon maps daemon YAML/env settings to metrics config.
func FromDaemon(cfg config.Daemon) Config {
	return Config{
		Enabled:    cfg.MetricsEnabled,
		Prefix:     cfg.MetricsPrefix,
		StatsDAddr: cfg.MetricsStatsDAddr,
	}
}

// Install registers the metrics bridge on instrument.Handler when enabled.
func Install(cfg Config) error {
	mu.Lock()
	defer mu.Unlock()
	if !cfg.Enabled {
		return nil
	}
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "kafka_batch"
	}

	var adapter emitter
	switch {
	case cfg.Handler != nil:
		adapter = procAdapter{handler: cfg.Handler}
	case cfg.StatsDAddr != "":
		client, err := newStatsDClient(cfg.StatsDAddr)
		if err != nil {
			return err
		}
		adapter = statsdAdapter{client: client, prefix: prefix}
	default:
		return errNoSink
	}

	instrument.SetHandler(func(event string, payload map[string]interface{}, durationMs float64) {
		adapter.emit(event, payload, durationMs)
	})
	installed = true
	log.Printf("[kbatch-metrics] installed prefix=%s statsd=%s proc=%v", prefix, cfg.StatsDAddr, cfg.Handler != nil)
	return nil
}

// Reset removes the metrics handler (tests).
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	if installed {
		instrument.SetHandler(nil)
		installed = false
	}
}

type emitter interface {
	emit(event string, payload map[string]interface{}, durationMs float64)
}

type procAdapter struct {
	handler func(string, map[string]interface{}, float64)
}

func (a procAdapter) emit(event string, payload map[string]interface{}, durationMs float64) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[kbatch-metrics] proc emit panic: %v", r)
		}
	}()
	a.handler(event, payload, durationMs)
}

type statsdAdapter struct {
	client statsDSink
	prefix string
}

type statsDSink interface {
	increment(name string, tags []string) error
	timing(name string, ms float64, tags []string) error
}

func (a statsdAdapter) emit(event string, payload map[string]interface{}, durationMs float64) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[kbatch-metrics] statsd emit panic: %v", r)
		}
	}()
	metric := a.prefix + "." + eventName(event)
	tags := tagsFor(payload)
	_ = a.client.increment(metric+".count", tags)
	_ = a.client.timing(metric+".duration", durationMs, tags)
}

func eventName(event string) string {
	out := make([]byte, 0, len(event))
	for i := 0; i < len(event); i++ {
		if event[i] == '.' {
			out = append(out, '_')
			continue
		}
		out = append(out, event[i])
	}
	return string(out)
}
