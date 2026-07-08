package schedule

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
)

func TestDaemonRouterPlainTopic(t *testing.T) {
	r := DaemonRouter{
		Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"integration.go_scheduled": {Runtime: "go", Topic: "worker.topic"},
		}},
		Default: "fallback",
	}
	route, err := r.Route(map[string]interface{}{
		"job_type": "integration.go_scheduled",
		"job_id":   "j1",
	})
	if err != nil || route.Topic != "worker.topic" || route.Key != "j1" {
		t.Fatalf("route %+v err=%v", route, err)
	}
}

func TestDaemonRouterFairTimeIngest(t *testing.T) {
	r := DaemonRouter{
		Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"fair.job": {Runtime: "go", Topic: "ignored", FairnessType: "time"},
		}},
		Cfg: config.Daemon{FairnessTimeIngest: "fair.ingest"},
	}
	route, err := r.Route(map[string]interface{}{
		"job_type":  "fair.job",
		"job_id":    "j1",
		"tenant_id": "acme",
	})
	if err != nil || route.Topic != "fair.ingest" || route.Key != "acme" {
		t.Fatalf("route %+v err=%v", route, err)
	}
}

func TestPollerTickHappyPath(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := NewRedisStore(rdb, 100)
	ctx := context.Background()
	now := time.Unix(100, 0)
	if err := store.Schedule(ctx, "j1", now.Add(-time.Second), 0, 5); err != nil {
		t.Fatal(err)
	}

	payload := []byte(`{"job_id":"j1","job_type":"integration.go_scheduled","worker_class":"go:integration.go_scheduled"}`)
	reader := &stubReader{found: map[string][]byte{"0:5": payload}}
	var produced struct{ topic, key string }
	poller := &Poller{
		Cfg:    config.Daemon{SkipCancelledJobs: true, ScheduleLeaseSeconds: 60, ScheduleBatchSize: 10},
		Store:  store,
		Reader: reader,
		Producer: producerFunc(func(_ context.Context, topic, key string, _ []byte) error {
			produced.topic, produced.key = topic, key
			return nil
		}),
		Router: DaemonRouter{
			Manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
				"integration.go_scheduled": {Runtime: "go", Topic: "worker.topic"},
			}},
		},
		Now: func() time.Time { return now },
	}
	n, err := poller.Tick(ctx)
	if err != nil || n != 1 {
		t.Fatalf("tick n=%d err=%v", n, err)
	}
	if produced.topic != "worker.topic" || produced.key != "j1" {
		t.Fatalf("produced %+v", produced)
	}
}

type stubReader struct {
	found map[string][]byte
	lost  []string
}

func (s *stubReader) Read(ctx context.Context, byPartition map[int32][]int64) (ReadResult, error) {
	return ReadResult{Found: s.found, Lost: s.lost}, nil
}

type producerFunc func(ctx context.Context, topic, key string, payload []byte) error

func (f producerFunc) Produce(ctx context.Context, topic, key string, payload []byte) error {
	return f(ctx, topic, key, payload)
}
