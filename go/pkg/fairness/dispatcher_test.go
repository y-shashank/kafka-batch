package fairness

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

func TestDispatcherDropsExpiredAtIngest(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sched := NewScheduler(rdb, Settings{Lane: LaneTime, ReadyWindow: 10, GlobalConcurrency: 4, LeaseTTL: 300})

	expired := false
	disp := &Dispatcher{
		Lane:      LaneTime,
		Scheduler: sched,
		Now:       func() time.Time { return time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC) },
		OnExpired: func(ctx context.Context, raw []byte, src protocol.SourceCoords) error {
			expired = true
			return nil
		},
	}
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "valid_till": "2000-01-01T00:00:00Z", "tenant_id": "t1",
	})
	out, err := disp.Process(context.Background(), raw, protocol.SourceCoords{Topic: "ingest", Partition: 0, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !out.CommitOffset || out.Enqueued {
		t.Fatalf("out %+v", out)
	}
	if !expired {
		t.Fatal("expected OnExpired")
	}
}

func TestDispatcherStampsSourceBeforeEnqueue(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	sched := NewScheduler(rdb, Settings{Lane: LaneTime, ReadyWindow: 10, GlobalConcurrency: 4, LeaseTTL: 300})
	disp := &Dispatcher{Lane: LaneTime, Scheduler: sched}
	raw, _ := json.Marshal(map[string]interface{}{"job_id": "j1", "tenant_id": "t1"})
	out, err := disp.Process(context.Background(), raw, protocol.SourceCoords{Topic: "ingest", Partition: 2, Offset: 9})
	if err != nil || !out.Enqueued {
		t.Fatalf("enqueue out=%+v err=%v", out, err)
	}
}
