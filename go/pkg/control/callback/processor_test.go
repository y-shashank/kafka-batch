package callback

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
)

type spyInvoker struct {
	calls []protocol.CallbackMessage
}

func (s *spyInvoker) Invoke(_ context.Context, cb protocol.CallbackMessage) error {
	s.calls = append(s.calls, cb)
	return nil
}

type spyDLT struct {
	calls int32
}

func (s *spyDLT) ProduceDLT(_ context.Context, _ string, _ []byte) error {
	atomic.AddInt32(&s.calls, 1)
	return nil
}

func TestProcessClaimsAndInvokes(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "cb-1"
	now := time.Now().UTC().Format(time.RFC3339)
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID, "status", "success", "total_jobs", "1",
		"completed_count", "1", "failed_count", "0", "locked_at", now,
	)
	mr.ZAdd("kafka_batch:index:done", 1, batchID)

	inv := &spyInvoker{}
	p := &Processor{Store: st, Invoker: inv, NodeID: "node-1"}

	raw, _ := json.Marshal(protocol.CallbackMessage{
		BatchID: batchID, Outcome: "success", TotalJobs: 1, CompletedCount: 1,
	})
	out, err := p.Process(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if !out.CommitOffset {
		t.Fatal("expected commit")
	}
	if len(inv.calls) != 1 || inv.calls[0].BatchID != batchID {
		t.Fatalf("invoker calls %+v", inv.calls)
	}
	dispatched, err := st.CallbackDispatched(context.Background(), batchID)
	if err != nil || !dispatched {
		t.Fatalf("dispatched=%v err=%v", dispatched, err)
	}
}

func TestProcessSkipsWhenAlreadyDispatched(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "cb-dup"
	now := time.Now().UTC().Format(time.RFC3339)
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID, "status", "success", "callback_dispatched_at", now,
	)

	inv := &spyInvoker{}
	p := &Processor{Store: st, Invoker: inv, NodeID: "node-1"}
	raw, _ := json.Marshal(protocol.CallbackMessage{BatchID: batchID, Outcome: "success"})
	_, err := p.Process(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.calls) != 0 {
		t.Fatalf("expected no invoke, got %+v", inv.calls)
	}
}

func TestProcessMalformedCallbackJSON(t *testing.T) {
	var failed, dlt int32
	instrument.SetHandler(func(event string, _ map[string]interface{}, _ float64) {
		switch event {
		case "callback.failed":
			atomic.AddInt32(&failed, 1)
		case "dlt.published":
			atomic.AddInt32(&dlt, 1)
		}
	})
	defer instrument.SetHandler(nil)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)
	dltSpy := &spyDLT{}
	p := &Processor{Store: st, Invoker: LogInvoker{}, DLT: dltSpy, NodeID: "n1"}

	out, err := p.Process(context.Background(), []byte("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	if !out.CommitOffset {
		t.Fatal("expected commit")
	}
	if failed != 1 || dlt != 1 || dltSpy.calls != 1 {
		t.Fatalf("failed=%d dlt_events=%d dlt_calls=%d", failed, dlt, dltSpy.calls)
	}
}
