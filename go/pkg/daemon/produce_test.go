package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/control/retry"
	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

type countingProducer struct {
	failures int32
	calls    int32
}

func (p *countingProducer) Produce(_ context.Context, _, _ string, _ []byte) error {
	atomic.AddInt32(&p.calls, 1)
	if atomic.AddInt32(&p.failures, -1) >= 0 {
		return errors.New("kafka down")
	}
	return nil
}

func TestProduceEventWithRetryEmitsOnFailure(t *testing.T) {
	var emitRetried int32
	instrument.SetHandler(func(event string, _ map[string]interface{}, _ float64) {
		if event == "job.emit_retried" {
			atomic.AddInt32(&emitRetried, 1)
		}
	})
	defer instrument.SetHandler(nil)

	prod := &countingProducer{failures: 2}
	cfg := config.Daemon{EventEmitRetries: 3, EventEmitBackoff: 0, EventsTopic: "events"}
	ev := &protocol.EventMessage{
		BatchID: "b1", JobID: "j1", SrcTopic: "jobs", SrcPartition: 0,
	}
	if err := produceEventWithRetry(context.Background(), cfg, prod, ev); err != nil {
		t.Fatal(err)
	}
	if emitRetried != 2 {
		t.Fatalf("emit_retried=%d calls=%d", emitRetried, prod.calls)
	}
}

func TestApplyRetryOutcomeExpiredInstruments(t *testing.T) {
	var events []string
	instrument.SetHandler(func(event string, _ map[string]interface{}, _ float64) {
		events = append(events, event)
	})
	defer instrument.SetHandler(nil)

	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "batch_id": "b1", "worker_class": "W",
		"valid_till": "2000-01-01T00:00:00Z", "retry_to": "jobs",
	})
	p := &retry.Processor{Now: func() time.Time { return now }, MaxPause: time.Second}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "retry.short"})
	if err != nil {
		t.Fatal(err)
	}

	cfg := config.Daemon{EventsTopic: "events", DeadLetterTopic: "dlt"}
	if err := applyRetryOutcome(context.Background(), cfg, &memProd{}, out, protocol.SourceCoords{Topic: "retry.short"}); err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for _, e := range events {
		got[e] = true
	}
	if !got["job.expired"] || !got["dlt.published"] {
		t.Fatalf("events=%v", events)
	}
}

type memProd struct{}

func (memProd) Produce(context.Context, string, string, []byte) error { return nil }

func TestEmitRetryDLT(t *testing.T) {
	var dltType string
	instrument.SetHandler(func(event string, payload map[string]interface{}, _ float64) {
		if event == "dlt.published" {
			dltType, _ = payload["dlt_type"].(string)
		}
	})
	defer instrument.SetHandler(nil)

	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "dlt_type": "retry_routing",
	})
	emitRetryDLT(raw, "retry.short")
	if dltType != "retry_routing" {
		t.Fatalf("dlt_type=%q", dltType)
	}
}
