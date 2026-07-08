package retry

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

func TestProcessExpiredRetryDLT(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "valid_till": "2000-01-01T00:00:00Z",
		"retry_to": "jobs.worker", "retry_after": now.Add(-time.Second).UTC().Format(time.RFC3339),
	})
	p := &Processor{Producer: &memProducer{}, Now: func() time.Time { return now }, MaxPause: 30 * time.Second}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "retry.short"})
	if err != nil {
		t.Fatal(err)
	}
	if out.DLTPayload == nil {
		t.Fatal("expected DLT")
	}
	var m map[string]interface{}
	_ = json.Unmarshal(out.DLTPayload, &m)
	if m["dlt_type"] != "expired" {
		t.Fatalf("dlt_type %v", m["dlt_type"])
	}
	if out.ProduceBody != nil {
		t.Fatal("should not republish expired job")
	}
}
