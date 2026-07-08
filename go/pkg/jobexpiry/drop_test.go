package jobexpiry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

func TestBuildDropExpiredDLT(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "valid_till": "2000-01-01T00:00:00Z",
	})
	src := protocol.SourceCoords{Topic: "jobs", Partition: 1, Offset: 9}
	out := BuildDrop(raw, src, now)
	var m map[string]interface{}
	if err := json.Unmarshal(out.DLTPayload, &m); err != nil {
		t.Fatal(err)
	}
	if m["dlt_type"] != "expired" {
		t.Fatalf("dlt_type %v", m["dlt_type"])
	}
	if m["dlt_error_class"] != ExpiredErrorClass {
		t.Fatalf("class %v", m["dlt_error_class"])
	}
}

func TestBuildDropEmitsEventWhenNotCounted(t *testing.T) {
	now := time.Now().UTC()
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "batch_id": "b1", "batch_seq": float64(2),
		"worker_class": "W", "valid_till": "2000-01-01T00:00:00Z",
	})
	out := BuildDrop(raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 1}, now)
	if out.Event == nil || out.Event.BatchSeq != 2 {
		t.Fatalf("event %+v", out.Event)
	}
	if out.Failure == nil || out.Failure.Status != "expired" {
		t.Fatalf("failure %+v", out.Failure)
	}
}

func TestBuildDropSkipsEventWhenBatchCounted(t *testing.T) {
	raw, _ := json.Marshal(map[string]interface{}{
		"job_id": "j1", "batch_id": "b1", "batch_seq": float64(1),
		"batch_counted": true, "valid_till": "2000-01-01T00:00:00Z",
	})
	out := BuildDrop(raw, protocol.SourceCoords{Topic: "jobs"}, time.Now())
	if out.Event != nil {
		t.Fatal("expected no event")
	}
}

func TestStampSource(t *testing.T) {
	m := map[string]interface{}{"job_id": "j1"}
	src := protocol.SourceCoords{Topic: "ingest", Partition: 2, Offset: 3}
	StampSource(m, src)
	if m["_src_topic"] != "ingest" {
		t.Fatalf("topic %v", m["_src_topic"])
	}
}
