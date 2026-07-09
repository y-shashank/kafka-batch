package metrics

import (
	"fmt"
	"testing"

	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
)

func TestEventName(t *testing.T) {
	if got := eventName("job.processed"); got != "job_processed" {
		t.Fatalf("got %q", got)
	}
}

func TestTagsForSkipsPayloadAndError(t *testing.T) {
	tags := tagsFor(map[string]interface{}{
		"worker_class": "go:echo",
		"payload":      map[string]interface{}{"x": 1},
		"error":        "boom",
		"attempt":      2,
	})
	if len(tags) != 2 {
		t.Fatalf("tags %v", tags)
	}
}

func TestStatsdAdapterEmit(t *testing.T) {
	rec := &recordStatsD{}
	adapter := statsdAdapter{client: rec, prefix: "kafka_batch"}
	adapter.emit("batch.created", map[string]interface{}{"batch_id": "b1"}, 12.5)
	if len(rec.lines) != 2 {
		t.Fatalf("lines %v", rec.lines)
	}
	if rec.lines[0] != "kafka_batch.batch_created.count:1|c|#batch_id:b1" {
		t.Fatalf("count line %q", rec.lines[0])
	}
	if rec.lines[1] != "kafka_batch.batch_created.duration:12|ms|#batch_id:b1" {
		t.Fatalf("timing line %q", rec.lines[1])
	}
}

func TestInstallProcHandler(t *testing.T) {
	var gotEvent string
	cfg := Config{
		Enabled: true,
		Handler: func(event string, _ map[string]interface{}, _ float64) {
			gotEvent = event
		},
	}
	if err := Install(cfg); err != nil {
		t.Fatal(err)
	}
	defer Reset()

	instrument.Emit("job.processed", map[string]interface{}{"job_id": "j1"}, 1)
	if gotEvent != "job.processed" {
		t.Fatalf("got %q", gotEvent)
	}
}

type recordStatsD struct {
	lines []string
}

func (r *recordStatsD) increment(name string, tags []string) error {
	r.lines = append(r.lines, name+":1|c"+tagSuffix(tags))
	return nil
}

func (r *recordStatsD) timing(name string, ms float64, tags []string) error {
	r.lines = append(r.lines, fmt.Sprintf("%s:%d|ms%s", name, int(ms), tagSuffix(tags)))
	return nil
}
