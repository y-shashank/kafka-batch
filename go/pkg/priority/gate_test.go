package priority

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
)

func TestLoadPriorityConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fast.yml")
	_ = os.WriteFile(path, []byte(`---
consumer_group_suffix: jobs-fast
mode: weighted
weighted_interleave: 3
topics:
  - kafka_batch.jobs.p0
  - kafka_batch.jobs.p1
`), 0o644)

	cfg := config.Daemon{ConsumerGroup: "kafka-batch"}
	pc, err := Load(path, cfg, "kafka_batch.jobs")
	if err != nil {
		t.Fatal(err)
	}
	if pc.ConsumerGroup != "kafka-batch-jobs-fast" || pc.Mode != ModeWeighted || pc.WeightedInterleave != 3 {
		t.Fatalf("loaded %+v", pc)
	}
	specs := pc.TopicSpecs()
	if specs[1].Rank != 1 || len(specs[1].HigherTopics) != 1 || specs[1].HigherTopics[0] != specs[0].Topic {
		t.Fatalf("specs %+v", specs)
	}
}

func TestShouldYieldStrict(t *testing.T) {
	spec := TopicSpec{
		Rank: 1, Mode: ModeStrict,
		HigherTopics: []string{"p0"}, ConsumerGroup: "g",
	}
	gate := &Gate{
		Interval: time.Second,
		Reader: &stubLag{hasLag: true},
	}
	tick := 0
	yield, _ := ShouldYield(spec, gate, &tick, context.Background())
	if !yield {
		t.Fatal("expected strict yield")
	}
}

func TestShouldYieldWeightedInterleave(t *testing.T) {
	spec := TopicSpec{
		Rank: 1, Mode: ModeWeighted, WeightedInterleave: 4,
		HigherTopics: []string{"p0"}, ConsumerGroup: "g",
	}
	gate := &Gate{Interval: time.Second, Reader: &stubLag{hasLag: true}}
	tick := 0
	for i := 0; i < 3; i++ {
		yield, _ := ShouldYield(spec, gate, &tick, context.Background())
		if !yield {
			t.Fatalf("expected yield on tick %d", i+1)
		}
	}
	yield, _ := ShouldYield(spec, gate, &tick, context.Background())
	if yield {
		t.Fatal("expected 4th message through weighted gate")
	}
}

type stubLag struct {
	hasLag bool
	err    error
}

func (s *stubLag) GroupHasLag(ctx context.Context, group string, topics []string) (bool, error) {
	return s.hasLag, s.err
}

// stubLag adapter — Gate uses LagReader not interface. Refactor Gate to use interface.
