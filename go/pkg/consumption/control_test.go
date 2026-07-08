package consumption

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestTopicLevelPauseAndActiveHigherTopics(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := NewControl(rdb, time.Nanosecond)
	ctx := context.Background()

	group := "kafka-batch-jobs-fast"
	if err := c.PauseTopic(ctx, group, "p0.topic"); err != nil {
		t.Fatal(err)
	}
	if !c.TopicLevelPaused(ctx, group, "p0.topic") {
		t.Fatal("expected topic paused")
	}
	if c.Paused(ctx, group, "p1.topic", 0) {
		t.Fatal("p1 should not be paused")
	}

	active := c.ActiveHigherTopics(ctx, group, []string{"p0.topic", "p1.topic"})
	if len(active) != 1 || active[0] != "p1.topic" {
		t.Fatalf("active higher %+v", active)
	}

	if err := c.ResumeTopic(ctx, group, "p0.topic"); err != nil {
		t.Fatal(err)
	}
	active = c.ActiveHigherTopics(ctx, group, []string{"p0.topic", "p1.topic"})
	if len(active) != 2 {
		t.Fatalf("after resume %+v", active)
	}
}
