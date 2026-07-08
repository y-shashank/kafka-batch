package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRecordCompletionsBatchFinalizes(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	batchID := "batch-1"
	// Seed batch hash like Ruby create_batch + seal
	now := time.Now().UTC().Format(time.RFC3339)
	mr.HSet("kafka_batch:b:"+batchID,
		"id", batchID,
		"total_jobs", "2",
		"completed_count", "0",
		"failed_count", "0",
		"status", "running",
		"locked_at", now,
		"on_success", "Cb",
		"on_complete", "Cb",
	)
	mr.ZAdd("kafka_batch:index:running", 1, batchID)

	res, err := st.RecordCompletionsBatch(ctx, []CompletionEvent{
		{BatchID: batchID, JobID: "j1", Status: "success", BatchSeq: 1},
		{BatchID: batchID, JobID: "j2", Status: "success", BatchSeq: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Finished) != 1 {
		t.Fatalf("expected 1 finished batch, got %+v", res.Finished)
	}
	if res.Finished[0].Outcome != "success" {
		t.Fatalf("outcome %q", res.Finished[0].Outcome)
	}
	batch, err := st.FindBatch(ctx, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Status != "success" {
		t.Fatalf("status %q", batch.Status)
	}
	if batch.CompletedCount != 2 {
		t.Fatalf("completed %d", batch.CompletedCount)
	}
}

func TestRecordCompletionsBatchDedup(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	batchID := "batch-dedup"
	mr.HSet("kafka_batch:b:"+batchID, "id", batchID, "total_jobs", "1", "completed_count", "0", "failed_count", "0", "status", "running", "locked_at", time.Now().UTC().Format(time.RFC3339))
	mr.ZAdd("kafka_batch:index:running", 1, batchID)

	ev := CompletionEvent{BatchID: batchID, JobID: "j1", Status: "success", BatchSeq: 1}
	if _, err := st.RecordCompletionsBatch(ctx, []CompletionEvent{ev}); err != nil {
		t.Fatal(err)
	}
	res, err := st.RecordCompletionsBatch(ctx, []CompletionEvent{ev})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Replays) != 1 || res.Replays[0] != batchID {
		t.Fatalf("replays %+v", res.Replays)
	}
}
