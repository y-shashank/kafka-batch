package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCreateAddSealBatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)

	ctx := context.Background()
	id := "batch-1"
	ok, err := st.CreateBatch(ctx, CreateBatchParams{
		ID: id, TotalJobs: 0, OnComplete: "MyCallback", Sealed: false,
	})
	if err != nil || !ok {
		t.Fatalf("create ok=%v err=%v", ok, err)
	}

	add, err := st.AddJobs(ctx, id, 2)
	if err != nil || add.Status != "ok" || add.SeqStart != 1 || add.SeqEnd != 2 {
		t.Fatalf("add %+v err=%v", add, err)
	}

	seal, err := st.SealBatch(ctx, id)
	if err != nil || seal.Status != "sealed" {
		t.Fatalf("seal %+v err=%v", seal, err)
	}

	row, err := st.FindBatch(ctx, id)
	if err != nil || row == nil || row.TotalJobs != 2 {
		t.Fatalf("row %+v err=%v", row, err)
	}
	if mr.HGet("kafka_batch:b:"+id, "locked_at") == "" {
		t.Fatal("expected locked_at set")
	}
}

func TestAddJobsRejectsClosedBatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()
	id := "closed-1"
	_, _ = st.CreateBatch(ctx, CreateBatchParams{ID: id, Sealed: true})
	mr.HSet("kafka_batch:b:"+id, "status", "success")

	add, err := st.AddJobs(ctx, id, 1)
	if err != nil || add.Status != "closed" {
		t.Fatalf("add %+v err=%v", add, err)
	}
}
