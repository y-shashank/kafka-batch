package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// CompletionEvent is input to RecordCompletionsBatch.
type CompletionEvent struct {
	BatchID  string
	JobID    string
	Status   string // success | failed
	BatchSeq int64
}

// Batch is a batch ledger record.
type Batch struct {
	ID              string
	TotalJobs       int64
	CompletedCount  int64
	FailedCount     int64
	Status          string
	OnSuccess       string
	OnComplete      string
	Meta            string
	Description     string
	TenantID        string
	FinishedAt      string
	CallbackClaimed bool
}

// FinishedBatch is returned when a batch just completed.
type FinishedBatch struct {
	Batch   *Batch
	Outcome string
}

// CompletionsResult aggregates record_completions_batch output.
type CompletionsResult struct {
	Finished []FinishedBatch
	Replays  []string
}

// RedisStore implements the batch ledger against Redis (Ruby-compatible).
type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisStore(client *redis.Client, ttl time.Duration) *RedisStore {
	return &RedisStore{client: client, ttl: ttl}
}

func (s *RedisStore) completionKeys(batchID string) []string {
	return []string{batchKey(batchID), bitmapKey(batchID), runningIndex, doneIndex, countsKey}
}

func (s *RedisStore) RecordCompletionsBatch(ctx context.Context, events []CompletionEvent) (CompletionsResult, error) {
	out := CompletionsResult{}
	if len(events) == 0 {
		return out, nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	nowFloat := fmt.Sprintf("%f", float64(time.Now().UnixNano())/1e9)
	ttlSec := strconv.Itoa(int(s.ttl.Seconds()))

	pipe := s.client.Pipeline()
	cmds := make([]*redis.Cmd, len(events))
	for i, e := range events {
		field := "failed_count"
		if e.Status == "success" {
			field = "completed_count"
		}
		cmds[i] = pipe.Eval(ctx, batchDoneJobLua, s.completionKeys(e.BatchID),
			e.BatchSeq, field, ttlSec, now, nowFloat)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return out, err
	}

	finalized := []int{}
	for i, cmd := range cmds {
		res, err := cmd.Slice()
		if err != nil {
			return out, err
		}
		code, _ := res[0].(int64)
		payload, _ := res[1].(string)
		switch code {
		case 1:
			finalized = append(finalized, i)
		case 0:
			if payload == "duplicate" {
				out.Replays = append(out.Replays, events[i].BatchID)
			}
		}
	}

	for _, idx := range finalized {
		res, _ := cmds[idx].Slice()
		outcome, _ := res[1].(string)
		batch, err := s.FindBatch(ctx, events[idx].BatchID)
		if err != nil {
			return out, err
		}
		out.Finished = append(out.Finished, FinishedBatch{Batch: batch, Outcome: outcome})
	}
	return out, nil
}

func (s *RedisStore) FindBatch(ctx context.Context, id string) (*Batch, error) {
	h, err := s.client.HGetAll(ctx, batchKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(h) == 0 {
		return nil, nil
	}
	b := &Batch{
		ID: h["id"], Status: h["status"], OnSuccess: h["on_success"], OnComplete: h["on_complete"],
		Meta: h["meta"], Description: h["description"], TenantID: h["tenant_id"],
		FinishedAt: h["finished_at"],
	}
	b.TotalJobs, _ = strconv.ParseInt(h["total_jobs"], 10, 64)
	b.CompletedCount, _ = strconv.ParseInt(h["completed_count"], 10, 64)
	b.FailedCount, _ = strconv.ParseInt(h["failed_count"], 10, 64)
	b.CallbackClaimed = h["callback_dispatched_at"] != ""
	return b, nil
}

func (s *RedisStore) ClaimCallback(ctx context.Context, batchID, nodeID string) (bool, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := s.client.Eval(ctx, claimCallbackLua,
		[]string{batchKey(batchID), doneIndex},
		now, nodeID, batchID,
	).Int()
	return res == 1, err
}

func (s *RedisStore) CallbackDispatched(ctx context.Context, batchID string) (bool, error) {
	v, err := s.client.HGet(ctx, batchKey(batchID), "callback_dispatched_at").Result()
	if err == redis.Nil {
		return false, nil
	}
	return v != "", err
}

func (s *RedisStore) BatchCancelled(ctx context.Context, batchID string) (bool, error) {
	score, err := s.client.ZScore(ctx, cancelledIndex, batchID).Result()
	if err == redis.Nil {
		return false, nil
	}
	return score > 0, err
}
