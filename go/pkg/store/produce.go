package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// CreateBatchParams mirrors KafkaBatch.store.create_batch.
type CreateBatchParams struct {
	ID          string
	TotalJobs   int64
	OnSuccess   string
	OnComplete  string
	Meta        map[string]interface{}
	Description string
	TenantID    string
	Sealed      bool
}

// AddJobsResult is the outcome of reserving batch job slots.
type AddJobsResult struct {
	Status   string // ok, not_found, cancelled, closed
	SeqStart int64
	SeqEnd   int64
}

// SealBatchResult is returned by SealBatch.
type SealBatchResult struct {
	Status  string // not_found, sealed, done
	Outcome string
	Batch   *Batch
}

// CreateBatch persists a new batch ledger row (Ruby create_batch).
func (s *RedisStore) CreateBatch(ctx context.Context, p CreateBatchParams) (bool, error) {
	if s == nil || s.client == nil {
		return false, fmt.Errorf("redis store not configured")
	}
	now := time.Now().UTC()
	metaJSON := ""
	if p.Meta != nil {
		b, err := json.Marshal(p.Meta)
		if err != nil {
			return false, err
		}
		metaJSON = string(b)
	}
	lockedAt := ""
	if p.Sealed {
		lockedAt = now.Format(time.RFC3339)
	}
	ttlSec := strconv.Itoa(int(s.ttl.Seconds()))
	res, err := s.client.Eval(ctx, createBatchLua, []string{batchKey(p.ID)},
		p.ID,
		strconv.FormatInt(p.TotalJobs, 10),
		p.OnSuccess,
		p.OnComplete,
		metaJSON,
		now.Format(time.RFC3339),
		ttlSec,
		lockedAt,
		p.Description,
		p.TenantID,
	).Int()
	if err != nil {
		return false, err
	}
	if res != 1 {
		return false, nil
	}
	score := float64(now.UnixNano()) / 1e9
	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, runningIndex, redis.Z{Score: score, Member: p.ID})
	pipe.ZAdd(ctx, allIndex, redis.Z{Score: score, Member: p.ID})
	pipe.HIncrBy(ctx, countsKey, "running", 1)
	if _, err := pipe.Exec(ctx); err != nil {
		return true, err
	}
	return true, nil
}

// AddJobs grows total_jobs and optionally reserves batch_seq values.
func (s *RedisStore) AddJobs(ctx context.Context, id string, count int64) (AddJobsResult, error) {
	out := AddJobsResult{}
	if s == nil || s.client == nil {
		return out, fmt.Errorf("redis store not configured")
	}
	ttlSec := strconv.Itoa(int(s.ttl.Seconds()))
	raw, err := s.client.Eval(ctx, addJobsLua,
		[]string{batchKey(id), seqKey(id)},
		strconv.FormatInt(count, 10), ttlSec,
	).Result()
	if err != nil {
		return out, err
	}
	switch v := raw.(type) {
	case int64:
		out.Status = addJobsStatus(int(v))
	case []interface{}:
		if len(v) == 0 {
			return out, fmt.Errorf("unexpected add_jobs result")
		}
		code, _ := v[0].(int64)
		out.Status = addJobsStatus(int(code))
		if out.Status == "ok" && count > 0 && len(v) >= 3 {
			out.SeqStart, _ = v[1].(int64)
			out.SeqEnd, _ = v[2].(int64)
		}
	default:
		return out, fmt.Errorf("unexpected add_jobs type %T", raw)
	}
	return out, nil
}

// SealBatch opens the completion gate (Ruby seal_batch).
func (s *RedisStore) SealBatch(ctx context.Context, id string) (SealBatchResult, error) {
	out := SealBatchResult{}
	if s == nil || s.client == nil {
		return out, fmt.Errorf("redis store not configured")
	}
	now := time.Now().UTC()
	ttlSec := strconv.Itoa(int(s.ttl.Seconds()))
	score := fmt.Sprintf("%f", float64(now.UnixNano())/1e9)
	raw, err := s.client.Eval(ctx, sealBatchLua,
		[]string{batchKey(id), countsKey, runningIndex, doneIndex, bitmapKey(id)},
		now.Format(time.RFC3339), ttlSec, score,
	).Slice()
	if err != nil {
		return out, err
	}
	code, _ := raw[0].(int64)
	payload, _ := raw[1].(string)
	switch code {
	case 0:
		out.Status = "not_found"
	case 1:
		out.Status = "done"
		out.Outcome = payload
		b, err := s.FindBatch(ctx, id)
		if err != nil {
			return out, err
		}
		out.Batch = b
	case 2:
		out.Status = "sealed"
	default:
		return out, fmt.Errorf("unexpected seal_batch code %d", code)
	}
	return out, nil
}

func addJobsStatus(code int) string {
	switch code {
	case 0:
		return "not_found"
	case 2:
		return "cancelled"
	case 3:
		return "closed"
	default:
		return "ok"
	}
}
