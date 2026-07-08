package store

import (
	"context"
	"encoding/json"
	"time"
)

const recordFailureLua = `
local cap = tonumber(ARGV[4])
if cap > 0 and redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
  if redis.call('HLEN', KEYS[1]) >= cap then
    return 0
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
`

func failuresKey(batchID string) string {
	return batchKey(batchID) + ":failures"
}

// FailureEntry is input to RecordFailure (Ruby-compatible).
type FailureEntry struct {
	BatchID      string
	JobID        string
	WorkerClass  string
	ErrorClass   string
	ErrorMessage string
	Attempt      int
	Status       string
	NextRetryAt  string
}

// RecordFailure stores a per-batch failure row for the Web UI (best-effort).
func (s *RedisStore) RecordFailure(ctx context.Context, e FailureEntry) error {
	if s == nil || s.client == nil || e.BatchID == "" || e.JobID == "" {
		return nil
	}
	status := e.Status
	if status == "" {
		status = "failed"
	}
	entry := map[string]interface{}{
		"job_id":        e.JobID,
		"worker_class":  e.WorkerClass,
		"error_class":   e.ErrorClass,
		"error_message": e.ErrorMessage,
		"attempt":       e.Attempt,
		"status":        status,
		"failed_at":     time.Now().UTC().Format(time.RFC3339),
	}
	if e.NextRetryAt != "" {
		entry["next_retry_at"] = e.NextRetryAt
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	ttl := int64(s.ttl.Seconds())
	if ttl <= 0 {
		ttl = 86400
	}
	_, err = s.client.Eval(ctx, recordFailureLua,
		[]string{failuresKey(e.BatchID)},
		e.JobID, string(raw), ttl, 0,
	).Result()
	return err
}
