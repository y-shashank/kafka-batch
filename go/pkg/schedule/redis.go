package schedule

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore is the delayed-job index (Ruby Schedule::RedisStore).
type RedisStore struct {
	client       *redis.Client
	reclaimLimit int
}

func NewRedisStore(client *redis.Client, reclaimLimit int) *RedisStore {
	if reclaimLimit <= 0 {
		reclaimLimit = 500
	}
	return &RedisStore{client: client, reclaimLimit: reclaimLimit}
}

func epoch(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

func (s *RedisStore) Schedule(ctx context.Context, jobID string, runAt time.Time, partition int32, offset int64) error {
	member := BuildMember(jobID, partition, offset)
	return s.client.ZAdd(ctx, pendingKey, redis.Z{Score: epoch(runAt), Member: member}).Err()
}

// ScheduleEntry is one delayed-job index row.
type ScheduleEntry struct {
	JobID     string
	RunAt     time.Time
	Partition int32
	Offset    int64
	BatchID   string // optional; used by MySQL index
}

// ScheduleMany bulk-writes schedule index members (Ruby schedule_many).
func (s *RedisStore) ScheduleMany(ctx context.Context, entries []ScheduleEntry) error {
	if len(entries) == 0 {
		return nil
	}
	members := make([]redis.Z, len(entries))
	for i, e := range entries {
		members[i] = redis.Z{
			Score:  epoch(e.RunAt),
			Member: BuildMember(e.JobID, e.Partition, e.Offset),
		}
	}
	return s.client.ZAdd(ctx, pendingKey, members...).Err()
}

func (s *RedisStore) ClaimDue(ctx context.Context, now time.Time, leaseSeconds, limit int) ([]string, error) {
	leaseUntil := epoch(now) + float64(leaseSeconds)
	res, err := s.client.Eval(ctx, claimDueLua,
		[]string{pendingKey, inflightKey},
		epoch(now), limit, leaseUntil,
	).Slice()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res))
	for _, v := range res {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

func (s *RedisStore) Ack(ctx context.Context, members []string) error {
	if len(members) == 0 {
		return nil
	}
	return s.client.ZRem(ctx, inflightKey, members).Err()
}

func (s *RedisStore) Reclaim(ctx context.Context, now time.Time) (int, error) {
	n, err := s.client.Eval(ctx, reclaimLua,
		[]string{inflightKey, pendingKey},
		epoch(now), s.reclaimLimit,
	).Int()
	return n, err
}

func (s *RedisStore) RecordReadMiss(ctx context.Context, member string) (int64, error) {
	return s.client.HIncrBy(ctx, readMissKey, member, 1).Result()
}

func (s *RedisStore) ClearReadMiss(ctx context.Context, member string) error {
	return s.client.HDel(ctx, readMissKey, member).Err()
}
