package store

import (
	"context"
	"encoding/hex"
)

const uniqKeyPrefix = "kafka_batch:uniq:"

const releaseUniqLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// ReleaseUniqLock drops a per-worker uniqueness lock using _uniq_fp from the job wire
// format (mirrors KafkaBatch::Uniqueness.release_by_name fast path).
func (s *RedisStore) ReleaseUniqLock(ctx context.Context, fpHex, jobID string) error {
	if s == nil || s.client == nil || fpHex == "" || jobID == "" {
		return nil
	}
	bin, err := hex.DecodeString(fpHex)
	if err != nil || len(bin) != 16 {
		return nil
	}
	key := uniqKeyPrefix + string(bin)
	return s.client.Eval(ctx, releaseUniqLua, []string{key}, jobID).Err()
}
