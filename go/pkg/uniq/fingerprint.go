package uniq

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
)

const keyPrefix = "kafka_batch:uniq:"

const releaseLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// Locker manages per-worker uniqueness locks (Ruby KafkaBatch::Uniqueness).
type Locker struct {
	client *redis.Client
	ttl    time.Duration
}

func NewLocker(client *redis.Client, ttl time.Duration) *Locker {
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	return &Locker{client: client, ttl: ttl}
}

// Claim tries to acquire the uniq lock. Returns true when acquired.
func (l *Locker) Claim(ctx context.Context, workerClassName string, payload map[string]interface{}, jobID string) (bool, error) {
	if l == nil || l.client == nil {
		return true, nil
	}
	key := redisKey(workerClassName, payload)
	ok, err := l.client.SetNX(ctx, key, jobID, l.ttl).Result()
	if err != nil {
		// Fail open like Ruby when Redis is unavailable.
		return true, nil
	}
	return ok, nil
}

// Release drops a lock by fingerprint hex from the wire message.
func (l *Locker) Release(ctx context.Context, fpHex, jobID string) error {
	if l == nil || l.client == nil || fpHex == "" || jobID == "" {
		return nil
	}
	bin, err := hex.DecodeString(fpHex)
	if err != nil || len(bin) != 16 {
		return nil
	}
	key := keyPrefix + string(bin)
	return l.client.Eval(ctx, releaseLua, []string{key}, jobID).Err()
}

// DigestHex returns the 32-char hex fingerprint for _uniq_fp on the wire.
func DigestHex(workerClassName string, payload map[string]interface{}) string {
	return hex.EncodeToString(fingerprint(workerClassName, payload))
}

func redisKey(workerClassName string, payload map[string]interface{}) string {
	return keyPrefix + string(fingerprint(workerClassName, payload))
}

func fingerprint(workerClassName string, payload map[string]interface{}) []byte {
	material := workerClassName + "\x00" + canonicalPayload(payload)
	h1 := xxhash.Sum64String(material)
	h2 := xxhash.Sum64String(material + "\x00uniq_salt_v1")
	buf := make([]byte, 16)
	for i, h := range []uint64{h1, h2} {
		for j := 0; j < 8; j++ {
			buf[i*8+j] = byte(h >> (8 * j))
		}
	}
	return buf
}

func canonicalPayload(payload map[string]interface{}) string {
	b, _ := json.Marshal(deepSortKeys(payload))
	return string(b)
}

func deepSortKeys(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]interface{}, len(t))
		for _, k := range keys {
			out[k] = deepSortKeys(t[k])
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = deepSortKeys(e)
		}
		return out
	default:
		return v
	}
}
