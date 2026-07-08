package store

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestReleaseUniqLock(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := NewRedisStore(rdb, time.Hour)
	ctx := context.Background()

	fp := "0123456789abcdef0123456789abcdef"
	bin, err := hex.DecodeString(fp)
	if err != nil {
		t.Fatal(err)
	}
	key := uniqKeyPrefix + string(bin)
	mr.Set(key, "job-1")

	if err := st.ReleaseUniqLock(ctx, fp, "job-1"); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(key) {
		t.Fatal("expected lock released")
	}

	mr.Set(key, "other")
	if err := st.ReleaseUniqLock(ctx, fp, "job-1"); err != nil {
		t.Fatal(err)
	}
	if !mr.Exists(key) {
		t.Fatal("expected lock kept for other owner")
	}
}
