package schedule

import (
	"context"
	"time"
)

// PayloadReader loads scheduled payloads (injectable for tests).
type PayloadReader interface {
	Read(ctx context.Context, byPartition map[int32][]int64) (ReadResult, error)
}

// IndexStore is the delayed-job Redis index.
type IndexStore interface {
	ClaimDue(ctx context.Context, now time.Time, leaseSeconds, limit int) ([]string, error)
	Ack(ctx context.Context, members []string) error
	Reclaim(ctx context.Context, now time.Time) (int, error)
	RecordReadMiss(ctx context.Context, member string) (int64, error)
	ClearReadMiss(ctx context.Context, member string) error
}
