package jobexpiry

import (
	"strings"
	"time"
)

// Expired reports whether valid_till has passed (mirrors JobExpiry.expired?).
// Unparseable timestamps are treated as expired (poison pill).
func Expired(validTill string, now time.Time) bool {
	validTill = strings.TrimSpace(validTill)
	if validTill == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339, validTill)
	if err != nil {
		return true
	}
	return !t.After(now)
}
