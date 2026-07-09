package jobexpiry

import (
	"strings"
	"time"
)

// NormalizeValidTill converts a producer-side valid_till to ISO8601 UTC.
func NormalizeValidTill(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		if sec, err2 := time.Parse(time.RFC3339Nano, value); err2 == nil {
			t = sec
		} else {
			return ""
		}
	}
	return t.UTC().Format(time.RFC3339)
}

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
