package client

import (
	"strconv"
	"time"
)

func clampRunAt(v interface{}, horizon time.Duration) time.Time {
	now := time.Now().UTC()
	max := now.Add(horizon)
	t := now

	switch x := v.(type) {
	case time.Time:
		t = x.UTC()
	case *time.Time:
		if x != nil {
			t = x.UTC()
		}
	case time.Duration:
		t = now.Add(x)
	case float64:
		t = time.Unix(int64(x), 0).UTC()
	case int64:
		t = time.Unix(x, 0).UTC()
	case int:
		t = time.Unix(int64(x), 0).UTC()
	case string:
		if parsed, err := time.Parse(time.RFC3339, x); err == nil {
			t = parsed.UTC()
		} else if sec, err := strconv.ParseInt(x, 10, 64); err == nil {
			t = time.Unix(sec, 0).UTC()
		}
	}

	if t.After(max) {
		return max
	}
	if t.Before(now) {
		return now
	}
	return t
}
