package client

import (
	"testing"
	"time"
)

func TestClampRunAt(t *testing.T) {
	horizon := 24 * time.Hour
	now := time.Now().UTC()

	past := now.Add(-time.Hour)
	if got := clampRunAt(past, horizon); got.Before(now.Add(-time.Second)) {
		t.Fatalf("past clamped to now, got %v", got)
	}

	future := now.Add(48 * time.Hour)
	if got := clampRunAt(future, horizon); got.After(now.Add(horizon).Add(time.Second)) {
		t.Fatalf("future clamped to horizon, got %v", got)
	}

	if got := clampRunAt(30*time.Minute, horizon); got.Before(now) || got.After(now.Add(horizon)) {
		t.Fatalf("duration clamp %+v", got)
	}

	if got := clampRunAt(now.Unix(), horizon); got.Sub(now) > time.Second {
		t.Fatalf("unix int clamp %+v", got)
	}
}
