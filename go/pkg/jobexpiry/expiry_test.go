package jobexpiry

import (
	"testing"
	"time"
)

func TestExpired(t *testing.T) {
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	if Expired("", now) {
		t.Fatal("empty")
	}
	if !Expired("2026-07-08T11:00:00Z", now) {
		t.Fatal("past")
	}
	if Expired("2026-07-08T13:00:00Z", now) {
		t.Fatal("future")
	}
	if !Expired("not-a-time", now) {
		t.Fatal("invalid")
	}
}
