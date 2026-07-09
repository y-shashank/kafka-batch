package uniq

import (
	"testing"
)

func TestFingerprintStableKeyOrder(t *testing.T) {
	a := DigestHex("Worker", map[string]interface{}{"a": 1, "b": 2})
	b := DigestHex("Worker", map[string]interface{}{"b": 2, "a": 1})
	if a != b {
		t.Fatalf("fingerprints differ: %s vs %s", a, b)
	}
	if len(a) != 32 {
		t.Fatalf("expected 32 hex chars, got %d", len(a))
	}
}

func TestFingerprintDiffersByWorker(t *testing.T) {
	payload := map[string]interface{}{"id": 1}
	a := DigestHex("WorkerA", payload)
	b := DigestHex("WorkerB", payload)
	if a == b {
		t.Fatal("expected different fingerprints")
	}
}
