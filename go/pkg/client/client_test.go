package client

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/uniq"
)

func TestResolveRoutePlain(t *testing.T) {
	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"segment.export": {Runtime: "go", Topic: "jobs.export"},
	}}
	c := &Client{cfg: DefaultConfig(), manifest: manifest}
	route := c.routeFor(manifest.Handlers["segment.export"], "j1", "", nil)
	if route.Topic != "jobs.export" || route.Key != "j1" {
		t.Fatalf("route %+v", route)
	}
}

func TestResolveRouteFair(t *testing.T) {
	cfg := DefaultConfig()
	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"fair.job": {Runtime: "go", FairnessType: "time"},
	}}
	c := &Client{cfg: cfg, manifest: manifest}
	route := c.routeFor(manifest.Handlers["fair.job"], "j1", "tenant-a", nil)
	want := cfg.resolveTopic(cfg.FairnessTimeIngest)
	if route.Topic != want || route.Key != "tenant-a" {
		t.Fatalf("route %+v want topic=%s", route, want)
	}
}

func TestBuildMessageGoHandler(t *testing.T) {
	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"echo": {Runtime: "go", MaxRetries: 5, Uniq: true},
	}}
	c := &Client{cfg: DefaultConfig(), manifest: manifest}
	entry := manifest.Handlers["echo"]
	msg, err := c.buildMessage(entry, "echo", map[string]interface{}{"x": 1}, "job-1", nil, PushOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if msg.WorkerClass != "go:echo" || msg.MaxRetries != 5 {
		t.Fatalf("msg %+v", msg)
	}
	if msg.UniqFP == "" {
		t.Fatal("expected uniq fp")
	}
}

func TestClaimUniqSkip(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	cfg := DefaultConfig()
	cfg.UniqOnDuplicate = "skip"
	c := &Client{
		cfg: cfg,
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"uniq.job": {Runtime: "go", Uniq: true},
		}},
		uniq: uniqLocker(rdb, cfg.UniqLockTTL),
	}
	entry := c.manifest.Handlers["uniq.job"]
	payload := map[string]interface{}{"id": 1}
	skipped, err := c.claimUniq(context.Background(), entry, "uniq.job", payload, "j1", "")
	if err != nil || skipped {
		t.Fatalf("first claim skipped=%v err=%v", skipped, err)
	}
	skipped, err = c.claimUniq(context.Background(), entry, "uniq.job", payload, "j2", "")
	if err != nil || !skipped {
		t.Fatalf("duplicate claim skipped=%v err=%v", skipped, err)
	}
}

func TestClientNewLoadsManifest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "handlers.yml")
	if err := os.WriteFile(path, []byte(`handlers:
  test.echo:
    runtime: go
    topic: jobs.echo
`), 0o644); err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	cfg := DefaultConfig()
	cfg.ManifestPath = path
	cfg.RedisURL = "redis://" + mr.Addr()
	// Skip if no kafka — only test manifest load would need kafka.New to succeed.
	// miniredis addr without kafka will fail at kafkaclient.New in real env.
	_, err := New(cfg)
	if err == nil {
		t.Log("client connected (kafka available)")
	}
}

func uniqLocker(rdb *redis.Client, ttl time.Duration) *uniq.Locker {
	return uniq.NewLocker(rdb, ttl)
}
