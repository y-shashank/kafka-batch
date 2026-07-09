# kbatch (Go runtime)

Go binaries for the KafkaBatch control plane and backend workers.

## Binaries

| Command | Role |
|---------|------|
| `kbatch daemon` | Control plane — fairness dispatch, events, retry, callbacks, schedule; consumes **ruby** job topics only |
| `kbatch worker` | Go backend — consumes **go** plain, priority, and fair-ready topics; runs handlers in-process |
| `kbatch serve` | **Deprecated** — Phase 2 sidecar for pure Ruby Karafka + `executor :go` only |

## Build

```bash
cd go
go build -o kbatch-daemon ./cmd/kbatch-daemon   # link your handlers via kbatch.Register
go build -o kbatch-worker ./cmd/kbatch-worker-ittest  # or your worker main
```

Integration test binaries:

```bash
go build -o ../bin/kbatch-daemon-ittest ./cmd/kbatch-daemon-ittest
go build -o ../bin/kbatch-worker-ittest ./cmd/kbatch-worker-ittest
KAFKA_BATCH_INTEGRATION=1 bundle exec rspec spec/integration/go_*.rb
```

## Three-tier deployment (v1.1+)

1. **Ruby gem** — `daemon_mode: true`, produce only
2. **`kbatch daemon`** — internal topics + ruby execution (unix socket to worker server)
3. **`kbatch worker`** — all `runtime: go` handlers

See the main [README](../README.md#go-stack-deployment-v110) for full deployment docs.

## Go produce client (`go/pkg/client`)

Go services can enqueue jobs without the Ruby gem:

```go
cfg := client.DefaultConfig()
cfg.Brokers = []string{"localhost:9092"}
cfg.RedisURL = "redis://localhost:6379/0"
cfg.ManifestPath = "config/kafka_batch_handlers.yml"

c, err := client.New(cfg)
defer c.Close()

// Standalone job
_, _ = c.EnqueueJob(ctx, "my.handler", map[string]interface{}{"id": 1}, client.PushOptions{})

// Batch (block form)
batch, _ := c.CreateBatch(ctx, client.BatchOptions{OnComplete: "MyCallback"}, func(b *client.Batch) error {
    _, err := b.PushJob(ctx, "my.handler", map[string]interface{}{"id": 1}, client.PushOptions{})
    return err
})
```

Wire-compatible with Ruby: same Redis batch keys, job JSON envelope, schedule index members, and uniq fingerprints.

## Legacy sidecar

`kbatch serve` is retained for Karafka-only apps that have not migrated to `daemon_mode`. Do not run it alongside `kbatch daemon` or `kbatch worker`.
