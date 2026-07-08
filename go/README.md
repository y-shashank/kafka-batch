# kbatch (Go sidecar)

Phase 2 execution host for KafkaBatch. Ruby Karafka keeps the control plane; this
binary runs Go `job_type` handlers over a Unix socket.

## Build

Requires **Go 1.24+** on macOS 26 (Tahoe) — older Go versions omit `LC_UUID` and dyld aborts at launch.

```bash
brew upgrade go
go version   # go1.24.x or newer
```

```bash
cd go
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o ../bin/kbatch ./cmd/kbatch
```

On Apple Silicon use `GOARCH=arm64`; on Intel Mac use `GOARCH=amd64`.

### Integration tests (real broker + sidecar)

```bash
cd go && go build -o ../bin/kbatch-ittest ./cmd/kbatch-ittest
KAFKA_BATCH_INTEGRATION=1 bundle exec rspec spec/integration/go_sidecar_spec.rb
```

CI builds `bin/kbatch-ittest` and runs this with the KRaft broker. Requires **Go 1.24+** on macOS 26.

## Run

```bash
./bin/kbatch serve --socket /var/run/kbatch.sock
```

Register handlers from your application (import your handler package in `main`):

```go
package main

import (
    "github.com/y-shashank/kafka-batch/go/cmd/kbatch"
    "github.com/y-shashank/kafka-batch/go/pkg/kbatch"
    _ "your/app/handlers" // calls kbatch.Register in init()
)

func main() { kbatch.Main() } // or copy serve wiring from cmd/kbatch/main.go
```

See `protocol/README.md` for the HTTP/JSON contract.
