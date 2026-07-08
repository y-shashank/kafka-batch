# kbatch (Go sidecar)

Phase 2 execution host for KafkaBatch. Ruby Karafka keeps the control plane; this
binary runs Go `job_type` handlers over a Unix socket.

## Build

```bash
cd go
go build -o ../bin/kbatch ./cmd/kbatch
```

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
