package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/y-shashank/kafka-batch/go/pkg/kbatch"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "serve":
		serve(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	socket := fs.String("socket", "/tmp/kbatch.sock", "Unix socket path for HTTP API")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := kbatch.NewServer(kbatch.ServerConfig{SocketPath: *socket})
	fmt.Printf("kbatch serve listening on %s\n", *socket)
	if err := srv.ListenAndServe(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch serve: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `kbatch — KafkaBatch Go handler sidecar (Phase 2)

Usage:
  kbatch serve [--socket PATH]

Register handlers from your app with kbatch.Register("job.type", fn) and import
that package from a small main, or use the example in go/cmd/kbatch.

Environment (Ruby host):
  KAFKA_BATCH_GO_SOCKET=/var/run/kbatch.sock
`)
}
