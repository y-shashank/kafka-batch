package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/y-shashank/kafka-batch/go/pkg/daemon"
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
	case "daemon":
		runDaemon(os.Args[2:])
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

func runDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	cfg := fs.String("config", "", "daemon config YAML path")
	manifest := fs.String("manifest", "", "handler manifest YAML path")
	_ = fs.Parse(args)
	if *cfg == "" {
		fmt.Fprintln(os.Stderr, "daemon requires --config")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := daemon.Run(ctx, *cfg, *manifest); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch daemon: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `kbatch — KafkaBatch Go runtime (Phase 2 sidecar + Phase 3 daemon)

Usage:
  kbatch serve [--socket PATH]           # Phase 2: handler sidecar only
  kbatch daemon --config PATH [--manifest PATH]   # Phase 3: control plane

Environment:
  KAFKA_BROKERS, KAFKA_PREFIX, REDIS_URL, KAFKA_BATCH_HANDLER_MANIFEST
`)
}
