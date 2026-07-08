// kbatch-ittest is a minimal Go sidecar for kafka-batch integration tests.
// It registers integration.go_echo and writes the job_id to KBATCH_ITEST_MARKER.
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

func init() {
	kbatch.Register("integration.go_echo", func(ctx *kbatch.Context) error {
		marker := os.Getenv("KBATCH_ITEST_MARKER")
		if marker == "" {
			return nil
		}
		return os.WriteFile(marker, []byte(ctx.JobID), 0o644)
	})
}

func main() {
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		fmt.Fprintf(os.Stderr, "usage: kbatch-ittest serve --socket PATH\n")
		os.Exit(2)
	}

	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	socket := fs.String("socket", "", "Unix socket path (required)")
	_ = fs.Parse(os.Args[2:])
	if *socket == "" {
		fmt.Fprintf(os.Stderr, "--socket is required\n")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := kbatch.NewServer(kbatch.ServerConfig{SocketPath: *socket}).ListenAndServe(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch-ittest: %v\n", err)
		os.Exit(1)
	}
}
