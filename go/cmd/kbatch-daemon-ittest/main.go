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

func init() {
	kbatch.Register("integration.go_daemon", func(ctx *kbatch.Context) error {
		if marker := os.Getenv("KBATCH_DAEMON_ITEST_MARKER"); marker != "" {
			return os.WriteFile(marker, []byte(ctx.JobID), 0o644)
		}
		return nil
	})

	kbatch.Register("integration.go_retry_once", func(ctx *kbatch.Context) error {
		if ctx.Attempt < 1 {
			return &kbatch.HandlerError{Class: "Transient", Message: "fail on first attempt"}
		}
		if marker := os.Getenv("KBATCH_DAEMON_ITEST_MARKER"); marker != "" {
			return os.WriteFile(marker, []byte(ctx.JobID), 0o644)
		}
		return nil
	})

	kbatch.Register("integration.go_always_fail", func(ctx *kbatch.Context) error {
		return &kbatch.HandlerError{Class: "Permanent", Message: "always fails"}
	})

	kbatch.Register("integration.go_multi", func(ctx *kbatch.Context) error {
		n, _ := ctx.Payload["n"].(float64)
		if n <= 0 {
			return &kbatch.HandlerError{Class: "Invalid", Message: "missing n"}
		}
		return nil
	})

	kbatch.Register("integration.go_fair", func(ctx *kbatch.Context) error {
		if marker := os.Getenv("KBATCH_DAEMON_ITEST_MARKER"); marker != "" {
			tenant, _ := ctx.Payload["tenant"].(string)
			return os.WriteFile(marker, []byte(ctx.JobID+":"+tenant), 0o644)
		}
		return nil
	})

	kbatch.Register("integration.go_scheduled", func(ctx *kbatch.Context) error {
		if marker := os.Getenv("KBATCH_DAEMON_ITEST_MARKER"); marker != "" {
			return os.WriteFile(marker, []byte(ctx.JobID), 0o644)
		}
		return nil
	})

	kbatch.Register("integration.go_p0", func(ctx *kbatch.Context) error {
		if marker := os.Getenv("KBATCH_DAEMON_ITEST_MARKER_P0"); marker != "" {
			return os.WriteFile(marker, []byte(ctx.JobID), 0o644)
		}
		return nil
	})

	kbatch.Register("integration.go_p1", func(ctx *kbatch.Context) error {
		if marker := os.Getenv("KBATCH_DAEMON_ITEST_MARKER"); marker != "" {
			return os.WriteFile(marker, []byte(ctx.JobID), 0o644)
		}
		return nil
	})
}

func main() {
	fs := flag.NewFlagSet("kbatch-daemon-ittest", flag.ExitOnError)
	cfg := fs.String("config", "", "daemon config YAML")
	manifest := fs.String("manifest", "", "handler manifest YAML")
	_ = fs.Parse(os.Args[1:])

	if *cfg == "" {
		fmt.Fprintln(os.Stderr, "--config required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := daemon.Run(ctx, *cfg, *manifest); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch-daemon-ittest: %v\n", err)
		os.Exit(1)
	}
}
