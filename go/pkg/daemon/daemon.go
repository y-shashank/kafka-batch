package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/control/callback"
	"github.com/y-shashank/kafka-batch/go/pkg/control/event"
	"github.com/y-shashank/kafka-batch/go/pkg/control/job"
	"github.com/y-shashank/kafka-batch/go/pkg/control/retry"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
)

// Run starts the Phase 3 control plane (plain topics + events + retries + callbacks).
func Run(ctx context.Context, cfgPath, manifestPath string) error {
	cfg, err := config.LoadDaemon(cfgPath)
	if err != nil {
		return err
	}
	if manifestPath != "" {
		cfg.HandlerManifest = manifestPath
	}
	manifest, err := config.LoadManifest(cfg.HandlerManifest, cfg.TopicPrefix)
	if err != nil {
		return err
	}
	config.SetHandlerLookup(func(jt string) bool { _, ok := kbatch.Lookup(jt); return ok })
	if err := manifest.Validate(); err != nil {
		return err
	}

	jobTopics := cfg.JobsTopics
	if len(jobTopics) == 0 {
		jobTopics = manifest.JobTopics(prefixOr(cfg.TopicPrefix, "") + "kafka_batch.jobs")
	}
	if len(jobTopics) == 0 {
		return fmt.Errorf("no job topics configured (set jobs_topics or handler manifest)")
	}

	rOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(rOpts)
	defer rdb.Close()

	st := store.NewRedisStore(rdb, cfg.BatchTTL)
	prod, err := kafkaclient.New(cfg.Brokers)
	if err != nil {
		return err
	}
	defer prod.Close()

	jobProc := &job.Processor{Cfg: cfg, Store: st, Producer: prod}
	eventProc := &event.Processor{Cfg: cfg, Store: st, Producer: prod}
	retryProc := &retry.Processor{Producer: prod, MaxPause: cfg.RetryMaxPause}
	cbProc := &callback.Processor{Store: st, Invoker: callback.LogInvoker{}, NodeID: cfg.NodeID}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 4)
	go runConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-jobs", jobTopics, func(rec *kgo.Record) error {
		src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
		out, err := jobProc.Process(ctx, rec.Value, src)
		if err != nil {
			return err
		}
		if out.Event != nil {
			raw, _ := json.Marshal(out.Event)
			key := fmt.Sprintf("%s/%d", out.Event.SrcTopic, out.Event.SrcPartition)
			if err := prod.Produce(ctx, cfg.EventsTopic, key, raw); err != nil {
				return err
			}
		}
		if out.RetryPayload != nil {
			if err := prod.Produce(ctx, out.RetryTopic, out.RetryKey, out.RetryPayload); err != nil {
				return err
			}
		}
		if out.DLTPayload != nil {
			if err := prod.Produce(ctx, cfg.DeadLetterTopic, out.DLTKey, out.DLTPayload); err != nil {
				return err
			}
		}
		if !out.CommitOffset {
			return fmt.Errorf("job not committed")
		}
		return nil
	}, errCh)

	go runConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-events", []string{cfg.EventsTopic}, func(rec *kgo.Record) error {
		_, err := eventProc.ProcessBatch(ctx, [][]byte{rec.Value})
		return err
	}, errCh)

	retryTopics := cfg.RetryTopics()
	if len(retryTopics) > 0 {
		go runConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-retry", retryTopics, func(rec *kgo.Record) error {
			src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
			out, err := retryProc.Process(ctx, rec.Value, src)
			if err != nil {
				return err
			}
			if out.Event != nil {
				raw, _ := json.Marshal(out.Event)
				key := fmt.Sprintf("%s/%d", out.Event.SrcTopic, out.Event.SrcPartition)
				if err := prod.Produce(ctx, cfg.EventsTopic, key, raw); err != nil {
					return err
				}
			}
			if out.ProduceBody != nil {
				if err := prod.Produce(ctx, out.ProduceTopic, out.ProduceKey, out.ProduceBody); err != nil {
					return err
				}
			}
			if out.DLTPayload != nil {
				if err := prod.Produce(ctx, cfg.DeadLetterTopic, out.DLTKey, out.DLTPayload); err != nil {
					return err
				}
			}
			if out.Pause {
				time.Sleep(out.PauseFor)
				return fmt.Errorf("retry paused")
			}
			return nil
		}, errCh)
	}

	go runConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-callbacks", []string{cfg.CallbacksTopic}, func(rec *kgo.Record) error {
		_, err := cbProc.Process(ctx, rec.Value)
		return err
	}, errCh)

	log.Printf("kbatch daemon running group=%s topics=%v", cfg.ConsumerGroup, jobTopics)
	if ready := os.Getenv("KBATCH_DAEMON_READY_FILE"); ready != "" {
		_ = os.WriteFile(ready, []byte("ok\n"), 0o644)
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func runConsumer(ctx context.Context, brokers []string, group string, topics []string, handle func(*kgo.Record) error, errCh chan<- error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		errCh <- err
		return
	}
	defer cl.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != nil {
					errCh <- e.Err
					return
				}
			}
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if err := handle(rec); err != nil {
				log.Printf("[kbatch-daemon] handler error topic=%s offset=%d: %v", rec.Topic, rec.Offset, err)
				return
			}
			cl.MarkCommitRecords(rec)
		})
		cl.AllowRebalance()
	}
}

func prefixOr(prefix, base string) string {
	if prefix == "" {
		return base
	}
	if strings.HasPrefix(base, prefix+".") {
		return base
	}
	return prefix + "." + base
}

// Main is a helper for cmd/kbatch daemon when no signal context provided.
func Main(cfgPath, manifestPath string) {
	if err := Run(context.Background(), cfgPath, manifestPath); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch daemon: %v\n", err)
		os.Exit(1)
	}
}
