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
	"github.com/y-shashank/kafka-batch/go/pkg/fairness"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch/go/pkg/priority"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/schedule"
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

	var prioReg priority.Registry
	if len(cfg.PriorityConfigPaths) > 0 {
		var err error
		prioReg, err = priority.LoadRegistry(cfg.PriorityConfigPaths, cfg, cfg.JobsTopics)
		if err != nil {
			return fmt.Errorf("priority config: %w", err)
		}
		reserved := map[string]struct{}{}
		for _, t := range prioReg.AllTopics() {
			reserved[t] = struct{}{}
		}
		filtered := make([]string, 0, len(jobTopics))
		for _, t := range jobTopics {
			if _, skip := reserved[t]; skip {
				continue
			}
			filtered = append(filtered, t)
		}
		jobTopics = filtered
	}
	if len(jobTopics) == 0 && len(prioReg.Configs) == 0 {
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
	handleJob := func(rec *kgo.Record) error {
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
	}
	eventProc := &event.Processor{Cfg: cfg, Store: st, Producer: prod}
	retryProc := &retry.Processor{Producer: prod, MaxPause: cfg.RetryMaxPause}
	cbProc := &callback.Processor{Store: st, Invoker: callback.LogInvoker{}, NodeID: cfg.NodeID}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 8)
	if len(jobTopics) > 0 {
		go runConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-jobs", jobTopics, handleJob, errCh)
	}

	lagClient, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return fmt.Errorf("lag client: %w", err)
	}
	defer lagClient.Close()
	priorityLag := priority.NewLagReader(lagClient)
	for _, pc := range prioReg.Configs {
		gate := priority.NewGate(priorityLag, cfg.PriorityLagCheckInterval)
		go runPriorityGroup(ctx, cfg, pc, gate, handleJob, errCh)
	}
	if len(prioReg.Configs) > 0 {
		log.Printf("kbatch priority consumers enabled groups=%d", len(prioReg.Configs))
	}

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

	if cfg.SchedulePollerEnabled {
		schedStore := schedule.NewRedisStore(rdb, cfg.ScheduleBatchSize*5)
		reader, err := schedule.NewReader(cfg.Brokers, cfg.ScheduledTopic)
		if err != nil {
			return fmt.Errorf("schedule reader: %w", err)
		}
		defer reader.Close()
		defaultTopic := ""
		if len(jobTopics) > 0 {
			defaultTopic = jobTopics[0]
		} else if topics := prioReg.AllTopics(); len(topics) > 0 {
			defaultTopic = topics[0]
		}
		poller := &schedule.Poller{
			Cfg:      cfg,
			Store:    schedStore,
			Reader:   reader,
			Producer: prod,
			Router: schedule.DaemonRouter{
				Manifest: manifest,
				Cfg:      cfg,
				Default:  defaultTopic,
			},
			Cancelled: st.BatchCancelled,
		}
		go poller.Run(ctx)
		log.Printf("kbatch schedule poller enabled topic=%s", cfg.ScheduledTopic)
	}

	if cfg.FairnessEnabled {
		fairSched := fairness.NewScheduler(rdb, cfg.FairnessTimeSettings())
		jobProc.FairTime = fairSched
		coord := fairness.NewCoordinator(func(lane fairness.Lane) {
			if lane != fairness.LaneTime {
				return
			}
			fwd := &fairness.Forwarder{
				Lane: lane, Scheduler: fairSched,
				ReadyTopic: cfg.FairnessTimeReady, Producer: prod,
			}
			go fwd.Run(ctx)
		})
		fairDisp := &fairness.Dispatcher{
			Lane: fairness.LaneTime, Scheduler: fairSched,
			OnStartFwd: coord.OnStart(fairness.LaneTime),
		}
		go runConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-fair-dispatch-time",
			[]string{cfg.FairnessTimeIngest}, func(rec *kgo.Record) error {
				out, err := fairDisp.Process(ctx, rec.Value)
				if err != nil {
					return err
				}
				if !out.CommitOffset {
					return fmt.Errorf("fair ingest backpressure tenant=%s", out.TenantID)
				}
				return nil
			}, errCh)
		go runConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-fair-ready-time",
			[]string{cfg.FairnessTimeReady}, handleJob, errCh)
		log.Printf("kbatch fairness enabled ingest=%s ready=%s", cfg.FairnessTimeIngest, cfg.FairnessTimeReady)
	}

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

func runPriorityGroup(ctx context.Context, cfg config.Daemon, pc priority.Config, gate *priority.Gate, handle func(*kgo.Record) error, errCh chan<- error) {
	specByTopic := map[string]priority.TopicSpec{}
	for _, s := range pc.TopicSpecs() {
		specByTopic[s.Topic] = s
	}
	weightedTicks := map[string]int{}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(pc.ConsumerGroup),
		kgo.ConsumeTopics(pc.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		errCh <- err
		return
	}
	defer cl.Close()

	yieldSleep := cfg.PriorityLagCheckInterval
	if yieldSleep <= 0 {
		yieldSleep = 2 * time.Second
	}

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
			spec, ok := specByTopic[rec.Topic]
			if !ok {
				return
			}
			tick := weightedTicks[rec.Topic]
			if yield, _ := priority.ShouldYield(spec, gate, &tick, ctx); yield {
				weightedTicks[rec.Topic] = tick
				time.Sleep(yieldSleep)
				return
			}
			weightedTicks[rec.Topic] = tick
			if err := handle(rec); err != nil {
				log.Printf("[kbatch-priority] handler error topic=%s offset=%d: %v", rec.Topic, rec.Offset, err)
				return
			}
			cl.MarkCommitRecords(rec)
		})
		cl.AllowRebalance()
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
