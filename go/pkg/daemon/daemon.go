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
	"github.com/y-shashank/kafka-batch/go/pkg/consumption"
	"github.com/y-shashank/kafka-batch/go/pkg/control/callback"
	"github.com/y-shashank/kafka-batch/go/pkg/control/event"
	"github.com/y-shashank/kafka-batch/go/pkg/control/job"
	"github.com/y-shashank/kafka-batch/go/pkg/control/retry"
	"github.com/y-shashank/kafka-batch/go/pkg/fairness"
	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch/go/pkg/metrics"
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
	defaultTopic := prefixOr(cfg.TopicPrefix, "") + "kafka_batch.jobs"
	if err := manifest.Validate(defaultTopic); err != nil {
		return err
	}
	if err := cfg.ValidateFairReadySplit(manifest); err != nil {
		return err
	}
	if manifest.HasRubyHandlers() && cfg.RubyWorkerSocket == "" {
		return fmt.Errorf("handler manifest includes runtime:ruby handlers but ruby_worker_socket is not set")
	}
	if manifest.HasGoHandlers() {
		log.Printf("kbatch daemon: runtime:go handlers are executed by kbatch worker (control plane does not run Go jobs)")
	}
	if err := metrics.Install(metrics.FromDaemon(cfg)); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	defer metrics.Reset()

	jobTopics := manifest.JobTopicsForRuntime(config.RuntimeRuby, defaultTopic)
	if len(cfg.JobsTopics) > 0 {
		for _, t := range cfg.JobsTopics {
			if manifest.TopicRuntime(t, defaultTopic) == config.RuntimeRuby {
				jobTopics = append(jobTopics, t)
			}
		}
	}
	jobTopics = uniqueTopicNames(jobTopics)

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
	rubyPrio := rubyPriorityConfigs(prioReg, manifest, defaultTopic)
	if len(jobTopics) == 0 && len(rubyPrio) == 0 && !manifest.HasRubyHandlers() {
		log.Printf("kbatch daemon: no ruby job topics (go handlers only — control plane + fair dispatch)")
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

	jobProc := &job.Processor{
		Cfg:      cfg,
		Manifest: manifest,
		Store:    st,
		Producer: prod,
	}
	if cfg.RubyWorkerSocket != "" {
		jobProc.RubyExec = job.RubySocketExecutor{
			SocketPath: cfg.RubyWorkerSocket,
			Timeout:    cfg.RubyWorkerTimeout,
		}
	}
	handleJob := BuildJobHandler(cfg, prod, jobProc)
	eventProc := &event.Processor{Cfg: cfg, Store: st, Producer: prod}
	retryProc := &retry.Processor{Producer: prod, MaxPause: cfg.RetryMaxPause}

	var cbInvoker callback.Invoker = callback.LogInvoker{}
	if cfg.RubyCallbackSocket != "" {
		cbInvoker = callback.RubySocketInvoker{SocketPath: cfg.RubyCallbackSocket}
	}
	cbProc := &callback.Processor{
		Store: st, Invoker: cbInvoker, NodeID: cfg.NodeID,
		DLT: callbackDLT{prod: prod, topic: cfg.DeadLetterTopic},
	}
	pauseCtl := consumption.NewControl(rdb, cfg.ConsumptionControlRefreshInterval)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 8)
	if len(jobTopics) > 0 {
		go RunConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-jobs", jobTopics, handleJob, errCh, pauseCtl)
	}

	lagClient, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return fmt.Errorf("lag client: %w", err)
	}
	defer lagClient.Close()
	priorityLag := priority.NewLagReader(lagClient)
	for _, pc := range rubyPrio {
		gate := priority.NewGate(priorityLag, cfg.PriorityLagCheckInterval)
		gate.Consumption = pauseCtl
		go RunPriorityGroup(ctx, cfg, pc, gate, handleJob, errCh, pauseCtl)
	}
	if len(rubyPrio) > 0 {
		log.Printf("kbatch priority consumers enabled groups=%d (ruby topics only)", len(rubyPrio))
	}

	go RunConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-events", []string{cfg.EventsTopic}, func(rec *kgo.Record) error {
		_, err := eventProc.ProcessBatch(ctx, [][]byte{rec.Value})
		return err
	}, errCh, nil)

	retryTopics := cfg.RetryTopics()
	if len(retryTopics) > 0 {
		go RunConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-retry", retryTopics, func(rec *kgo.Record) error {
			src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
			out, err := retryProc.Process(ctx, rec.Value, src)
			if err != nil {
				return err
			}
			return applyRetryOutcome(ctx, cfg, prod, out, src)
		}, errCh, pauseCtl)
	}

	go RunConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-callbacks", []string{cfg.CallbacksTopic}, func(rec *kgo.Record) error {
		_, err := cbProc.Process(ctx, rec.Value)
		return err
	}, errCh, pauseCtl)

	if cfg.SchedulePollerEnabled {
		var schedStore schedule.IndexStore
		var mysqlSched *schedule.MysqlStore
		switch strings.ToLower(cfg.ScheduleStore) {
		case "mysql":
			ms, err := schedule.NewMysqlStore(cfg.ScheduleMySQLDSN, cfg.ScheduleBatchSize*5)
			if err != nil {
				return fmt.Errorf("schedule mysql store: %w", err)
			}
			mysqlSched = ms
			schedStore = ms
			defer mysqlSched.Close()
		default:
			schedStore = schedule.NewRedisStore(rdb, cfg.ScheduleBatchSize*5)
		}
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
		wireFairLane(ctx, cfg, manifest, rdb, prod, st, jobProc, handleJob, errCh, pauseCtl,
			fairness.LaneTime, cfg.FairnessTimeIngest, cfg.FairnessTimeSettings())
		wireFairLane(ctx, cfg, manifest, rdb, prod, st, jobProc, handleJob, errCh, pauseCtl,
			fairness.LaneThroughput, cfg.FairnessThroughputIngest, cfg.FairnessThroughputSettings())
		log.Printf("kbatch fairness enabled time=%s throughput=%s (ruby ready consumers only)",
			cfg.FairnessTimeIngest, cfg.FairnessThroughputIngest)
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

func wireFairLane(
	ctx context.Context,
	cfg config.Daemon,
	manifest config.Manifest,
	rdb *redis.Client,
	prod *kafkaclient.Client,
	st *store.RedisStore,
	jobProc *job.Processor,
	handleJob func(*kgo.Record) error,
	errCh chan<- error,
	pauseCtl *consumption.Control,
	lane fairness.Lane,
	ingest string,
	settings fairness.Settings,
) {
	sched := fairness.NewScheduler(rdb, settings)
	if lane == fairness.LaneTime {
		jobProc.FairTime = sched
	} else {
		jobProc.FairThroughput = sched
	}
	laneName := string(lane)
	resolveReady := fairReadyResolver(manifest, cfg, laneName)
	readyTopics := controlFairReadyTopics(cfg, manifest, laneName)
	expPub := newExpiredPublisher(cfg, prod, st)
	coord := fairness.NewCoordinator(func(l fairness.Lane) {
		if l != lane {
			return
		}
		fwd := &fairness.Forwarder{
			Lane: l, Scheduler: sched, ResolveReadyTopic: resolveReady, Producer: prod,
			OnExpired: func(ctx context.Context, _ *fairness.CheckoutResult, raw []byte) error {
				var m map[string]interface{}
				_ = json.Unmarshal(raw, &m)
				src := jobexpiry.SourceCoords(m)
				return expPub.publish(ctx, raw, src)
			},
		}
		if len(readyTopics) == 1 {
			fwd.ReadyTopic = readyTopics[0]
		}
		go fwd.Run(ctx)
	})
	disp := &fairness.Dispatcher{
		Lane: lane, Scheduler: sched, OnStartFwd: coord.OnStart(lane),
		OnExpired: func(ctx context.Context, raw []byte, src protocol.SourceCoords) error {
			return expPub.publish(ctx, raw, src)
		},
	}
	suffix := string(lane)
	go RunConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-fair-dispatch-"+suffix,
		[]string{ingest}, func(rec *kgo.Record) error {
			src := protocol.SourceCoords{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}
			out, err := disp.Process(ctx, rec.Value, src)
			if err != nil {
				return err
			}
			if !out.CommitOffset {
				return fmt.Errorf("fair ingest backpressure lane=%s tenant=%s", lane, out.TenantID)
			}
			return nil
		}, errCh, pauseCtl)
	if len(readyTopics) > 0 {
		go RunConsumer(ctx, cfg.Brokers, cfg.ConsumerGroup+"-fair-ready-"+suffix,
			readyTopics, handleJob, errCh, pauseCtl)
	}
}

func controlFairReadyTopics(cfg config.Daemon, manifest config.Manifest, lane string) []string {
	topics := cfg.FairReadyTopics(lane)
	if cfg.RuntimeSplitFairReady(lane) {
		if topics.Ruby != "" {
			return []string{topics.Ruby}
		}
		return nil
	}
	if topics.Legacy != "" && manifest.HasFairHandlersForRuntime(config.RuntimeRuby, lane) {
		return []string{topics.Legacy}
	}
	return nil
}

func rubyPriorityConfigs(reg priority.Registry, manifest config.Manifest, defaultTopic string) []priority.Config {
	var out []priority.Config
	for _, pc := range reg.Configs {
		topics := manifest.FilterTopicsForRuntime(config.RuntimeRuby, pc.Topics, defaultTopic)
		if len(topics) == 0 {
			continue
		}
		out = append(out, pc.WithTopics(topics))
	}
	return out
}

func uniqueTopicNames(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

type callbackDLT struct {
	prod  *kafkaclient.Client
	topic string
}

func (d callbackDLT) ProduceDLT(ctx context.Context, key string, payload []byte) error {
	return d.prod.Produce(ctx, d.topic, key, payload)
}

func RunPriorityGroup(ctx context.Context, cfg config.Daemon, pc priority.Config, gate *priority.Gate, handle func(*kgo.Record) error, errCh chan<- error, pauseCtl *consumption.Control) {
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
			if pauseCtl != nil && pauseCtl.Paused(ctx, pc.ConsumerGroup, rec.Topic, rec.Partition) {
				time.Sleep(yieldSleep)
				return
			}
			tick := weightedTicks[rec.Topic]
			if yield, _ := priority.ShouldYield(spec, gate, &tick, ctx); yield {
				weightedTicks[rec.Topic] = tick
				p0 := ""
				if len(spec.HigherTopics) > 0 {
					p0 = spec.HigherTopics[0]
				}
				instrument.ConsumerPriorityYielded(
					"kbatch.priority", p0, spec.ConsumerGroup,
					yieldSleep.Milliseconds(), string(spec.Mode), spec.Rank, spec.HigherTopics,
				)
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

func RunConsumer(ctx context.Context, brokers []string, group string, topics []string, handle func(*kgo.Record) error, errCh chan<- error, pauseCtl *consumption.Control) {
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
			if pauseCtl != nil && pauseCtl.Paused(ctx, group, rec.Topic, rec.Partition) {
				time.Sleep(time.Second)
				return
			}
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
