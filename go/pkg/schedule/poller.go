package schedule

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"sort"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
)

// Producer dispatches due jobs back to execution topics.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// PartitionedProducer optionally sends to an explicit partition.
type PartitionedProducer interface {
	ProducePartition(ctx context.Context, topic, key string, payload []byte, partition int32) error
}

// Router resolves a scheduled payload to a destination.
type Router interface {
	Route(payload map[string]interface{}) (Route, error)
}

// BatchCancelled reports whether a batch should be dropped.
type BatchCancelled func(ctx context.Context, batchID string) (bool, error)

// Poller drains the delayed-job index (Ruby SchedulePoller).
type Poller struct {
	Cfg       config.Daemon
	Store     IndexStore
	Reader    PayloadReader
	Producer  Producer
	Router    Router
	Cancelled BatchCancelled
	Now       func() time.Time

	lastReclaim time.Time
}

const maxReadMisses = 10

func (p *Poller) Tick(ctx context.Context) (int, error) {
	now := p.now()
	if p.Cfg.ScheduleReclaimEvery > 0 && now.Sub(p.lastReclaim) >= p.Cfg.ScheduleReclaimEvery {
		if _, err := p.Store.Reclaim(ctx, now); err != nil {
			return 0, err
		}
		p.lastReclaim = now
	}

	members, err := p.Store.ClaimDue(ctx, now, p.Cfg.ScheduleLeaseSeconds, p.Cfg.ScheduleBatchSize)
	if err != nil || len(members) == 0 {
		return 0, err
	}

	byPartition := map[int32][]int64{}
	parsed := make([]struct {
		member string
		Member
	}, 0, len(members))
	for _, m := range members {
		pm, ok := ParseMember(m)
		if !ok {
			continue
		}
		parsed = append(parsed, struct {
			member string
			Member
		}{m, pm})
		byPartition[pm.Partition] = append(byPartition[pm.Partition], pm.Offset)
	}
	for part := range byPartition {
		sort.Slice(byPartition[part], func(i, j int) bool { return byPartition[part][i] < byPartition[part][j] })
	}

	read, err := p.Reader.Read(ctx, byPartition)
	if err != nil {
		return 0, err
	}
	lostSet := map[string]struct{}{}
	for _, loc := range read.Lost {
		lostSet[loc] = struct{}{}
	}

	acked := 0
	done := make([]string, 0, len(parsed))
	for _, item := range parsed {
		loc := BuildKey(item.Partition, item.Offset)
		if _, lost := lostSet[loc]; lost {
			log.Printf("[kbatch-schedule] payload missing at %s/%s (retention) — dropping job_id=%s",
				p.Cfg.ScheduledTopic, loc, item.JobID)
			done = append(done, item.member)
			continue
		}

		raw, ok := read.Found[loc]
		if !ok {
			misses, _ := p.Store.RecordReadMiss(ctx, item.member)
			if misses >= maxReadMisses {
				_ = p.Store.ClearReadMiss(ctx, item.member)
				done = append(done, item.member)
			}
			continue
		}
		_ = p.Store.ClearReadMiss(ctx, item.member)

		if p.produceDue(ctx, raw, item.JobID) {
			acked++
			done = append(done, item.member)
		}
	}
	if len(done) > 0 {
		_ = p.Store.Ack(ctx, done)
	}
	return acked, nil
}

func (p *Poller) produceDue(ctx context.Context, raw []byte, jobID string) bool {
	var data map[string]interface{}
	if err := json.Unmarshal(raw, &data); err != nil {
		log.Printf("[kbatch-schedule] invalid payload job_id=%s: %v", jobID, err)
		return true
	}
	if p.Cfg.SkipCancelledJobs {
		if batchID, _ := data["batch_id"].(string); batchID != "" && p.Cancelled != nil {
			cancelled, err := p.Cancelled(ctx, batchID)
			if err == nil && cancelled {
				workerClass, _ := data["worker_class"].(string)
				instrument.JobCancelled(jobID, batchID, workerClass)
				return true
			}
		}
	}
	route, err := p.Router.Route(data)
	if err != nil {
		log.Printf("[kbatch-schedule] route job_id=%s: %v", jobID, err)
		return true
	}
	key := route.Key
	if key == "" {
		if k, ok := data["job_id"].(string); ok {
			key = k
		}
	}
	var produceErr error
	if route.Partition != nil {
		if pp, ok := p.Producer.(PartitionedProducer); ok {
			produceErr = pp.ProducePartition(ctx, route.Topic, key, raw, *route.Partition)
		} else {
			produceErr = p.Producer.Produce(ctx, route.Topic, key, raw)
		}
	} else {
		produceErr = p.Producer.Produce(ctx, route.Topic, key, raw)
	}
	if produceErr != nil {
		log.Printf("[kbatch-schedule] produce job_id=%s: %v", jobID, produceErr)
		return false
	}
	batchID, _ := data["batch_id"].(string)
	workerClass, _ := data["worker_class"].(string)
	if workerClass == "" {
		if jt, ok := data["job_type"].(string); ok {
			workerClass = jt
		}
	}
	instrument.ScheduledDispatched(jobID, batchID, workerClass, route.Topic)
	return true
}

func (p *Poller) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// Run blocks until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	interval := p.Cfg.SchedulePollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	cap := p.Cfg.SchedulePollMaxInterval
	if cap <= 0 {
		cap = 60 * time.Second
	}
	if cap < interval {
		cap = interval
	}
	wait := interval
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := p.Tick(ctx)
		if err != nil {
			log.Printf("[kbatch-schedule] tick error: %v", err)
			time.Sleep(p.jittered(wait))
			wait = min(wait*2, cap)
			continue
		}
		if n == 0 {
			time.Sleep(p.jittered(wait))
			wait = min(wait*2, cap)
		} else {
			wait = interval
		}
	}
}

func (p *Poller) jittered(d time.Duration) time.Duration {
	j := p.Cfg.SchedulePollJitter
	if j <= 0 {
		return d
	}
	f := 1 + ((rand.Float64()*2)-1)*j
	return time.Duration(float64(d) * f)
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
