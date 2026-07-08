package fairness

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/jobexpiry"
)

// Producer publishes to Kafka.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Forwarder checks out fairly-selected jobs and produces to the ready topic.
type Forwarder struct {
	Lane       Lane
	Scheduler  *Scheduler
	ReadyTopic string
	Producer   Producer
	IdleSleep  time.Duration
	Burst      int
	Now        func() time.Time
	OnExpired  func(ctx context.Context, job *CheckoutResult, raw []byte) error

	lastLeaseReclaim   time.Time
	lastForwardReclaim time.Time
}

const (
	defaultIdleSleep = 50 * time.Millisecond
	defaultBurst     = 50
	reclaimInterval  = 30 * time.Second
)

// ForwardOnce forwards one job when available.
func (f *Forwarder) ForwardOnce(ctx context.Context) bool {
	if f.Scheduler == nil {
		return false
	}
	job, err := f.Scheduler.Checkout(ctx)
	if err != nil || job == nil {
		return false
	}
	if f.handleExpired(ctx, job) {
		return true
	}
	if err := f.forwardJob(ctx, job); err != nil {
		log.Printf("[kbatch-fair-forwarder] forward error lane=%s: %v", f.Lane, err)
		return false
	}
	return true
}

func (f *Forwarder) handleExpired(ctx context.Context, job *CheckoutResult) bool {
	var m map[string]interface{}
	if err := json.Unmarshal(job.Payload, &m); err != nil {
		return false
	}
	validTill, _ := m["valid_till"].(string)
	if !jobexpiry.Expired(validTill, f.now()) {
		return false
	}
	_ = f.Scheduler.Complete(ctx, job.TenantID, job.SlotID, 0)
	_, _ = f.Scheduler.ConfirmForward(ctx, job.SlotID)
	if f.OnExpired != nil {
		if err := f.OnExpired(ctx, job, job.Payload); err != nil {
			log.Printf("[kbatch-fair-forwarder] expired drop lane=%s: %v", f.Lane, err)
		}
	}
	return true
}

func (f *Forwarder) forwardJob(ctx context.Context, job *CheckoutResult) error {
	marked, key, err := markSlot(job.Payload, job.TenantID, job.SlotID, f.Lane)
	if err != nil {
		_, _ = f.Scheduler.AbortForward(ctx, job.SlotID, job.TenantID)
		return err
	}
	if key == "" {
		key = job.TenantID
	}
	if err := f.Producer.Produce(ctx, f.ReadyTopic, key, marked); err != nil {
		_, _ = f.Scheduler.AbortForward(ctx, job.SlotID, job.TenantID)
		return err
	}
	_, err = f.Scheduler.ConfirmForward(ctx, job.SlotID)
	return err
}

// Run blocks until ctx is cancelled.
func (f *Forwarder) Run(ctx context.Context) {
	idle := f.IdleSleep
	if idle <= 0 {
		idle = defaultIdleSleep
	}
	burst := f.Burst
	if burst <= 0 {
		burst = defaultBurst
	}
	log.Printf("[kbatch-fair-forwarder] started lane=%s ready=%s", f.Lane, f.ReadyTopic)
	for {
		select {
		case <-ctx.Done():
			log.Printf("[kbatch-fair-forwarder] stopped lane=%s", f.Lane)
			return
		default:
		}
		forwarded := 0
		for forwarded < burst {
			if !f.ForwardOnce(ctx) {
				break
			}
			forwarded++
		}
		f.maybeReclaim(ctx)
		if forwarded == 0 {
			time.Sleep(idle)
		}
	}
}

func (f *Forwarder) maybeReclaim(ctx context.Context) {
	now := time.Now()
	if now.Sub(f.lastLeaseReclaim) >= reclaimInterval {
		f.lastLeaseReclaim = now
		if n, err := f.Scheduler.ReclaimExpiredLeases(ctx); err == nil && n > 0 {
			log.Printf("[kbatch-fair-forwarder] reclaimed %d expired lease(s) lane=%s", n, f.Lane)
		}
	}
	if now.Sub(f.lastForwardReclaim) >= reclaimInterval {
		f.lastForwardReclaim = now
		stale, err := f.Scheduler.ListStaleForwards(ctx)
		if err != nil {
			return
		}
		for _, e := range stale {
			_ = f.Scheduler.ReclaimStaleForward(ctx, e, func(payload []byte, key string) error {
				return f.Producer.Produce(ctx, f.ReadyTopic, key, payload)
			})
		}
	}
}

func (f *Forwarder) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}
