package fairness

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

// Dispatcher consumes fair ingest messages and enqueues into the WFQ scheduler.
type Dispatcher struct {
	Lane       Lane
	Scheduler  *Scheduler
	OnStartFwd func(lane Lane)
	Now        func() time.Time
	OnExpired  func(ctx context.Context, raw []byte, src protocol.SourceCoords) error
}

// Outcome describes one ingest message.
type Outcome struct {
	CommitOffset bool
	Enqueued     bool
	Backpressure bool
	TenantID     string
}

func (d *Dispatcher) Process(ctx context.Context, raw []byte, src protocol.SourceCoords) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	if d.OnStartFwd != nil {
		d.OnStartFwd(d.Lane)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return out, nil
	}
	tenantID := TenantFromMessage(m)
	out.TenantID = tenantID

	if validTill, _ := m["valid_till"].(string); jobexpiry.Expired(validTill, d.now()) {
		if d.OnExpired != nil {
			if err := d.OnExpired(ctx, raw, src); err != nil {
				return out, err
			}
		}
		return out, nil
	}

	if d.Scheduler == nil {
		return out, nil
	}

	jobexpiry.StampSource(m, src)
	stamped, err := json.Marshal(m)
	if err != nil {
		return out, err
	}

	ok, err := d.Scheduler.Enqueue(ctx, tenantID, stamped)
	if err != nil {
		return out, err
	}
	if !ok {
		out.Backpressure = true
		out.CommitOffset = false
		return out, nil
	}
	out.Enqueued = true
	return out, nil
}

func (d *Dispatcher) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Coordinator starts forwarder goroutines per lane (idempotent).
type Coordinator struct {
	mu        sync.Mutex
	started   map[Lane]bool
	startFunc func(lane Lane)
}

func NewCoordinator(start func(lane Lane)) *Coordinator {
	return &Coordinator{started: map[Lane]bool{}, startFunc: start}
}

func (c *Coordinator) Ensure(lane Lane) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started[lane] {
		return
	}
	c.started[lane] = true
	if c.startFunc != nil {
		c.startFunc(lane)
	}
}

func (c *Coordinator) OnStart(lane Lane) func(Lane) {
	return func(l Lane) { c.Ensure(l) }
}
