package callback

import (
	"context"
	"encoding/json"
	"log"

	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
)

// Invoker runs batch callbacks (Ruby classes in legacy mode; log-only default).
type Invoker interface {
	Invoke(ctx context.Context, cb protocol.CallbackMessage) error
}

// LogInvoker records callback class names (Phase 3 default).
type LogInvoker struct{}

func (LogInvoker) Invoke(_ context.Context, cb protocol.CallbackMessage) error {
	log.Printf("[kbatch-daemon] callback batch_id=%s outcome=%s on_success=%s on_complete=%s",
		cb.BatchID, cb.Outcome, cb.OnSuccess, cb.OnComplete)
	return nil
}

// Processor claims and invokes batch callbacks.
type Processor struct {
	Store    *store.RedisStore
	Invoker  Invoker
	NodeID   string
}

type Outcome struct {
	CommitOffset bool
}

func (p *Processor) Process(ctx context.Context, raw []byte) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	var cb protocol.CallbackMessage
	if err := json.Unmarshal(raw, &cb); err != nil {
		return out, nil
	}
	if cb.BatchID == "" {
		return out, nil
	}
	dispatched, err := p.Store.CallbackDispatched(ctx, cb.BatchID)
	if err != nil {
		return out, err
	}
	if dispatched {
		return out, nil
	}
	if p.Invoker != nil {
		_ = p.Invoker.Invoke(ctx, cb)
	}
	_, _ = p.Store.ClaimCallback(ctx, cb.BatchID, p.NodeID)
	return out, nil
}
