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

// DLTProducer publishes failed callback payloads.
type DLTProducer interface {
	ProduceDLT(ctx context.Context, key string, payload []byte) error
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
	Store   *store.RedisStore
	Invoker Invoker
	DLT     DLTProducer
	NodeID  string
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
		if err := p.Invoker.Invoke(ctx, cb); err != nil {
			log.Printf("[kbatch-daemon] callback invoke batch_id=%s: %v", cb.BatchID, err)
			if p.DLT != nil {
				dlt := map[string]interface{}{
					"batch_id":           cb.BatchID,
					"dlt_type":           "callback_error",
					"dlt_error_message":  err.Error(),
					"on_success":         cb.OnSuccess,
					"on_complete":        cb.OnComplete,
					"outcome":            cb.Outcome,
				}
				rawDLT, _ := json.Marshal(dlt)
				_ = p.DLT.ProduceDLT(ctx, cb.BatchID, rawDLT)
			}
		}
	}
	_, _ = p.Store.ClaimCallback(ctx, cb.BatchID, p.NodeID)
	return out, nil
}
