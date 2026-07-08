package event

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
)

// Producer publishes Kafka messages.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Processor applies completion events to the batch ledger.
type Processor struct {
	Cfg      config.Daemon
	Store    *store.RedisStore
	Producer Producer
}

// Outcome per poll batch.
type Outcome struct {
	Callbacks []protocol.CallbackMessage
}

func (p *Processor) ProcessBatch(ctx context.Context, rawEvents [][]byte) (Outcome, error) {
	out := Outcome{}
	events := make([]store.CompletionEvent, 0, len(rawEvents))
	for _, raw := range rawEvents {
		var ev protocol.EventMessage
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		if ev.BatchID == "" || ev.BatchSeq <= 0 {
			continue
		}
		events = append(events, store.CompletionEvent{
			BatchID: ev.BatchID, JobID: ev.JobID, Status: ev.Status, BatchSeq: ev.BatchSeq,
		})
	}
	if len(events) == 0 {
		return out, nil
	}

	result, err := p.Store.RecordCompletionsBatch(ctx, events)
	if err != nil {
		return out, err
	}

	for _, fin := range result.Finished {
		if fin.Batch == nil {
			continue
		}
		cb := protocol.CallbackMessage{
			BatchID:        fin.Batch.ID,
			Outcome:        fin.Outcome,
			TotalJobs:      fin.Batch.TotalJobs,
			CompletedCount: fin.Batch.CompletedCount,
			FailedCount:    fin.Batch.FailedCount,
			OnSuccess:      fin.Batch.OnSuccess,
			OnComplete:     fin.Batch.OnComplete,
			FinishedAt:     fin.Batch.FinishedAt,
		}
		out.Callbacks = append(out.Callbacks, cb)
	}

	for _, batchID := range result.Replays {
		dispatched, err := p.Store.CallbackDispatched(ctx, batchID)
		if err != nil {
			return out, err
		}
		if dispatched {
			continue
		}
		batch, err := p.Store.FindBatch(ctx, batchID)
		if err != nil || batch == nil {
			continue
		}
		if batch.Status != "success" && batch.Status != "complete" {
			continue
		}
		outcome := batch.Status
		out.Callbacks = append(out.Callbacks, protocol.CallbackMessage{
			BatchID:        batch.ID,
			Outcome:        outcome,
			TotalJobs:      batch.TotalJobs,
			CompletedCount: batch.CompletedCount,
			FailedCount:    batch.FailedCount,
			OnSuccess:      batch.OnSuccess,
			OnComplete:     batch.OnComplete,
			FinishedAt:     batch.FinishedAt,
		})
	}

	for _, cb := range out.Callbacks {
		raw, _ := json.Marshal(cb)
		if err := p.Producer.Produce(ctx, p.Cfg.CallbacksTopic, cb.BatchID, raw); err != nil {
			return out, fmt.Errorf("callback produce: %w", err)
		}
	}
	return out, nil
}
