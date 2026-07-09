package client

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
)

// BatchOptions configures batch creation.
type BatchOptions struct {
	ID          string
	OnSuccess   string
	OnComplete  string
	Meta        map[string]interface{}
	Description string
	TenantID    string
}

// PushOptions configures a single enqueue/push.
type PushOptions struct {
	JobID     string
	TenantID  string
	ValidTill string
}

func (o PushOptions) jobID() string {
	if o.JobID != "" {
		return o.JobID
	}
	return uuid.NewString()
}

func (o PushOptions) tenantID(batchDefault string) string {
	if o.TenantID != "" {
		return o.TenantID
	}
	return batchDefault
}

// Batch is one open batch ledger (Ruby KafkaBatch::Batch).
type Batch struct {
	client      *Client
	id          string
	onSuccess   string
	onComplete  string
	meta        map[string]interface{}
	description string
	tenantID    string
	seqCursor   int64
	seqEnd      int64
}

// ID returns the batch uuid.
func (b *Batch) ID() string { return b.id }

// PushJob enqueues one manifest job into this batch.
func (b *Batch) PushJob(ctx context.Context, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	entry, err := b.client.lookupHandler(jobType)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := b.client.claimUniq(ctx, entry, jobType, payload, jobID, b.id); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	seq, err := b.reserve(ctx, 1)
	if err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		return "", err
	}
	msg, err := b.client.buildMessage(entry, jobType, payload, jobID, &b.id, opts, &seq)
	if err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	route := b.client.routeFor(entry, jobID, opts.tenantID(b.tenantID), &b.id)
	if err := b.client.produceJob(ctx, route, msg); err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, msg.UniqFP)
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	return jobID, nil
}

// PushJobAt schedules one manifest job into this batch.
func (b *Batch) PushJobAt(ctx context.Context, runAt interface{}, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	entry, err := b.client.lookupHandler(jobType)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := b.client.claimUniq(ctx, entry, jobType, payload, jobID, b.id); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	seq, err := b.reserve(ctx, 1)
	if err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		return "", err
	}
	msg, err := b.client.buildMessage(entry, jobType, payload, jobID, &b.id, opts, &seq)
	if err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, "")
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	if err := b.client.scheduleMessage(ctx, msg, clampRunAt(runAt, b.client.cfg.MaxScheduleHorizon), b.id); err != nil {
		b.client.releaseUniq(entry, jobType, payload, jobID, msg.UniqFP)
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	return jobID, nil
}

// Seal opens the completion gate (Ruby seal!).
func (b *Batch) Seal(ctx context.Context) (*SealResult, error) {
	result, err := b.client.store.SealBatch(ctx, b.id)
	if err != nil {
		return nil, err
	}
	switch result.Status {
	case "not_found":
		return nil, BatchNotFoundError{BatchID: b.id}
	case "done":
		if result.Batch != nil {
			if err := b.client.produceCallback(ctx, result.Batch, result.Outcome); err != nil {
				return &SealResult{Status: result.Status, Outcome: result.Outcome}, fmt.Errorf("callback produce: %w", err)
			}
		}
	}
	row, _ := b.client.store.FindBatch(ctx, b.id)
	total := int64(0)
	if row != nil {
		total = row.TotalJobs
	}
	instrument.BatchSealed(b.id, total)
	return &SealResult{Status: result.Status, Outcome: result.Outcome}, nil
}

// SealResult summarizes a seal operation.
type SealResult struct {
	Status  string
	Outcome string
}

func (b *Batch) reserve(ctx context.Context, count int64) (int64, error) {
	res, err := b.client.store.AddJobs(ctx, b.id, count)
	if err != nil {
		return 0, err
	}
	switch res.Status {
	case "closed":
		return 0, BatchClosedError{BatchID: b.id, Reason: "closed"}
	case "cancelled":
		return 0, BatchClosedError{BatchID: b.id, Reason: "cancelled"}
	case "not_found":
		return 0, BatchNotFoundError{BatchID: b.id}
	}
	if count == 1 {
		b.seqCursor = res.SeqStart
		b.seqEnd = res.SeqEnd
		return res.SeqStart, nil
	}
	b.seqCursor = res.SeqStart
	b.seqEnd = res.SeqEnd
	return 0, nil
}
