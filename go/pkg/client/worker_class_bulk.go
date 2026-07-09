package client

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/uniq"
)

type workerPushPlan struct {
	jobID   string
	payload map[string]interface{}
	fp      string
}

func (c *Client) planWorkerPushes(ctx context.Context, workerClass string, payloads []map[string]interface{}, batchID string) (string, config.HandlerEntry, []workerPushPlan, []string, error) {
	jobType, entry, err := c.lookupWorkerClass(workerClass)
	if err != nil {
		return "", entry, nil, nil, err
	}
	jobIDs := make([]string, len(payloads))
	plans := make([]workerPushPlan, 0, len(payloads))
	for i, payload := range payloads {
		if payload == nil {
			payload = map[string]interface{}{}
		}
		jobID := uuid.NewString()
		skipped, err := c.claimUniqWorker(ctx, entry, workerClass, payload, jobID, batchID)
		if err != nil {
			return "", entry, nil, nil, err
		}
		if skipped {
			jobIDs[i] = ""
			continue
		}
		fp := ""
		if entry.Uniq && c.cfg.UniqEnabled {
			fp = uniq.DigestHex(workerClass, payload)
		}
		plans = append(plans, workerPushPlan{jobID: jobID, payload: payload, fp: fp})
		jobIDs[i] = jobID
	}
	return jobType, entry, plans, jobIDs, nil
}

func (c *Client) rollbackWorkerPlans(entry config.HandlerEntry, workerClass string, plans []workerPushPlan, produced int) {
	for i := produced; i < len(plans); i++ {
		p := plans[i]
		c.releaseUniqWorker(entry, workerClass, p.payload, p.jobID, p.fp)
	}
}

// EnqueueMany enqueues many standalone Ruby worker class jobs immediately.
func (c *Client) EnqueueMany(ctx context.Context, workerClass string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	jobType, entry, plans, jobIDs, err := c.planWorkerPushes(ctx, workerClass, payloads, "")
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}
	tid := opts.tenantID("")
	records := make([]kafkaclient.ProduceRecord, 0, len(plans))
	for _, p := range plans {
		msg := c.buildWorkerMessage(entry, jobType, workerClass, p.payload, p.jobID, nil, opts, nil)
		if tid != "" && msg.TenantID == nil {
			msg.TenantID = &tid
		}
		raw, err := json.Marshal(msg)
		if err != nil {
			c.rollbackWorkerPlans(entry, workerClass, plans, 0)
			return nil, err
		}
		route := c.routeFor(entry, p.jobID, tid, nil)
		records = append(records, kafkaclient.ProduceRecord{Topic: route.Topic, Key: route.Key, Payload: raw, Partition: route.Partition})
	}
	produced, err := c.produceInChunks(ctx, records)
	if err != nil {
		c.rollbackWorkerPlans(entry, workerClass, plans, produced)
		return nil, err
	}
	_ = produced
	return jobIDs, nil
}

// EnqueueManyAt schedules many standalone Ruby worker class jobs.
func (c *Client) EnqueueManyAt(ctx context.Context, runAt interface{}, workerClass string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	jobType, entry, plans, jobIDs, err := c.planWorkerPushes(ctx, workerClass, payloads, "")
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}
	tid := opts.tenantID("")
	at := clampRunAt(runAt, c.cfg.MaxScheduleHorizon)
	messages := make([]protocol.JobMessage, 0, len(plans))
	for _, p := range plans {
		msg := c.buildWorkerMessage(entry, jobType, workerClass, p.payload, p.jobID, nil, opts, nil)
		if tid != "" && msg.TenantID == nil {
			msg.TenantID = &tid
		}
		messages = append(messages, msg)
	}
	if err := c.scheduleMessages(ctx, messages, at, ""); err != nil {
		produced := 0
		if pe, ok := err.(*PartialProduceError); ok {
			produced = pe.ProducedCount
		}
		c.rollbackWorkerPlans(entry, workerClass, plans, produced)
		return nil, err
	}
	return jobIDs, nil
}

// EnqueueManyIn schedules many standalone Ruby worker class jobs after a duration.
func (c *Client) EnqueueManyIn(ctx context.Context, d time.Duration, workerClass string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	return c.EnqueueManyAt(ctx, time.Now().Add(d), workerClass, payloads, opts)
}

// PushMany enqueues many Ruby worker class jobs into this batch immediately.
func (b *Batch) PushMany(ctx context.Context, workerClass string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	jobType, entry, plans, jobIDs, err := b.client.planWorkerPushes(ctx, workerClass, payloads, b.id)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}
	if _, err := b.reserve(ctx, int64(len(plans))); err != nil {
		b.client.rollbackWorkerPlans(entry, workerClass, plans, 0)
		return nil, err
	}

	tid := opts.tenantID(b.tenantID)
	records := make([]kafkaclient.ProduceRecord, 0, len(plans))
	for _, p := range plans {
		seq, err := b.nextBatchSeq()
		if err != nil {
			b.rollbackWorkerPlans(ctx, entry, workerClass, plans, 0)
			return nil, err
		}
		msg := b.client.buildWorkerMessage(entry, jobType, workerClass, p.payload, p.jobID, &b.id, opts, &seq)
		if tid != "" && msg.TenantID == nil {
			msg.TenantID = &tid
		}
		raw, err := json.Marshal(msg)
		if err != nil {
			b.rollbackWorkerPlans(ctx, entry, workerClass, plans, 0)
			return nil, err
		}
		route := b.client.routeFor(entry, p.jobID, tid, &b.id)
		records = append(records, kafkaclient.ProduceRecord{Topic: route.Topic, Key: route.Key, Payload: raw, Partition: route.Partition})
	}
	produced, err := b.client.produceInChunks(ctx, records)
	if err != nil {
		b.rollbackWorkerPlans(ctx, entry, workerClass, plans, produced)
		return nil, err
	}
	_ = produced
	return jobIDs, nil
}

// PushManyAt schedules many Ruby worker class jobs into this batch.
func (b *Batch) PushManyAt(ctx context.Context, runAt interface{}, workerClass string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	jobType, entry, plans, jobIDs, err := b.client.planWorkerPushes(ctx, workerClass, payloads, b.id)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}
	if _, err := b.reserve(ctx, int64(len(plans))); err != nil {
		b.client.rollbackWorkerPlans(entry, workerClass, plans, 0)
		return nil, err
	}

	tid := opts.tenantID(b.tenantID)
	at := clampRunAt(runAt, b.client.cfg.MaxScheduleHorizon)
	messages := make([]protocol.JobMessage, 0, len(plans))
	for _, p := range plans {
		seq, err := b.nextBatchSeq()
		if err != nil {
			b.rollbackWorkerPlans(ctx, entry, workerClass, plans, 0)
			return nil, err
		}
		msg := b.client.buildWorkerMessage(entry, jobType, workerClass, p.payload, p.jobID, &b.id, opts, &seq)
		if tid != "" && msg.TenantID == nil {
			msg.TenantID = &tid
		}
		messages = append(messages, msg)
	}
	if err := b.client.scheduleMessages(ctx, messages, at, b.id); err != nil {
		produced := 0
		if pe, ok := err.(*PartialProduceError); ok {
			produced = pe.ProducedCount
		}
		b.rollbackWorkerPlans(ctx, entry, workerClass, plans, produced)
		return nil, err
	}
	return jobIDs, nil
}

// PushManyIn schedules many Ruby worker class jobs into this batch after a duration.
func (b *Batch) PushManyIn(ctx context.Context, d time.Duration, workerClass string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	return b.PushManyAt(ctx, time.Now().Add(d), workerClass, payloads, opts)
}

func (b *Batch) rollbackWorkerPlans(ctx context.Context, entry config.HandlerEntry, workerClass string, plans []workerPushPlan, produced int) {
	b.client.rollbackWorkerPlans(entry, workerClass, plans, produced)
	unproduced := int64(len(plans) - produced)
	if unproduced > 0 {
		_, _ = b.client.store.AddJobs(ctx, b.id, -unproduced)
	}
}
