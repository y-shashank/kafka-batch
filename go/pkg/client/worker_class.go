package client

import (
	"context"
	"strings"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/uniq"
)

type workerBinding struct {
	jobType string
	entry   config.HandlerEntry
}

func (c *Client) buildWorkerIndex() {
	c.workerByClass = map[string]workerBinding{}
	for jt, h := range c.manifest.Handlers {
		if h.WorkerClass == "" {
			continue
		}
		c.workerByClass[h.WorkerClass] = workerBinding{jobType: jt, entry: h}
	}
	for name, wc := range c.cfg.Workers {
		if _, ok := c.workerByClass[name]; ok {
			continue
		}
		c.workerByClass[name] = workerBinding{jobType: wc.jobTypeFor(name), entry: wc.toEntry(name, c.cfg)}
	}
}

func (wc WorkerClassConfig) jobTypeFor(className string) string {
	if wc.JobType != "" {
		return wc.JobType
	}
	return className
}

func (wc WorkerClassConfig) toEntry(className string, cfg Config) config.HandlerEntry {
	topic := wc.Topic
	applyPrefix := wc.ApplyTopicPrefix
	if topic != "" && applyPrefix {
		topic = cfg.resolveTopic(topic)
		applyPrefix = false
	}
	return config.HandlerEntry{
		Runtime:              config.RuntimeRuby,
		WorkerClass:          className,
		Topic:                topic,
		ApplyTopicPrefix:     applyPrefix,
		MaxRetries:           wc.MaxRetries,
		CompleteAfterRetries: wc.CompleteAfterRetries,
		RetryTier:            wc.RetryTier,
		FairnessType:         wc.FairnessType,
		Uniq:                 wc.Uniq,
	}
}

func (c *Client) lookupWorkerClass(workerClass string) (string, config.HandlerEntry, error) {
	b, ok := c.workerByClass[workerClass]
	if !ok {
		return "", config.HandlerEntry{}, UnknownWorkerClassError{WorkerClass: workerClass}
	}
	entry := b.entry
	entry.WorkerClass = workerClass
	if entry.Runtime == "" {
		entry.Runtime = config.RuntimeRuby
	}
	entry = c.resolveWorkerEntry(entry)
	return b.jobType, entry, nil
}

func (c *Client) buildWorkerMessage(entry config.HandlerEntry, jobType, workerClass string, payload map[string]interface{}, jobID string, batchID *string, opts PushOptions, batchSeq *int64) protocol.JobMessage {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	msg := protocol.JobMessage{
		JobID:                jobID,
		BatchID:              batchID,
		JobType:              jobType,
		WorkerClass:          workerClass,
		Payload:              payload,
		Attempt:              0,
		MaxRetries:           c.maxRetries(entry),
		CompleteAfterRetries: c.completeAfter(entry),
		EnqueuedAt:           protocol.NowISO(),
	}
	if tid := opts.tenantID(""); tid != "" {
		msg.TenantID = &tid
	}
	if batchSeq != nil && batchID != nil {
		msg.BatchSeq = batchSeq
	}
	if tier := entry.RetryTier; tier != "" {
		msg.RetryTier = tier
	}
	if opts.ValidTill != "" {
		if norm := jobexpiry.NormalizeValidTill(opts.ValidTill); norm != "" {
			msg.ValidTill = norm
		}
	}
	if entry.Uniq && c.cfg.UniqEnabled {
		msg.UniqFP = uniq.DigestHex(workerClass, payload)
	}
	return msg
}

func (c *Client) claimUniqWorker(ctx context.Context, entry config.HandlerEntry, workerClass string, payload map[string]interface{}, jobID, batchID string) (skipped bool, err error) {
	if !entry.Uniq || !c.cfg.UniqEnabled {
		return false, nil
	}
	ok, err := c.uniq.Claim(ctx, workerClass, payload, jobID)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}
	instrument.JobUniqSkipped(workerClass, payload, jobID, batchID)
	if c.cfg.UniqOnDuplicate == "raise" {
		return false, DuplicateJobError{WorkerClass: workerClass}
	}
	return true, nil
}

func (c *Client) releaseUniqWorker(entry config.HandlerEntry, workerClass string, payload map[string]interface{}, jobID, fp string) {
	if !entry.Uniq || !c.cfg.UniqEnabled {
		return
	}
	if fp != "" {
		_ = c.uniq.Release(context.Background(), fp, jobID)
		return
	}
	fp = uniq.DigestHex(workerClass, payload)
	_ = c.uniq.Release(context.Background(), fp, jobID)
}

// Enqueue enqueues a standalone Ruby worker class job immediately.
func (c *Client) Enqueue(ctx context.Context, workerClass string, payload map[string]interface{}, opts PushOptions) (string, error) {
	jobType, entry, err := c.lookupWorkerClass(workerClass)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := c.claimUniqWorker(ctx, entry, workerClass, payload, jobID, ""); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	msg := c.buildWorkerMessage(entry, jobType, workerClass, payload, jobID, nil, opts, nil)
	route := c.routeFor(entry, jobID, opts.tenantID(""), nil)
	if err := c.produceJob(ctx, route, msg); err != nil {
		c.releaseUniqWorker(entry, workerClass, payload, jobID, msg.UniqFP)
		return "", err
	}
	return jobID, nil
}

// EnqueueAt schedules a standalone Ruby worker class job.
func (c *Client) EnqueueAt(ctx context.Context, runAt interface{}, workerClass string, payload map[string]interface{}, opts PushOptions) (string, error) {
	jobType, entry, err := c.lookupWorkerClass(workerClass)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := c.claimUniqWorker(ctx, entry, workerClass, payload, jobID, ""); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	msg := c.buildWorkerMessage(entry, jobType, workerClass, payload, jobID, nil, opts, nil)
	if err := c.scheduleMessage(ctx, msg, clampRunAt(runAt, c.cfg.MaxScheduleHorizon), ""); err != nil {
		c.releaseUniqWorker(entry, workerClass, payload, jobID, msg.UniqFP)
		return "", err
	}
	return jobID, nil
}

// EnqueueIn schedules a standalone Ruby worker class job after a duration.
func (c *Client) EnqueueIn(ctx context.Context, d time.Duration, workerClass string, payload map[string]interface{}, opts PushOptions) (string, error) {
	return c.EnqueueAt(ctx, time.Now().Add(d), workerClass, payload, opts)
}

// Push enqueues one Ruby worker class job into this batch.
func (b *Batch) Push(ctx context.Context, workerClass string, payload map[string]interface{}, opts PushOptions) (string, error) {
	jobType, entry, err := b.client.lookupWorkerClass(workerClass)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := b.client.claimUniqWorker(ctx, entry, workerClass, payload, jobID, b.id); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	seq, err := b.reserve(ctx, 1)
	if err != nil {
		b.client.releaseUniqWorker(entry, workerClass, payload, jobID, "")
		return "", err
	}
	msg := b.client.buildWorkerMessage(entry, jobType, workerClass, payload, jobID, &b.id, opts, &seq)
	route := b.client.routeFor(entry, jobID, opts.tenantID(b.tenantID), &b.id)
	if err := b.client.produceJob(ctx, route, msg); err != nil {
		b.client.releaseUniqWorker(entry, workerClass, payload, jobID, msg.UniqFP)
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	return jobID, nil
}

// PushAt schedules one Ruby worker class job into this batch.
func (b *Batch) PushAt(ctx context.Context, runAt interface{}, workerClass string, payload map[string]interface{}, opts PushOptions) (string, error) {
	jobType, entry, err := b.client.lookupWorkerClass(workerClass)
	if err != nil {
		return "", err
	}
	jobID := opts.jobID()
	if skipped, err := b.client.claimUniqWorker(ctx, entry, workerClass, payload, jobID, b.id); skipped || err != nil {
		if skipped {
			return "", ErrJobSkipped
		}
		return "", err
	}
	seq, err := b.reserve(ctx, 1)
	if err != nil {
		b.client.releaseUniqWorker(entry, workerClass, payload, jobID, "")
		return "", err
	}
	msg := b.client.buildWorkerMessage(entry, jobType, workerClass, payload, jobID, &b.id, opts, &seq)
	if err := b.client.scheduleMessage(ctx, msg, clampRunAt(runAt, b.client.cfg.MaxScheduleHorizon), b.id); err != nil {
		b.client.releaseUniqWorker(entry, workerClass, payload, jobID, msg.UniqFP)
		_, _ = b.client.store.AddJobs(ctx, b.id, -1)
		return "", err
	}
	return jobID, nil
}

// PushIn schedules one Ruby worker class job into this batch after a duration.
func (b *Batch) PushIn(ctx context.Context, d time.Duration, workerClass string, payload map[string]interface{}, opts PushOptions) (string, error) {
	return b.PushAt(ctx, time.Now().Add(d), workerClass, payload, opts)
}

// resolveWorkerEntry ensures routeFor uses a resolved topic for worker-class entries.
func (c *Client) resolveWorkerEntry(entry config.HandlerEntry) config.HandlerEntry {
	if entry.Topic != "" && entry.ApplyTopicPrefix && c.cfg.TopicPrefix != "" {
		if !strings.HasPrefix(entry.Topic, c.cfg.TopicPrefix+".") {
			entry.Topic = c.cfg.resolveTopic(entry.Topic)
			entry.ApplyTopicPrefix = false
		}
	}
	return entry
}
