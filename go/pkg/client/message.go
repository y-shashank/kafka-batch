package client

import (
	"context"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/uniq"
)

func (c *Client) buildMessage(entry config.HandlerEntry, jobType string, payload map[string]interface{}, jobID string, batchID *string, opts PushOptions, batchSeq *int64) (protocol.JobMessage, error) {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	workerName := workerClassName(entry, jobType)
	msg := protocol.JobMessage{
		JobID:                jobID,
		BatchID:              batchID,
		JobType:              jobType,
		WorkerClass:          workerName,
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
		fp := uniq.DigestHex(workerName, payload)
		msg.UniqFP = fp
	}
	return msg, nil
}

func workerClassName(entry config.HandlerEntry, jobType string) string {
	if entry.WorkerClass != "" {
		return entry.WorkerClass
	}
	return "go:" + jobType
}

func (c *Client) maxRetries(entry config.HandlerEntry) int {
	if entry.MaxRetries > 0 {
		return entry.MaxRetries
	}
	if c.cfg.MaxRetries > 0 {
		return c.cfg.MaxRetries
	}
	return 25
}

func (c *Client) completeAfter(entry config.HandlerEntry) int {
	if entry.CompleteAfterRetries > 0 {
		return entry.CompleteAfterRetries
	}
	if c.cfg.CompleteAfterRetries > 0 {
		return c.cfg.CompleteAfterRetries
	}
	return 3
}

func (c *Client) claimUniq(ctx context.Context, entry config.HandlerEntry, jobType string, payload map[string]interface{}, jobID, batchID string) (skipped bool, err error) {
	if !entry.Uniq || !c.cfg.UniqEnabled {
		return false, nil
	}
	name := workerClassName(entry, jobType)
	ok, err := c.uniq.Claim(ctx, name, payload, jobID)
	if err != nil {
		return false, err
	}
	if ok {
		return false, nil
	}
	instrument.JobUniqSkipped(name, payload, jobID, batchID)
	if c.cfg.UniqOnDuplicate == "raise" {
		return false, DuplicateJobError{WorkerClass: name}
	}
	return true, nil
}

func (c *Client) releaseUniq(entry config.HandlerEntry, jobType string, payload map[string]interface{}, jobID, fp string) {
	if !entry.Uniq || !c.cfg.UniqEnabled {
		return
	}
	if fp != "" {
		_ = c.uniq.Release(context.Background(), fp, jobID)
		return
	}
	name := workerClassName(entry, jobType)
	fp = uniq.DigestHex(name, payload)
	_ = c.uniq.Release(context.Background(), fp, jobID)
}
