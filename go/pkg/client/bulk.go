package client

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/schedule"
	"github.com/y-shashank/kafka-batch/go/pkg/uniq"
)

type pushPlan struct {
	jobID   string
	payload map[string]interface{}
	fp      string
}

func (c *Client) chunkSize() int {
	if c.cfg.ProduceChunkSize < 1 {
		return 500
	}
	return c.cfg.ProduceChunkSize
}

func (c *Client) produceInChunks(ctx context.Context, records []kafkaclient.ProduceRecord) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	size := c.chunkSize()
	producedTotal := 0
	for i := 0; i < len(records); i += size {
		end := i + size
		if end > len(records) {
			end = len(records)
		}
		chunk := records[i:end]
		dels, err := c.prod.ProduceManySync(ctx, chunk)
		if err != nil {
			producedTotal += len(dels)
			return producedTotal, &PartialProduceError{
				Message:       err.Error(),
				ProducedCount: producedTotal,
			}
		}
		producedTotal += len(chunk)
		_ = dels
	}
	return producedTotal, nil
}

func (c *Client) scheduleMessages(ctx context.Context, messages []protocol.JobMessage, runAt time.Time, batchID string) error {
	if len(messages) == 0 {
		return nil
	}
	topic := c.cfg.resolveTopic(c.cfg.ScheduledTopic)
	size := c.chunkSize()
	producedTotal := 0

	for i := 0; i < len(messages); i += size {
		end := i + size
		if end > len(messages) {
			end = len(messages)
		}
		chunk := messages[i:end]
		records := make([]kafkaclient.ProduceRecord, len(chunk))
		for j, msg := range chunk {
			raw, err := json.Marshal(msg)
			if err != nil {
				return err
			}
			records[j] = kafkaclient.ProduceRecord{Topic: topic, Key: msg.JobID, Payload: raw}
		}

		dels, err := c.prod.ProduceManySync(ctx, records)
		if err != nil {
			delivered := len(dels)
			if delivered > 0 {
				entries := scheduleEntriesFrom(chunk[:delivered], dels, runAt, batchID)
				if werr := c.writeScheduleIndex(ctx, entries, batchID, chunk[0].JobID, delivered); werr != nil {
					return werr
				}
				producedTotal += delivered
			}
			return &PartialProduceError{
				Message:       err.Error(),
				ProducedCount: producedTotal,
			}
		}

		entries := scheduleEntriesFrom(chunk, dels, runAt, batchID)
		if err := c.writeScheduleIndex(ctx, entries, batchID, chunk[0].JobID, len(entries)); err != nil {
			return err
		}
		producedTotal += len(chunk)
	}

	workerClass := messages[0].WorkerClass
	if producedTotal == 1 {
		instrument.ScheduledEnqueued(messages[0].JobID, batchID, workerClass, runAt)
	} else {
		instrument.ScheduledEnqueuedBulk(producedTotal, batchID, workerClass, runAt)
	}
	return nil
}

func scheduleEntriesFrom(msgs []protocol.JobMessage, dels []kafkaclient.Delivery, runAt time.Time, batchID string) []schedule.ScheduleEntry {
	out := make([]schedule.ScheduleEntry, len(msgs))
	for i, msg := range msgs {
		out[i] = schedule.ScheduleEntry{
			JobID: msg.JobID, RunAt: runAt, BatchID: batchID,
			Partition: dels[i].Partition, Offset: dels[i].Offset,
		}
	}
	return out
}

func (b *Batch) planPushes(ctx context.Context, jobType string, payloads []map[string]interface{}) (config.HandlerEntry, []pushPlan, []string, error) {
	entry, err := b.client.lookupHandler(jobType)
	if err != nil {
		return entry, nil, nil, err
	}
	jobIDs := make([]string, len(payloads))
	plans := make([]pushPlan, 0, len(payloads))
	for i, payload := range payloads {
		if payload == nil {
			payload = map[string]interface{}{}
		}
		jobID := uuid.NewString()
		skipped, err := b.client.claimUniq(ctx, entry, jobType, payload, jobID, b.id)
		if err != nil {
			return entry, nil, nil, err
		}
		if skipped {
			jobIDs[i] = ""
			continue
		}
		fp := ""
		if entry.Uniq && b.client.cfg.UniqEnabled {
			fp = uniq.DigestHex(workerClassName(entry, jobType), payload)
		}
		plans = append(plans, pushPlan{jobID: jobID, payload: payload, fp: fp})
		jobIDs[i] = jobID
	}
	return entry, plans, jobIDs, nil
}

func (b *Batch) nextBatchSeq() (int64, error) {
	if b.seqCursor == 0 || b.seqEnd == 0 {
		return 0, BatchClosedError{BatchID: b.id, Reason: "no reserved batch_seq slots"}
	}
	if b.seqCursor > b.seqEnd {
		return 0, BatchClosedError{BatchID: b.id, Reason: "reserved too few batch_seq slots"}
	}
	seq := b.seqCursor
	b.seqCursor++
	return seq, nil
}

func (b *Batch) rollbackPlans(ctx context.Context, entry config.HandlerEntry, jobType string, plans []pushPlan, produced int) {
	for i := produced; i < len(plans); i++ {
		p := plans[i]
		b.client.releaseUniq(entry, jobType, p.payload, p.jobID, p.fp)
	}
	unproduced := int64(len(plans) - produced)
	if unproduced > 0 {
		_, _ = b.client.store.AddJobs(ctx, b.id, -unproduced)
	}
}

// PushManyJobs enqueues many manifest jobs into this batch (Ruby push_many).
// Returns job IDs in payload order; empty string marks a uniq-skipped slot.
func (b *Batch) PushManyJobs(ctx context.Context, jobType string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	entry, plans, jobIDs, err := b.planPushes(ctx, jobType, payloads)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}

	if _, err := b.reserve(ctx, int64(len(plans))); err != nil {
		for _, p := range plans {
			b.client.releaseUniq(entry, jobType, p.payload, p.jobID, p.fp)
		}
		return nil, err
	}

	tid := opts.tenantID(b.tenantID)
	records := make([]kafkaclient.ProduceRecord, 0, len(plans))
	for _, p := range plans {
		seq, err := b.nextBatchSeq()
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, 0)
			return nil, err
		}
		msg, err := b.client.buildMessage(entry, jobType, p.payload, p.jobID, &b.id, opts, &seq)
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, 0)
			return nil, err
		}
		if tid != "" && msg.TenantID == nil {
			msg.TenantID = &tid
		}
		raw, err := json.Marshal(msg)
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, 0)
			return nil, err
		}
		route := b.client.routeFor(entry, p.jobID, tid, &b.id)
		rec := kafkaclient.ProduceRecord{Topic: route.Topic, Key: route.Key, Payload: raw, Partition: route.Partition}
		records = append(records, rec)
	}

	produced, err := b.client.produceInChunks(ctx, records)
	if err != nil {
		b.rollbackPlans(ctx, entry, jobType, plans, produced)
		return nil, err
	}
	_ = produced
	return jobIDs, nil
}

// PushManyJobsAt schedules many manifest jobs into this batch (Ruby push_many_at).
func (b *Batch) PushManyJobsAt(ctx context.Context, runAt interface{}, jobType string, payloads []map[string]interface{}, opts PushOptions) ([]string, error) {
	if len(payloads) == 0 {
		return nil, nil
	}
	entry, plans, jobIDs, err := b.planPushes(ctx, jobType, payloads)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return jobIDs, nil
	}

	if _, err := b.reserve(ctx, int64(len(plans))); err != nil {
		for _, p := range plans {
			b.client.releaseUniq(entry, jobType, p.payload, p.jobID, p.fp)
		}
		return nil, err
	}

	tid := opts.tenantID(b.tenantID)
	at := clampRunAt(runAt, b.client.cfg.MaxScheduleHorizon)
	messages := make([]protocol.JobMessage, 0, len(plans))
	for _, p := range plans {
		seq, err := b.nextBatchSeq()
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, 0)
			return nil, err
		}
		msg, err := b.client.buildMessage(entry, jobType, p.payload, p.jobID, &b.id, opts, &seq)
		if err != nil {
			b.rollbackPlans(ctx, entry, jobType, plans, 0)
			return nil, err
		}
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
		b.rollbackPlans(ctx, entry, jobType, plans, produced)
		return nil, err
	}
	return jobIDs, nil
}

// PushJobIn schedules one job after a duration (Ruby push_in).
func (b *Batch) PushJobIn(ctx context.Context, d time.Duration, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	return b.PushJobAt(ctx, time.Now().Add(d), jobType, payload, opts)
}

// EnqueueJobIn enqueues a standalone job after a duration.
func (c *Client) EnqueueJobIn(ctx context.Context, d time.Duration, jobType string, payload map[string]interface{}, opts PushOptions) (string, error) {
	return c.EnqueueJobAt(ctx, time.Now().Add(d), jobType, payload, opts)
}
