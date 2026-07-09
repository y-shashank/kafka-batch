package client

import (
	"context"
	"encoding/json"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/schedule"
)

func (c *Client) produceJob(ctx context.Context, route Route, msg protocol.JobMessage) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = c.prod.ProduceSync(ctx, route.Topic, route.Key, raw, route.Partition)
	return err
}

func (c *Client) scheduleMessage(ctx context.Context, msg protocol.JobMessage, runAt time.Time, batchID string) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	topic := c.cfg.resolveTopic(c.cfg.ScheduledTopic)
	del, err := c.prod.ProduceSync(ctx, topic, msg.JobID, raw, nil)
	if err != nil {
		return err
	}
	entry := schedule.ScheduleEntry{
		JobID: msg.JobID, RunAt: runAt,
		Partition: del.Partition, Offset: del.Offset,
	}
	if err := c.writeScheduleIndex(ctx, []schedule.ScheduleEntry{entry}, batchID, msg.JobID, 1); err != nil {
		return err
	}
	instrument.ScheduledEnqueued(msg.JobID, batchID, msg.WorkerClass, runAt)
	return nil
}

func (c *Client) writeScheduleIndex(ctx context.Context, entries []schedule.ScheduleEntry, batchID, jobID string, count int) error {
	retries := c.cfg.ScheduleIndexWriteRetries
	if retries < 1 {
		retries = 3
	}
	backoff := c.cfg.ScheduleIndexWriteBackoff
	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		var err error
		if len(entries) == 1 {
			e := entries[0]
			err = c.sched.Schedule(ctx, e.JobID, e.RunAt, e.Partition, e.Offset)
		} else {
			err = c.sched.ScheduleMany(ctx, entries)
		}
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < retries && backoff > 0 {
			time.Sleep(time.Duration(attempt) * backoff)
		}
	}
	instrument.ScheduledIndexFailed(count, batchID, jobID, retries, lastErr)
	return PartialProduceError{
		Message:       "schedule index write failed: " + lastErr.Error(),
		ProducedCount: 0,
	}
}

// ProduceRecord is a test/export alias.
type ProduceRecord = kafkaclient.ProduceRecord
