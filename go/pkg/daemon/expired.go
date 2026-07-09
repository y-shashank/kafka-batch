package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch/go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
)

type expiredPublisher struct {
	cfg   config.Daemon
	prod  *kafkaclient.Client
	store *store.RedisStore
	now   func() time.Time
}

func newExpiredPublisher(cfg config.Daemon, prod *kafkaclient.Client, st *store.RedisStore) expiredPublisher {
	return expiredPublisher{cfg: cfg, prod: prod, store: st, now: time.Now}
}

func (p expiredPublisher) publish(ctx context.Context, raw []byte, src protocol.SourceCoords) error {
	drop := jobexpiry.BuildDrop(raw, src, p.now())
	var job protocol.JobMessage
	_ = json.Unmarshal(raw, &job)

	if drop.Event != nil && p.store != nil {
		result, err := p.store.RecordCompletionsBatch(ctx, []store.CompletionEvent{{
			BatchID: drop.Event.BatchID, JobID: drop.Event.JobID,
			Status: drop.Event.Status, BatchSeq: drop.Event.BatchSeq,
		}})
		if err != nil {
			return err
		}
		for _, fin := range result.Finished {
			if fin.Batch == nil {
				continue
			}
			instrument.BatchCompleted(
				fin.Batch.ID, fin.Outcome,
				fin.Batch.TotalJobs, fin.Batch.CompletedCount, fin.Batch.FailedCount,
			)
			cb := protocol.CallbackMessage{
				BatchID: fin.Batch.ID, Outcome: fin.Outcome,
				TotalJobs: fin.Batch.TotalJobs, CompletedCount: fin.Batch.CompletedCount,
				FailedCount: fin.Batch.FailedCount, OnSuccess: fin.Batch.OnSuccess,
				OnComplete: fin.Batch.OnComplete, FinishedAt: fin.Batch.FinishedAt,
			}
			body, _ := json.Marshal(cb)
			if err := p.prod.Produce(ctx, p.cfg.CallbacksTopic, cb.BatchID, body); err != nil {
				return err
			}
		}
		ev, _ := json.Marshal(drop.Event)
		key := fmt.Sprintf("%s/%d", drop.Event.SrcTopic, drop.Event.SrcPartition)
		if err := p.prod.Produce(ctx, p.cfg.EventsTopic, key, ev); err != nil {
			return err
		}
	}
	if drop.Failure != nil && p.store != nil {
		_ = p.store.RecordFailure(ctx, store.FailureEntry{
			BatchID: drop.Failure.BatchID, JobID: drop.Failure.JobID,
			WorkerClass: drop.Failure.WorkerClass, ErrorClass: drop.Failure.ErrorClass,
			ErrorMessage: drop.Failure.ErrorMessage, Status: drop.Failure.Status,
			Attempt: drop.Failure.Attempt,
		})
	}
	if drop.DLTPayload != nil {
		if err := p.prod.Produce(ctx, p.cfg.DeadLetterTopic, drop.DLTKey, drop.DLTPayload); err != nil {
			return err
		}
		jid, bid, dt := dltMeta(drop.DLTPayload)
		instrument.DLTPublished(jid, bid, dt, src.Topic)
	}
	if job.JobID != "" {
		instrument.JobExpired(job.JobID, derefStr(job.BatchID), job.WorkerClass, job.ValidTill)
	}
	if p.store != nil && job.JobID != "" {
		_ = p.store.ReleaseUniqLock(ctx, job.UniqFP, job.JobID)
	}
	return nil
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
