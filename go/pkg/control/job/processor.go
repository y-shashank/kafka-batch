package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/fairness"
	"github.com/y-shashank/kafka-batch/go/pkg/jobexpiry"
	"github.com/y-shashank/kafka-batch/go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
)

// Producer publishes Kafka messages.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Processor runs plain-topic job messages (no fairness).
type Processor struct {
	Cfg            config.Daemon
	Manifest       config.Manifest
	Store          *store.RedisStore
	Producer       Producer
	FairTime       *fairness.Scheduler
	FairThroughput *fairness.Scheduler
	RubyExec       RubyExecutor
	Now            func() time.Time
}

// Outcome describes what happened to one job message.
type Outcome struct {
	CommitOffset bool
	Event        *protocol.EventMessage
	RetryTopic   string
	RetryKey     string
	RetryPayload []byte
	DLTPayload   []byte
	DLTKey       string
}

func (p *Processor) Process(ctx context.Context, raw []byte, src protocol.SourceCoords) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	var job protocol.JobMessage
	if err := json.Unmarshal(raw, &job); err != nil {
		dlt, key := p.dltPayload(map[string]interface{}{"raw_payload": string(raw)}, src.Topic, "json.ParseError", err.Error())
		out.DLTPayload = dlt
		out.DLTKey = key
		return out, nil
	}

	if p.Cfg.SkipCancelledJobs && job.BatchID != nil {
		cancelled, err := p.Store.BatchCancelled(ctx, *job.BatchID)
		if err != nil {
			return out, err
		}
		if cancelled {
			p.releaseUniq(ctx, job)
			return out, nil
		}
	}

	if jobexpiry.Expired(job.ValidTill, p.now()) {
		if job.BatchID != nil && job.BatchSeq != nil {
			ev := p.buildEvent(job, "failed", src)
			out.Event = &ev
		}
		dlt, key := p.dltPayload(jobMap(raw), src.Topic, "ExpiredError", "job expired")
		out.DLTPayload = dlt
		out.DLTKey = key
		p.releaseUniq(ctx, job)
		return out, nil
	}

	entry, fromManifest := p.Manifest.Handlers[job.JobType]
	_, goHandler := kbatch.Lookup(job.JobType)
	rubyHandler := fromManifest && entry.Runtime == "ruby"
	if !goHandler && !rubyHandler {
		err := fmt.Errorf("unknown job_type %q", job.JobType)
		if job.BatchID != nil && job.BatchSeq != nil {
			ev := p.buildEvent(job, "failed", src)
			out.Event = &ev
		}
		dlt, key := p.dltPayload(jobMap(raw), src.Topic, "UnknownHandler", err.Error())
		out.DLTPayload = dlt
		out.DLTKey = key
		p.releaseUniq(ctx, job)
		return out, nil
	}
	if rubyHandler && p.RubyExec == nil {
		err := &RubyExecutionError{
			Class:   "RubyWorkerUnavailable",
			Message: "ruby_worker_socket is not configured",
		}
		if job.BatchID != nil && job.BatchSeq != nil {
			ev := p.buildEvent(job, "failed", src)
			out.Event = &ev
		}
		dlt, key := p.dltPayload(jobMap(raw), src.Topic, rubyClassName(err), err.Error())
		out.DLTPayload = dlt
		out.DLTKey = key
		p.releaseUniq(ctx, job)
		return out, nil
	}

	run := func() error {
		if rubyHandler {
			return p.RubyExec.Execute(ctx, job)
		}
		handler, _ := kbatch.Lookup(job.JobType)
		hctx := &kbatch.Context{
			JobType: job.JobType,
			JobID:   job.JobID,
			Attempt: job.Attempt,
			Payload: job.Payload,
		}
		if job.BatchID != nil {
			hctx.BatchID = *job.BatchID
		}
		return handler(hctx)
	}

	if err := p.withFairSlot(ctx, raw, run); err != nil {
		if errors.Is(err, errFairSkipped) {
			return out, nil
		}
		return p.handleFailure(ctx, job, raw, src, err)
	}

	if job.BatchID != nil && job.BatchSeq != nil && !job.BatchCounted {
		ev := p.buildEvent(job, "success", src)
		out.Event = &ev
	}
	p.releaseUniq(ctx, job)
	return out, nil
}

func (p *Processor) handleFailure(ctx context.Context, job protocol.JobMessage, raw []byte, src protocol.SourceCoords, execErr error) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	maxRetries := job.MaxRetries
	if maxRetries == 0 {
		maxRetries = p.Cfg.MaxRetries
	}
	completeAfter := job.CompleteAfterRetries
	if completeAfter == 0 {
		completeAfter = p.Cfg.CompleteAfter
	}

	if nonRetryable(execErr) {
		if job.BatchID != nil && !job.BatchCounted && job.BatchSeq != nil {
			ev := p.buildEvent(job, "failed", src)
			out.Event = &ev
		}
		dlt, key := p.dltPayload(jobMap(raw), src.Topic, className(execErr), execErr.Error())
		out.DLTPayload = dlt
		out.DLTKey = key
		p.releaseUniq(ctx, job)
		return out, nil
	}

	if job.Attempt < maxRetries {
		next := job.Attempt + 1
		tier := p.Cfg.RetryTierFor(next, job.RetryTier)
		delay := p.retryDelay(tier)
		retryAt := p.now().Add(delay)

		if job.BatchID != nil && !job.BatchCounted && job.Attempt >= completeAfter && job.BatchSeq != nil {
			ev := p.buildEvent(job, "failed", src)
			out.Event = &ev
			job.BatchCounted = true
		}

		retryPayload, err := p.buildRetryPayload(raw, job, retryAt, src.Topic)
		if err != nil {
			return out, err
		}
		out.RetryTopic = p.Cfg.RetryTopic(tier)
		out.RetryKey = job.JobID
		out.RetryPayload = retryPayload
		return out, nil
	}

	if job.BatchID != nil && !job.BatchCounted && job.BatchSeq != nil {
		ev := p.buildEvent(job, "failed", src)
		out.Event = &ev
	}
	dlt, key := p.dltPayload(jobMap(raw), src.Topic, className(execErr), execErr.Error())
	out.DLTPayload = dlt
	out.DLTKey = key
	p.releaseUniq(ctx, job)
	return out, nil
}

func (p *Processor) releaseUniq(ctx context.Context, job protocol.JobMessage) {
	if p.Store == nil {
		return
	}
	_ = p.Store.ReleaseUniqLock(ctx, job.UniqFP, job.JobID)
}

func (p *Processor) buildEvent(job protocol.JobMessage, status string, src protocol.SourceCoords) protocol.EventMessage {
	ev := protocol.EventMessage{
		BatchID:      deref(job.BatchID),
		JobID:        job.JobID,
		Status:       status,
		WorkerClass:  job.WorkerClass,
		OccurredAt:   protocol.NowISO(),
		SrcTopic:     src.Topic,
		SrcPartition: src.Partition,
		SrcOffset:    src.Offset,
	}
	if job.BatchSeq != nil {
		ev.BatchSeq = *job.BatchSeq
	}
	return ev
}

func (p *Processor) buildRetryPayload(raw []byte, job protocol.JobMessage, retryAt time.Time, retryTo string) ([]byte, error) {
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	m["attempt"] = job.Attempt + 1
	m["retry_after"] = retryAt.UTC().Format(time.RFC3339)
	m["retry_to"] = retryTo
	if job.BatchCounted {
		m["batch_counted"] = true
	}
	delete(m, "_fair_slot")
	delete(m, "_fair_slot_id")
	delete(m, "_fair_type")
	return json.Marshal(m)
}

func (p *Processor) dltPayload(base map[string]interface{}, topic, errClass, errMsg string) ([]byte, string) {
	base["dlt_type"] = "job"
	base["dlt_source_topic"] = topic
	base["dlt_error_class"] = errClass
	base["dlt_error_message"] = errMsg
	base["dlt_at"] = protocol.NowISO()
	raw, _ := json.Marshal(base)
	key, _ := base["job_id"].(string)
	if key == "" {
		key = "dlt"
	}
	return raw, key
}

func (p *Processor) retryDelay(tier string) time.Duration {
	sec, ok := p.Cfg.RetryTiers[tier]
	if !ok || sec < 0 {
		sec = 30
	}
	return time.Duration(sec) * time.Second
}

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func jobMap(raw []byte) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	if m == nil {
		m = map[string]interface{}{}
	}
	return m
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func className(err error) string {
	if re, ok := err.(*RubyExecutionError); ok && re.Class != "" {
		return re.Class
	}
	if he, ok := err.(*kbatch.HandlerError); ok && he.Class != "" {
		return he.Class
	}
	return "GoExecutionError"
}

func nonRetryable(err error) bool {
	switch className(err) {
	case "UnknownHandler", "NotRubyHandler":
		return true
	default:
		return false
	}
}
