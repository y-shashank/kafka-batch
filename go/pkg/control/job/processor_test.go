package job

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch/go/pkg/config"
	"github.com/y-shashank/kafka-batch/go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
	"github.com/y-shashank/kafka-batch/go/pkg/store"
)

type memProducer struct {
	msgs []struct {
		topic string
		key   string
		body  []byte
	}
}

func (m *memProducer) Produce(_ context.Context, topic, key string, payload []byte) error {
	m.msgs = append(m.msgs, struct {
		topic string
		key   string
		body  []byte
	}{topic, key, payload})
	return nil
}

func TestProcessSuccessEmitsEvent(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.echo", func(ctx *kbatch.Context) error { return nil })

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	batchID := "b1"
	seq := int64(1)
	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", BatchID: &batchID, JobType: "test.echo", WorkerClass: "go:test.echo",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3, CompleteAfterRetries: 3,
		BatchSeq: &seq,
	})

	p := &Processor{Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{}}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if out.Event == nil || out.Event.Status != "success" {
		t.Fatalf("event %+v", out.Event)
	}
	if out.Event.BatchSeq != 1 {
		t.Fatalf("batch_seq %d", out.Event.BatchSeq)
	}
}

func TestProcessHandlerErrorSchedulesRetry(t *testing.T) {
	kbatch.Reset()
	kbatch.Register("test.fail", func(ctx *kbatch.Context) error {
		return &kbatch.HandlerError{Class: "Boom", Message: "boom"}
	})

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", JobType: "test.fail", WorkerClass: "go:test.fail",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3,
	})

	cfg := config.DefaultDaemon()
	p := &Processor{Cfg: cfg, Store: st, Producer: &memProducer{}, Now: func() time.Time { return time.Unix(0, 0) }}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 2})
	if err != nil {
		t.Fatal(err)
	}
	if out.RetryTopic != cfg.RetryTopic("short") {
		t.Fatalf("retry topic %q", out.RetryTopic)
	}
	if out.RetryPayload == nil {
		t.Fatal("expected retry payload")
	}
}

func TestProcessExpiredJobDLT(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", JobType: "any", ValidTill: "2000-01-01T00:00:00Z",
		Payload: map[string]interface{}{}, Attempt: 0,
	})
	p := &Processor{Cfg: config.DefaultDaemon(), Store: st, Producer: &memProducer{},
		Now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if out.DLTPayload == nil {
		t.Fatal("expected DLT")
	}
}

func TestProcessRubyUnknownHandlerDLTWithoutRetry(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	st := store.NewRedisStore(rdb, time.Hour)

	raw, _ := json.Marshal(protocol.JobMessage{
		JobID: "j1", JobType: "ruby.missing", WorkerClass: "Missing",
		Payload: map[string]interface{}{}, Attempt: 0, MaxRetries: 3,
	})
	manifest := config.Manifest{Handlers: map[string]config.HandlerEntry{
		"ruby.missing": {Runtime: "ruby", Topic: "jobs"},
	}}
	p := &Processor{
		Cfg: config.DefaultDaemon(), Manifest: manifest, Store: st, Producer: &memProducer{},
		RubyExec: rubyStub{err: &RubyExecutionError{Class: "UnknownHandler", Message: "missing"}},
	}
	out, err := p.Process(context.Background(), raw, protocol.SourceCoords{Topic: "jobs", Partition: 0, Offset: 4})
	if err != nil {
		t.Fatal(err)
	}
	if out.RetryPayload != nil {
		t.Fatal("expected DLT not retry")
	}
	if out.DLTPayload == nil {
		t.Fatal("expected DLT")
	}
}

type rubyStub struct{ err error }

func (r rubyStub) Execute(context.Context, protocol.JobMessage) error { return r.err }
