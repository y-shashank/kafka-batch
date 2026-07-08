package retry

import (
	"context"
	"encoding/json"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

// Producer publishes Kafka messages.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// Processor handles retry-tier messages.
type Processor struct {
	Producer Producer
	Now      func() time.Time
	MaxPause time.Duration
}

// Outcome for one retry message.
type Outcome struct {
	CommitOffset bool
	Pause        bool
	PauseFor     time.Duration
	ProduceTopic string
	ProduceKey   string
	ProduceBody  []byte
	DLTPayload   []byte
	DLTKey       string
	Event        *protocol.EventMessage
}

func (p *Processor) Process(ctx context.Context, raw []byte, src protocol.SourceCoords) (Outcome, error) {
	out := Outcome{CommitOffset: true}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		dlt, key := dltRaw(raw, src.Topic)
		out.DLTPayload = dlt
		out.DLTKey = key
		return out, nil
	}

	retryTo, _ := m["retry_to"].(string)
	if retryTo == "" {
		if ev := failedEvent(m, src); ev != nil {
			out.Event = ev
		}
		dlt, key := dltMap(m, raw, src.Topic)
		out.DLTPayload = dlt
		out.DLTKey = key
		return out, nil
	}

	retryAfter := parseTime(m["retry_after"])
	wait := time.Duration(0)
	if retryAfter != nil {
		wait = retryAfter.Sub(p.now())
	}
	if wait > 0 {
		if wait > p.MaxPause {
			wait = p.MaxPause
		}
		out.CommitOffset = false
		out.Pause = true
		out.PauseFor = wait
		return out, nil
	}

	delete(m, "retry_after")
	delete(m, "retry_to")
	body, _ := json.Marshal(m)
	key, _ := m["job_id"].(string)
	out.ProduceTopic = retryTo
	out.ProduceKey = key
	out.ProduceBody = body
	return out, nil
}

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func parseTime(v interface{}) *time.Time {
	s, ok := v.(string)
	if !ok || s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return &t
}

func dltRaw(raw []byte, topic string) ([]byte, string) {
	m := map[string]interface{}{
		"dlt_type": "retry_routing", "dlt_source_topic": topic,
		"dlt_raw_payload": string(raw), "dlt_at": protocol.NowISO(),
	}
	b, _ := json.Marshal(m)
	return b, "retry"
}

func dltMap(m map[string]interface{}, raw []byte, topic string) ([]byte, string) {
	m["dlt_type"] = "retry_routing"
	m["dlt_source_topic"] = topic
	m["dlt_raw_payload"] = string(raw)
	m["dlt_at"] = protocol.NowISO()
	b, _ := json.Marshal(m)
	key, _ := m["job_id"].(string)
	if key == "" {
		key = "retry"
	}
	return b, key
}

func failedEvent(m map[string]interface{}, src protocol.SourceCoords) *protocol.EventMessage {
	batchID, _ := m["batch_id"].(string)
	seqF, _ := m["batch_seq"].(float64)
	if batchID == "" || seqF <= 0 {
		return nil
	}
	return &protocol.EventMessage{
		BatchID: batchID, JobID: str(m["job_id"]), Status: "failed",
		WorkerClass: str(m["worker_class"]), OccurredAt: protocol.NowISO(),
		SrcTopic: src.Topic, SrcPartition: src.Partition, SrcOffset: src.Offset,
		BatchSeq: int64(seqF),
	}
}

func str(v interface{}) string {
	s, _ := v.(string)
	return s
}
