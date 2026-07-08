package jobexpiry

import (
	"encoding/json"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

// ExpiredErrorClass matches KafkaBatch::JobExpiry::ExpiredError on the Ruby path.
const ExpiredErrorClass = "KafkaBatch::JobExpiry::ExpiredError"

// DropOutcome is the side effects of routing an expired job to DLT.
type DropOutcome struct {
	Event      *protocol.EventMessage
	DLTPayload []byte
	DLTKey     string
	Failure    *FailureRecord
}

// FailureRecord mirrors RedisStore#record_failure for dashboard parity.
type FailureRecord struct {
	BatchID      string
	JobID        string
	WorkerClass  string
	ErrorClass   string
	ErrorMessage string
	Status       string
	Attempt      int
}

// StampSource records immutable Kafka coordinates on fair ingest (Ruby JobExpiry.stamp_source!).
func StampSource(m map[string]interface{}, src protocol.SourceCoords) {
	if _, ok := m["_src_topic"]; !ok {
		m["_src_topic"] = src.Topic
	}
	if _, ok := m["_src_partition"]; !ok {
		m["_src_partition"] = src.Partition
	}
	if _, ok := m["_src_offset"]; !ok {
		m["_src_offset"] = src.Offset
	}
}

// SourceCoords reads stamped or inline source coordinates from a job map.
func SourceCoords(m map[string]interface{}) protocol.SourceCoords {
	topic, _ := m["_src_topic"].(string)
	if topic == "" {
		topic, _ = m["src_topic"].(string)
	}
	part := int32(0)
	if v, ok := m["_src_partition"].(float64); ok {
		part = int32(v)
	} else if v, ok := m["src_partition"].(float64); ok {
		part = int32(v)
	}
	off := int64(0)
	if v, ok := m["_src_offset"].(float64); ok {
		off = int64(v)
	} else if v, ok := m["src_offset"].(float64); ok {
		off = int64(v)
	}
	return protocol.SourceCoords{Topic: topic, Partition: part, Offset: off}
}

// BuildDrop constructs DLT payload, optional batch event, and failure record for an expired job.
func BuildDrop(raw []byte, src protocol.SourceCoords, now time.Time) DropOutcome {
	m := map[string]interface{}{}
	_ = json.Unmarshal(raw, &m)
	if m == nil {
		m = map[string]interface{}{}
	}

	validTill, _ := m["valid_till"].(string)
	msg := "Job expired (valid_till=" + validTill + ")"
	if validTill == "" {
		msg = "job expired"
	}

	dlt := cloneMap(m)
	dlt["dlt_type"] = "expired"
	dlt["dlt_source_topic"] = src.Topic
	dlt["dlt_error_class"] = ExpiredErrorClass
	dlt["dlt_error_message"] = msg
	dlt["dlt_at"] = now.UTC().Format(time.RFC3339)
	dltRaw, _ := json.Marshal(dlt)
	key, _ := m["job_id"].(string)
	if key == "" {
		key = "expired"
	}

	out := DropOutcome{DLTPayload: dltRaw, DLTKey: key}

	batchID, _ := m["batch_id"].(string)
	seqF, hasSeq := m["batch_seq"].(float64)
	batchCounted, _ := m["batch_counted"].(bool)
	if batchID != "" && hasSeq && seqF > 0 && !batchCounted {
		jobID, _ := m["job_id"].(string)
		worker, _ := m["worker_class"].(string)
		out.Event = &protocol.EventMessage{
			BatchID:      batchID,
			JobID:        jobID,
			Status:       "failed",
			WorkerClass:  worker,
			OccurredAt:   now.UTC().Format(time.RFC3339),
			SrcTopic:     src.Topic,
			SrcPartition: src.Partition,
			SrcOffset:    src.Offset,
			BatchSeq:     int64(seqF),
		}
		out.Failure = &FailureRecord{
			BatchID:      batchID,
			JobID:        jobID,
			WorkerClass:  worker,
			ErrorClass:   ExpiredErrorClass,
			ErrorMessage: msg,
			Status:       "expired",
			Attempt:      intFrom(m["attempt"]),
		}
	}
	return out
}

func cloneMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func intFrom(v interface{}) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return 0
	}
}
