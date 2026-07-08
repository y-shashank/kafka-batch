package protocol

import "time"

// JobMessage is the Kafka job envelope (Ruby Batch.build_message_for).
type JobMessage struct {
	JobID                string                 `json:"job_id"`
	BatchID              *string                `json:"batch_id,omitempty"`
	JobType              string                 `json:"job_type"`
	WorkerClass          string                 `json:"worker_class"`
	Payload              map[string]interface{} `json:"payload"`
	Attempt              int                    `json:"attempt"`
	MaxRetries           int                    `json:"max_retries"`
	CompleteAfterRetries int                    `json:"complete_after_retries"`
	EnqueuedAt           string                 `json:"enqueued_at,omitempty"`
	TenantID             *string                `json:"tenant_id,omitempty"`
	BatchSeq             *int64                 `json:"batch_seq,omitempty"`
	RetryTier            string                 `json:"retry_tier,omitempty"`
	ValidTill            string                 `json:"valid_till,omitempty"`
	BatchCounted         bool                   `json:"batch_counted,omitempty"`
	RetryAfter           string                 `json:"retry_after,omitempty"`
	RetryTo              string                 `json:"retry_to,omitempty"`
	UniqFP               string                 `json:"_uniq_fp,omitempty"`
}

// EventMessage is produced to the events topic after job completion.
type EventMessage struct {
	BatchID      string `json:"batch_id"`
	JobID        string `json:"job_id"`
	Status       string `json:"status"` // success | failed
	WorkerClass  string `json:"worker_class"`
	OccurredAt   string `json:"occurred_at"`
	SrcTopic     string `json:"src_topic"`
	SrcPartition int32  `json:"src_partition"`
	SrcOffset    int64  `json:"src_offset"`
	BatchSeq     int64  `json:"batch_seq"`
}

// CallbackMessage is produced when a batch finalizes.
type CallbackMessage struct {
	BatchID        string                 `json:"batch_id"`
	Outcome        string                 `json:"outcome"` // success | complete
	TotalJobs      int64                  `json:"total_jobs"`
	CompletedCount int64                  `json:"completed_count"`
	FailedCount    int64                  `json:"failed_count"`
	OnSuccess      string                 `json:"on_success,omitempty"`
	OnComplete     string                 `json:"on_complete,omitempty"`
	Meta           map[string]interface{} `json:"meta,omitempty"`
	FinishedAt     string                 `json:"finished_at"`
}

// SourceCoords identifies a consumed Kafka record.
type SourceCoords struct {
	Topic     string
	Partition int32
	Offset    int64
}

func NowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}
