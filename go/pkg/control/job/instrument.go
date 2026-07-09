package job

import (
	"encoding/json"

	"github.com/y-shashank/kafka-batch/go/pkg/instrument"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

func emitJobProcessed(job protocol.JobMessage, durationMs float64) {
	instrument.JobProcessed(job.JobID, deref(job.BatchID), job.WorkerClass, durationMs)
}

func emitJobCancelled(job protocol.JobMessage) {
	instrument.JobCancelled(job.JobID, deref(job.BatchID), job.WorkerClass)
}

func emitJobExpired(job protocol.JobMessage, validTill string) {
	instrument.JobExpired(job.JobID, deref(job.BatchID), job.WorkerClass, validTill)
}

func emitJobRetried(job protocol.JobMessage, nextAttempt int, retryTopic string) {
	instrument.JobRetried(job.JobID, deref(job.BatchID), job.WorkerClass, job.Attempt, nextAttempt, retryTopic)
}

func emitJobFailed(job protocol.JobMessage, attempt int, errClass, errMsg string) {
	instrument.JobFailed(job.JobID, deref(job.BatchID), job.WorkerClass, attempt, errClass, errMsg)
}

func emitDLTPublished(jobID, batchID, dltType, sourceTopic string) {
	instrument.DLTPublished(jobID, batchID, dltType, sourceTopic)
}

func dltMeta(raw []byte) (jobID, batchID, dltType string) {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	if s, ok := m["job_id"].(string); ok {
		jobID = s
	}
	if s, ok := m["batch_id"].(string); ok {
		batchID = s
	}
	if s, ok := m["dlt_type"].(string); ok {
		dltType = s
	}
	return jobID, batchID, dltType
}
