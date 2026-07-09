package instrument

import "time"

// JobPayload builds the common job-scoped instrumentation payload.
func JobPayload(jobID, batchID, workerClass string, extra map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{
		"job_id":       jobID,
		"batch_id":     batchID,
		"worker_class": workerClass,
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func JobProcessed(jobID, batchID, workerClass string, durationMs float64) {
	Emit("job.processed", JobPayload(jobID, batchID, workerClass, nil), durationMs)
}

func JobCancelled(jobID, batchID, workerClass string) {
	Emit("job.cancelled", JobPayload(jobID, batchID, workerClass, nil), 0)
}

func JobExpired(jobID, batchID, workerClass, validTill string) {
	Emit("job.expired", JobPayload(jobID, batchID, workerClass, map[string]interface{}{
		"valid_till": validTill,
	}), 0)
}

func JobRetried(jobID, batchID, workerClass string, attempt, nextAttempt int, retryTopic string) {
	Emit("job.retried", JobPayload(jobID, batchID, workerClass, map[string]interface{}{
		"attempt":      attempt,
		"next_attempt": nextAttempt,
		"retry_topic":  retryTopic,
	}), 0)
}

func JobFailed(jobID, batchID, workerClass string, attempt int, errClass, errMsg string) {
	Emit("job.failed", JobPayload(jobID, batchID, workerClass, map[string]interface{}{
		"attempt":       attempt,
		"error_class":   errClass,
		"error_message": errMsg,
	}), 0)
}

func JobEmitRetried(jobID, batchID string, attempt int, err error) {
	errClass, errMsg := "", ""
	if err != nil {
		errClass = err.Error()
		errMsg = err.Error()
	}
	Emit("job.emit_retried", map[string]interface{}{
		"job_id":        jobID,
		"batch_id":      batchID,
		"attempt":       attempt,
		"error_class":   errClass,
		"error_message": errMsg,
	}, 0)
}

func JobUniqSkipped(workerClass string, payload map[string]interface{}, jobID, batchID string) {
	Emit("job.uniq_skipped", map[string]interface{}{
		"worker_class": workerClass,
		"payload":      payload,
		"job_id":       jobID,
		"batch_id":     batchID,
	}, 0)
}

func DLTPublished(jobID, batchID, dltType, sourceTopic string) {
	Emit("dlt.published", map[string]interface{}{
		"job_id":       jobID,
		"batch_id":     batchID,
		"dlt_type":     dltType,
		"source_topic": sourceTopic,
	}, 0)
}

func BatchCreated(batchID, description, tenantID, onSuccess, onComplete string) {
	Emit("batch.created", map[string]interface{}{
		"batch_id":    batchID,
		"description": description,
		"tenant_id":   tenantID,
		"on_success":  onSuccess,
		"on_complete": onComplete,
	}, 0)
}

func BatchSealed(batchID string, totalJobs int64) {
	Emit("batch.sealed", map[string]interface{}{
		"batch_id":   batchID,
		"total_jobs": totalJobs,
	}, 0)
}

func BatchCompleted(batchID, outcome string, totalJobs, completedCount, failedCount int64) {
	Emit("batch.completed", map[string]interface{}{
		"batch_id":        batchID,
		"outcome":         outcome,
		"total_jobs":      totalJobs,
		"completed_count": completedCount,
		"failed_count":    failedCount,
	}, 0)
}

func ScheduledEnqueued(jobID, batchID, workerClass string, runAt time.Time) {
	Emit("scheduled.enqueued", map[string]interface{}{
		"job_id":       jobID,
		"batch_id":     batchID,
		"worker_class": workerClass,
		"run_at":       runAt.UTC().Format(time.RFC3339),
	}, 0)
}

func ScheduledEnqueuedBulk(count int, batchID, workerClass string, runAt time.Time) {
	Emit("scheduled.enqueued_bulk", map[string]interface{}{
		"count":        count,
		"batch_id":     batchID,
		"worker_class": workerClass,
		"run_at":       runAt.UTC().Format(time.RFC3339),
	}, 0)
}

func ScheduledIndexFailed(count int, batchID, jobID string, attempts int, err error) {
	errClass, errMsg := "", ""
	if err != nil {
		errClass = err.Error()
		errMsg = err.Error()
	}
	Emit("scheduled.index_failed", map[string]interface{}{
		"count":         count,
		"batch_id":      batchID,
		"job_id":        jobID,
		"attempts":      attempts,
		"error_class":   errClass,
		"error_message": errMsg,
	}, 0)
}

func ScheduledDispatched(jobID, batchID, workerClass, topic string) {
	Emit("scheduled.dispatched", map[string]interface{}{
		"job_id":       jobID,
		"batch_id":     batchID,
		"worker_class": workerClass,
		"topic":        topic,
	}, 0)
}

func CallbackInvoked(batchID, callbackClass, callbackMethod string) {
	Emit("callback.invoked", map[string]interface{}{
		"batch_id":        batchID,
		"callback_class":  callbackClass,
		"callback_method": callbackMethod,
	}, 0)
}

func CallbackFailed(batchID, callbackClass, callbackMethod, errClass, errMsg string) {
	Emit("callback.failed", map[string]interface{}{
		"batch_id":        batchID,
		"callback_class":  callbackClass,
		"callback_method": callbackMethod,
		"error_class":     errClass,
		"error_message":   errMsg,
	}, 0)
}

func ConsumerPriorityYielded(consumerClass, p0Topic, consumerGroup string, pauseMs int64, mode string, rank int, higherTopics []string) {
	Emit("consumer.priority_yielded", map[string]interface{}{
		"consumer_class": consumerClass,
		"p0_topic":       p0Topic,
		"consumer_group": consumerGroup,
		"pause_ms":       pauseMs,
		"mode":           mode,
		"rank":           rank,
		"higher_topics":  higherTopics,
	}, 0)
}

func ReconcilerRan(staleCount, lostCount int, duration time.Duration, triggeredBy string) {
	Emit("reconciler.ran", map[string]interface{}{
		"stale_count":    staleCount,
		"lost_count":     lostCount,
		"duration":       duration.Seconds(),
		"triggered_by":   triggeredBy,
	}, 0)
}
