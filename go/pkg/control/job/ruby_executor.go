package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/kbatch"
	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

// RubyExecutor runs Ruby Worker#perform via the worker-server Unix socket.
type RubyExecutor interface {
	Execute(ctx context.Context, job protocol.JobMessage) error
}

// RubySocketExecutor implements RubyExecutor using POST /v1/execute.
type RubySocketExecutor struct {
	SocketPath string
	Timeout    time.Duration
}

// RubyExecutionError is returned when the Ruby worker raises or rejects a job.
type RubyExecutionError struct {
	Class   string
	Message string
}

func (e *RubyExecutionError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Class
}

type rubyExecuteResponse struct {
	OK           bool   `json:"ok"`
	ErrorClass   string `json:"error_class"`
	ErrorMessage string `json:"error_message"`
}

// Execute calls the Ruby worker server (protocol/execute_request.json).
func (r RubySocketExecutor) Execute(ctx context.Context, job protocol.JobMessage) error {
	body, err := json.Marshal(buildRubyExecuteRequest(job))
	if err != nil {
		return err
	}

	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 300 * time.Second
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix/v1/execute", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", r.SocketPath)
			},
		},
		Timeout: timeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		return &RubyExecutionError{
			Class:   "RubyWorkerUnavailable",
			Message: fmt.Sprintf("ruby worker request: %v", err),
		}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("read ruby worker response: %w", err)
	}

	var parsed rubyExecuteResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return fmt.Errorf("ruby worker invalid json (HTTP %d): %w", resp.StatusCode, err)
	}
	if parsed.OK {
		return nil
	}

	errClass := parsed.ErrorClass
	if errClass == "" {
		errClass = "RubyExecutionError"
	}
	msg := parsed.ErrorMessage
	if msg == "" {
		msg = fmt.Sprintf("ruby worker HTTP %d", resp.StatusCode)
	}
	return &RubyExecutionError{Class: errClass, Message: msg}
}

func buildRubyExecuteRequest(job protocol.JobMessage) map[string]interface{} {
	req := map[string]interface{}{
		"job_type": job.JobType,
		"job_id":   job.JobID,
		"attempt":  job.Attempt,
		"payload":  job.Payload,
	}
	if job.BatchID != nil {
		req["batch_id"] = *job.BatchID
	}
	if job.WorkerClass != "" {
		req["worker_class"] = job.WorkerClass
	}
	if job.TenantID != nil {
		req["tenant_id"] = *job.TenantID
	}
	if job.EnqueuedAt != "" {
		req["enqueued_at"] = job.EnqueuedAt
	}
	if job.BatchSeq != nil {
		req["batch_seq"] = *job.BatchSeq
	}
	if job.MaxRetries > 0 {
		req["max_retries"] = job.MaxRetries
	}
	if job.RetryTier != "" {
		req["retry_tier"] = job.RetryTier
	}
	if job.ValidTill != "" {
		req["valid_till"] = job.ValidTill
	}
	if job.UniqFP != "" {
		req["_uniq_fp"] = job.UniqFP
	}
	return req
}

func rubyClassName(err error) string {
	if re, ok := err.(*RubyExecutionError); ok && re.Class != "" {
		return re.Class
	}
	if he, ok := err.(*kbatch.HandlerError); ok && he.Class != "" {
		return he.Class
	}
	return "RubyExecutionError"
}
