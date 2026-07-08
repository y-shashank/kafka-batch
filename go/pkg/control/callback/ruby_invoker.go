package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/y-shashank/kafka-batch/go/pkg/protocol"
)

// RubySocketInvoker calls a Ruby callback server over a Unix domain socket.
type RubySocketInvoker struct {
	SocketPath string
	Timeout    time.Duration
}

type rubyCallbackRequest struct {
	ClassName  string                 `json:"class_name"`
	MethodName string                 `json:"method_name"`
	Summary    map[string]interface{} `json:"summary"`
}

// Invoke runs on_success / on_complete via the Ruby callback server.
func (r RubySocketInvoker) Invoke(ctx context.Context, cb protocol.CallbackMessage) error {
	summary := map[string]interface{}{
		"batch_id":        cb.BatchID,
		"outcome":         cb.Outcome,
		"total_jobs":      cb.TotalJobs,
		"completed_count": cb.CompletedCount,
		"failed_count":    cb.FailedCount,
		"on_success":      cb.OnSuccess,
		"on_complete":     cb.OnComplete,
		"finished_at":     cb.FinishedAt,
	}
	if cb.Meta != nil {
		summary["meta"] = cb.Meta
	}
	if cb.Outcome == "success" && cb.OnSuccess != "" {
		if err := r.call(ctx, cb.OnSuccess, "on_success", summary); err != nil {
			return err
		}
	}
	if cb.OnComplete != "" {
		if err := r.call(ctx, cb.OnComplete, "on_complete", summary); err != nil {
			return err
		}
	}
	return nil
}

func (r RubySocketInvoker) call(ctx context.Context, className, methodName string, summary map[string]interface{}) error {
	body, err := json.Marshal(rubyCallbackRequest{
		ClassName: className, MethodName: methodName, Summary: summary,
	})
	if err != nil {
		return err
	}
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix/v1/callback", bytes.NewReader(body))
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
		return fmt.Errorf("ruby callback request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ruby callback HTTP %d", resp.StatusCode)
	}
	return nil
}
