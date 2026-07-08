package kbatch

import "encoding/json"

// ExecuteRequest is the JSON body for POST /v1/execute.
// Golden fixture: protocol/execute_request.json
type ExecuteRequest struct {
	JobType    string                 `json:"job_type"`
	JobID      string                 `json:"job_id"`
	BatchID    *string                `json:"batch_id,omitempty"`
	Attempt    int                    `json:"attempt"`
	Payload    map[string]interface{} `json:"payload"`
	TenantID   *string                `json:"tenant_id,omitempty"`
	EnqueuedAt *string                `json:"enqueued_at,omitempty"`
}

// ExecuteResponse is returned by the sidecar after running a handler.
type ExecuteResponse struct {
	OK           bool   `json:"ok"`
	ErrorClass   string `json:"error_class,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

func (r ExecuteResponse) MarshalJSON() ([]byte, error) {
	type alias ExecuteResponse
	if r.OK {
		return json.Marshal(struct {
			OK bool `json:"ok"`
		}{OK: true})
	}
	return json.Marshal(alias(r))
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	OK bool `json:"ok"`
}
