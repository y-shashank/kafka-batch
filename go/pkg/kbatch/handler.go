package kbatch

import (
	"fmt"
	"sync"
)

// Context is passed to registered Go job handlers.
type Context struct {
	JobType    string
	JobID      string
	BatchID    string
	Attempt    int
	Payload    map[string]interface{}
	TenantID   string
	EnqueuedAt string
}

// HandlerFunc runs one job. Return nil on success or an error to fail the job.
type HandlerFunc func(ctx *Context) error

// HandlerError carries a stable error_class for the Ruby control plane.
type HandlerError struct {
	Class   string
	Message string
}

func (e *HandlerError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Class
}

var (
	registryMu sync.RWMutex
	registry   = map[string]HandlerFunc{}
)

// Register binds a job_type to a Go handler. Panics on duplicate registration.
func Register(jobType string, fn HandlerFunc) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[jobType]; exists {
		panic(fmt.Sprintf("kbatch: job_type %q already registered", jobType))
	}
	registry[jobType] = fn
}

// Lookup returns the handler for job_type.
func Lookup(jobType string) (HandlerFunc, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	fn, ok := registry[jobType]
	return fn, ok
}

// Reset clears all registrations (tests only).
func Reset() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = map[string]HandlerFunc{}
}

func runHandler(req ExecuteRequest) ExecuteResponse {
	fn, ok := Lookup(req.JobType)
	if !ok {
		return ExecuteResponse{
			OK:           false,
			ErrorClass:   "UnknownHandler",
			ErrorMessage: fmt.Sprintf("unknown job_type: %s", req.JobType),
		}
	}

	ctx := &Context{
		JobType: req.JobType,
		JobID:   req.JobID,
		Attempt: req.Attempt,
		Payload: req.Payload,
	}
	if req.BatchID != nil {
		ctx.BatchID = *req.BatchID
	}
	if req.TenantID != nil {
		ctx.TenantID = *req.TenantID
	}
	if req.EnqueuedAt != nil {
		ctx.EnqueuedAt = *req.EnqueuedAt
	}
	if ctx.Payload == nil {
		ctx.Payload = map[string]interface{}{}
	}

	if err := fn(ctx); err != nil {
		class := "GoExecutionError"
		message := err.Error()
		if he, ok := err.(*HandlerError); ok {
			if he.Class != "" {
				class = he.Class
			}
			if he.Message != "" {
				message = he.Message
			}
		}
		return ExecuteResponse{
			OK:           false,
			ErrorClass:   class,
			ErrorMessage: message,
		}
	}

	return ExecuteResponse{OK: true}
}
