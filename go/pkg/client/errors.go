package client

import "fmt"

// BatchClosedError is raised when pushing into a completed or cancelled batch.
type BatchClosedError struct {
	BatchID string
	Reason  string
}

func (e BatchClosedError) Error() string {
	return fmt.Sprintf("batch %s is %s", e.BatchID, e.Reason)
}

// BatchNotFoundError is raised when a batch id is missing from the store.
type BatchNotFoundError struct {
	BatchID string
}

func (e BatchNotFoundError) Error() string {
	return fmt.Sprintf("batch %s not found", e.BatchID)
}

// DuplicateJobError is raised when uniq_on_duplicate is raise.
type DuplicateJobError struct {
	WorkerClass string
}

func (e DuplicateJobError) Error() string {
	return fmt.Sprintf("duplicate uniq job for %s", e.WorkerClass)
}

// UnknownHandlerError is raised for unknown manifest job types.
type UnknownHandlerError struct {
	JobType string
}

func (e UnknownHandlerError) Error() string {
	return fmt.Sprintf("unknown job_type %q", e.JobType)
}

// PartialProduceError reports a gap in bulk produce (Ruby PartialProduceError).
type PartialProduceError struct {
	Message       string
	ProducedCount int
}

func (e PartialProduceError) Error() string {
	return e.Message
}

// ConfigurationError indicates missing client configuration.
type ConfigurationError struct {
	Message string
}

func (e ConfigurationError) Error() string {
	return e.Message
}
