package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Validation bounds shared by the API and the SQL CHECK constraints.
const (
	MinPriority       = -1000
	MaxPriority       = 1000
	MaxMaxRetries     = 25
	MinTimeoutSeconds = 1
	MaxTimeoutSeconds = 3600
	MaxTypeLen        = 64

	DefaultPriority       = 0
	DefaultMaxRetries     = 3
	DefaultTimeoutSeconds = 30
)

// Job mirrors a row of the jobs table.
type Job struct {
	ID             uuid.UUID
	Type           string
	Payload        json.RawMessage
	Priority       int
	Status         Status
	AttemptCount   int
	MaxRetries     int
	TimeoutSeconds int
	RunAt          time.Time
	IdempotencyKey *string
	WorkerID       *string
	LeaseExpiresAt *time.Time
	LastError      *string
	Result         json.RawMessage
	ScheduleID     *uuid.UUID
	ReplayOf       *uuid.UUID
	CreatedAt      time.Time
	UpdatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

// Attempt mirrors a row of the job_attempts table.
type Attempt struct {
	JobID         uuid.UUID
	AttemptNumber int
	WorkerID      string
	Status        AttemptStatus
	Error         *string
	StartedAt     time.Time
	FinishedAt    *time.Time
	DurationMS    *int64
}
