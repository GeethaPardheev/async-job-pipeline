package controlplane

import (
	"encoding/json"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/google/uuid"
)

type attemptResponse struct {
	AttemptNumber int                  `json:"attempt_number"`
	WorkerID      string               `json:"worker_id"`
	Status        domain.AttemptStatus `json:"status"`
	Error         *string              `json:"error"`
	StartedAt     time.Time            `json:"started_at"`
	FinishedAt    *time.Time           `json:"finished_at"`
	DurationMS    *int64               `json:"duration_ms"`
}

type jobResponse struct {
	ID             uuid.UUID         `json:"id"`
	Type           string            `json:"type"`
	Status         domain.Status     `json:"status"`
	Priority       int               `json:"priority"`
	Payload        json.RawMessage   `json:"payload"`
	AttemptCount   int               `json:"attempt_count"`
	MaxRetries     int               `json:"max_retries"`
	TimeoutSeconds int               `json:"timeout_seconds"`
	LastError      *string           `json:"last_error"`
	Result         json.RawMessage   `json:"result"`
	RunAt          time.Time         `json:"run_at"`
	IdempotencyKey *string           `json:"idempotency_key"`
	WorkerID       *string           `json:"worker_id"`
	ScheduleID     *uuid.UUID        `json:"schedule_id"`
	ReplayOf       *uuid.UUID        `json:"replay_of"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
	StartedAt      *time.Time        `json:"started_at"`
	FinishedAt     *time.Time        `json:"finished_at"`
	Attempts       []attemptResponse `json:"attempts,omitempty"`
}

func toJobResponse(j domain.Job, attempts []domain.Attempt) jobResponse {
	out := jobResponse{
		ID: j.ID, Type: j.Type, Status: j.Status, Priority: j.Priority, Payload: j.Payload,
		AttemptCount: j.AttemptCount, MaxRetries: j.MaxRetries, TimeoutSeconds: j.TimeoutSeconds,
		LastError: j.LastError, Result: j.Result, RunAt: j.RunAt, IdempotencyKey: j.IdempotencyKey,
		WorkerID: j.WorkerID, ScheduleID: j.ScheduleID, ReplayOf: j.ReplayOf,
		CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt,
	}
	if attempts != nil {
		out.Attempts = make([]attemptResponse, len(attempts))
		for i, a := range attempts {
			out.Attempts[i] = attemptResponse{
				AttemptNumber: a.AttemptNumber, WorkerID: a.WorkerID, Status: a.Status, Error: a.Error,
				StartedAt: a.StartedAt, FinishedAt: a.FinishedAt, DurationMS: a.DurationMS,
			}
		}
	}
	return out
}

type submitResponse struct {
	JobID  uuid.UUID     `json:"job_id"`
	Status domain.Status `json:"status"`
	RunAt  time.Time     `json:"run_at"`
}

type dlqEntryResponse struct {
	JobID          uuid.UUID       `json:"job_id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxRetries     int             `json:"max_retries"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	AttemptCount   int             `json:"attempt_count"`
	LastError      *string         `json:"last_error"`
	DeadLetteredAt time.Time       `json:"dead_lettered_at"`
	ReplayedAt     *time.Time      `json:"replayed_at"`
	ReplayJobID    *uuid.UUID      `json:"replay_job_id"`
}

func toDLQResponse(e store.DLQEntry) dlqEntryResponse {
	return dlqEntryResponse{
		JobID: e.JobID, Type: e.Type, Payload: e.Payload, Priority: e.Priority, MaxRetries: e.MaxRetries,
		TimeoutSeconds: e.TimeoutSeconds, AttemptCount: e.AttemptCount, LastError: e.LastError,
		DeadLetteredAt: e.DeadLetteredAt, ReplayedAt: e.ReplayedAt, ReplayJobID: e.ReplayJobID,
	}
}

type replayResponse struct {
	JobID    uuid.UUID     `json:"job_id"`
	ReplayOf uuid.UUID     `json:"replay_of"`
	Status   domain.Status `json:"status"`
}

type scheduleResponse struct {
	ID             uuid.UUID       `json:"id"`
	Name           *string         `json:"name"`
	CronExpr       string          `json:"cron_expr"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxRetries     int             `json:"max_retries"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	Enabled        bool            `json:"enabled"`
	NextRunAt      time.Time       `json:"next_run_at"`
	LastRunAt      *time.Time      `json:"last_run_at"`
	LastJobID      *uuid.UUID      `json:"last_job_id"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

func toScheduleResponse(sc store.Schedule) scheduleResponse {
	return scheduleResponse{
		ID: sc.ID, Name: sc.Name, CronExpr: sc.CronExpr, Type: sc.Type, Payload: sc.Payload, Priority: sc.Priority,
		MaxRetries: sc.MaxRetries, TimeoutSeconds: sc.TimeoutSeconds, Enabled: sc.Enabled, NextRunAt: sc.NextRunAt,
		LastRunAt: sc.LastRunAt, LastJobID: sc.LastJobID, CreatedAt: sc.CreatedAt, UpdatedAt: sc.UpdatedAt,
	}
}

type priorityDepthResponse struct {
	Priority int   `json:"priority"`
	Queued   int64 `json:"queued"`
	Running  int64 `json:"running"`
}

type depthResponse struct {
	ByStatus   map[domain.Status]int64 `json:"by_status"`
	ByPriority []priorityDepthResponse `json:"by_priority"`
	Ready      int64                   `json:"ready"`
	Delayed    int64                   `json:"delayed"`
}

type statsResponse struct {
	GeneratedAt       time.Time     `json:"generated_at"`
	WindowSeconds     int64         `json:"window_seconds"`
	Draining          bool          `json:"draining"`
	DrainMode         *string       `json:"drain_mode"`
	Depth             depthResponse `json:"depth"`
	ActiveWorkers     int64         `json:"active_workers"`
	TotalConcurrency  int64         `json:"total_concurrency"`
	ActiveJobs        int64         `json:"active_jobs"`
	WorkerUtilization float64       `json:"worker_utilization"`
	P50LatencyMS      *float64      `json:"p50_latency_ms"`
	P95LatencyMS      *float64      `json:"p95_latency_ms"`
	DeadLetterRate    float64       `json:"dead_letter_rate"`
	DeadLetterCount   int64         `json:"dead_letter_count"`
}

func toStatsResponse(st store.QueueStats) statsResponse {
	byPrio := make([]priorityDepthResponse, len(st.ByPriority))
	for i, p := range st.ByPriority {
		byPrio[i] = priorityDepthResponse{Priority: p.Priority, Queued: p.Queued, Running: p.Running}
	}
	return statsResponse{
		GeneratedAt: st.GeneratedAt, WindowSeconds: int64(st.Window.Seconds()), Draining: st.Draining, DrainMode: st.DrainMode,
		Depth:         depthResponse{ByStatus: st.ByStatus, ByPriority: byPrio, Ready: st.Ready, Delayed: st.Delayed},
		ActiveWorkers: st.ActiveWorkers, TotalConcurrency: st.TotalConcurrency, ActiveJobs: st.ActiveJobs,
		WorkerUtilization: st.WorkerUtilization, P50LatencyMS: st.P50LatencyMS, P95LatencyMS: st.P95LatencyMS,
		DeadLetterRate: st.DeadLetterRate, DeadLetterCount: st.DeadLetterCount,
	}
}
