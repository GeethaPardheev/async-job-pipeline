package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const jobCols = `id, type, payload, priority, status, attempt_count, max_retries, timeout_seconds, run_at,
	idempotency_key, worker_id, lease_expires_at, last_error, result, schedule_id, replay_of,
	created_at, updated_at, started_at, finished_at`

// maxErrorLen bounds last_error / job_attempts.error so a chatty handler can't bloat hot rows.
const maxErrorLen = 4096

// querier is satisfied by *pgxpool.Pool, *pgxpool.Conn and pgx.Tx.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func scanJob(row pgx.Row) (domain.Job, error) {
	var j domain.Job
	err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Priority, &j.Status, &j.AttemptCount, &j.MaxRetries,
		&j.TimeoutSeconds, &j.RunAt, &j.IdempotencyKey, &j.WorkerID, &j.LeaseExpiresAt, &j.LastError,
		&j.Result, &j.ScheduleID, &j.ReplayOf, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt)
	return j, err
}

func (s *Store) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{}, fn)
}

func truncateErr(msg string) string {
	if len(msg) <= maxErrorLen {
		return msg
	}
	return msg[:maxErrorLen-3] + "..."
}

// jsonParam maps an empty RawMessage to SQL NULL.
func jsonParam(m json.RawMessage) any {
	if len(m) == 0 {
		return nil
	}
	return m
}

// NewJob is the input to EnqueueJob.
type NewJob struct {
	ID             uuid.UUID
	Type           string
	Payload        json.RawMessage
	Priority       int
	MaxRetries     int
	TimeoutSeconds int
	RunAt          *time.Time // nil → now()
	IdempotencyKey *string
	RequestHash    *string
	ScheduleID     *uuid.UUID
	ReplayOf       *uuid.UUID
}

// EnqueueResult reports whether EnqueueJob created a row or returned an idempotent match.
type EnqueueResult struct {
	Job     domain.Job
	Created bool
}

// EnqueueJob inserts a queued job (SPEC §6.8). With an idempotency key, a duplicate returns the
// existing job (Created=false), or ErrIdempotencyConflict when the request hash differs.
// Returns ErrDraining while the queue is draining.
func (s *Store) EnqueueJob(ctx context.Context, nj NewJob) (EnqueueResult, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	payload := nj.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}

	var res EnqueueResult
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var draining bool
		if err := tx.QueryRow(ctx, "SELECT draining FROM queue_state WHERE id").Scan(&draining); err != nil {
			return fmt.Errorf("read queue_state: %w", err)
		}
		if draining {
			return ErrDraining
		}

		j, err := scanJob(tx.QueryRow(ctx, `
			INSERT INTO jobs (id, type, payload, priority, max_retries, timeout_seconds, run_at,
			                  idempotency_key, request_hash, schedule_id, replay_of)
			VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $8, $9, $10, $11)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING `+jobCols,
			nj.ID, nj.Type, payload, nj.Priority, nj.MaxRetries, nj.TimeoutSeconds, nj.RunAt,
			nj.IdempotencyKey, nj.RequestHash, nj.ScheduleID, nj.ReplayOf))
		if err == nil {
			res = EnqueueResult{Job: j, Created: true}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("insert job: %w", err)
		}
		if nj.IdempotencyKey == nil {
			return errors.New("insert job: no row returned without idempotency key")
		}

		var existingHash *string
		row := tx.QueryRow(ctx, "SELECT "+jobCols+", request_hash FROM jobs WHERE idempotency_key = $1", *nj.IdempotencyKey)
		err = row.Scan(&j.ID, &j.Type, &j.Payload, &j.Priority, &j.Status, &j.AttemptCount, &j.MaxRetries,
			&j.TimeoutSeconds, &j.RunAt, &j.IdempotencyKey, &j.WorkerID, &j.LeaseExpiresAt, &j.LastError,
			&j.Result, &j.ScheduleID, &j.ReplayOf, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt, &existingHash)
		if err != nil {
			return fmt.Errorf("load idempotent job: %w", err)
		}
		if nj.RequestHash != nil && existingHash != nil && *nj.RequestHash != *existingHash {
			return ErrIdempotencyConflict
		}
		res = EnqueueResult{Job: j, Created: false}
		return nil
	})
	if err != nil {
		return EnqueueResult{}, err
	}
	return res, nil
}

// GetJob loads one job.
func (s *Store) GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	j, err := scanJob(s.pool.QueryRow(ctx, "SELECT "+jobCols+" FROM jobs WHERE id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, ErrNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// ListAttempts returns the most recent attempts of a job, newest first.
func (s *Store) ListAttempts(ctx context.Context, jobID uuid.UUID, limit int) ([]domain.Attempt, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT job_id, attempt_number, worker_id, status, error, started_at, finished_at, duration_ms
		  FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number DESC LIMIT $2`, jobID, limit)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	attempts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Attempt, error) {
		var a domain.Attempt
		err := r.Scan(&a.JobID, &a.AttemptNumber, &a.WorkerID, &a.Status, &a.Error, &a.StartedAt, &a.FinishedAt, &a.DurationMS)
		return a, err
	})
	if err != nil {
		return nil, fmt.Errorf("scan attempts: %w", err)
	}
	return attempts, nil
}
