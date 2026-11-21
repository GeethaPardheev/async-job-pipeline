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
)

// Drain modes (must match the queue_state.drain_mode CHECK constraint).
const (
	DrainPause         = "pause"
	DrainCancelPending = "cancel_pending"
)

// ListJobsFilter selects jobs for ListJobs.
type ListJobsFilter struct {
	Status *domain.Status
	Type   *string
	Limit  int
}

// ListJobs returns jobs newest first.
func (s *Store) ListJobs(ctx context.Context, f ListJobsFilter) ([]domain.Job, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	var status, typ any
	if f.Status != nil {
		status = string(*f.Status)
	}
	if f.Type != nil {
		typ = *f.Type
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobCols+` FROM jobs
		 WHERE ($1::text IS NULL OR status = $1) AND ($2::text IS NULL OR type = $2)
		 ORDER BY created_at DESC, id DESC
		 LIMIT $3`, status, typ, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Job, error) { return scanJob(r) })
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return jobs, nil
}

// CancelJob is T7: queued → cancelled. An already-cancelled job is returned as-is (idempotent).
// Any other state returns the job with ErrConflict; a missing job returns ErrNotFound.
func (s *Store) CancelJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	j, err := scanJob(s.pool.QueryRow(ctx, `
		UPDATE jobs SET status = 'cancelled', finished_at = now(), updated_at = now()
		 WHERE id = $1 AND status = 'queued'
		RETURNING `+jobCols, id))
	if err == nil {
		return j, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, fmt.Errorf("cancel job: %w", err)
	}
	cur, err := s.GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	if cur.Status == domain.StatusCancelled {
		return cur, nil
	}
	return cur, ErrConflict
}

// QueueState is the persisted drain flag.
type QueueState struct {
	Draining  bool
	DrainMode *string
	UpdatedAt time.Time
}

// GetQueueState reads the singleton queue_state row.
func (s *Store) GetQueueState(ctx context.Context) (QueueState, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	var qs QueueState
	if err := s.pool.QueryRow(ctx, "SELECT draining, drain_mode, updated_at FROM queue_state WHERE id").
		Scan(&qs.Draining, &qs.DrainMode, &qs.UpdatedAt); err != nil {
		return QueueState{}, fmt.Errorf("read queue_state: %w", err)
	}
	return qs, nil
}

// DrainResult reports the effect of DrainQueue.
type DrainResult struct {
	Mode      string
	Cancelled int64
	Queued    int64
	Running   int64
}

// DrainQueue sets draining=true (stops intake and new claims). With cancel_pending it also cancels
// every queued job in the same transaction.
func (s *Store) DrainQueue(ctx context.Context, mode string) (DrainResult, error) {
	if mode != DrainPause && mode != DrainCancelPending {
		return DrainResult{}, fmt.Errorf("invalid drain mode %q", mode)
	}
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	res := DrainResult{Mode: mode}
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		res = DrainResult{Mode: mode}
		if _, err := tx.Exec(ctx, "UPDATE queue_state SET draining = true, drain_mode = $1, updated_at = now() WHERE id", mode); err != nil {
			return fmt.Errorf("set draining: %w", err)
		}
		if mode == DrainCancelPending {
			tag, err := tx.Exec(ctx, `
				UPDATE jobs SET status = 'cancelled', finished_at = now(), updated_at = now()
				 WHERE status = 'queued'`)
			if err != nil {
				return fmt.Errorf("cancel queued jobs: %w", err)
			}
			res.Cancelled = tag.RowsAffected()
		}
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE status = 'queued'), count(*) FILTER (WHERE status = 'running')
			  FROM jobs WHERE status IN ('queued', 'running')`).Scan(&res.Queued, &res.Running); err != nil {
			return fmt.Errorf("count backlog: %w", err)
		}
		return nil
	})
	if err != nil {
		return DrainResult{}, err
	}
	return res, nil
}

// ResumeQueue clears the drain flag.
func (s *Store) ResumeQueue(ctx context.Context) error {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	if _, err := s.pool.Exec(ctx, "UPDATE queue_state SET draining = false, drain_mode = NULL, updated_at = now() WHERE id"); err != nil {
		return fmt.Errorf("resume queue: %w", err)
	}
	return nil
}

// DLQEntry mirrors a row of dead_letter_jobs (without the snapshot).
type DLQEntry struct {
	JobID          uuid.UUID
	Type           string
	Payload        json.RawMessage
	Priority       int
	MaxRetries     int
	TimeoutSeconds int
	AttemptCount   int
	LastError      *string
	DeadLetteredAt time.Time
	ReplayedAt     *time.Time
	ReplayJobID    *uuid.UUID
}

// ListDLQ returns dead-lettered entries, newest first.
func (s *Store) ListDLQ(ctx context.Context, limit int) ([]DLQEntry, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT job_id, type, payload, priority, max_retries, timeout_seconds, attempt_count, last_error,
		       dead_lettered_at, replayed_at, replay_job_id
		  FROM dead_letter_jobs ORDER BY dead_lettered_at DESC, job_id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list dlq: %w", err)
	}
	entries, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (DLQEntry, error) {
		var e DLQEntry
		err := r.Scan(&e.JobID, &e.Type, &e.Payload, &e.Priority, &e.MaxRetries, &e.TimeoutSeconds, &e.AttemptCount,
			&e.LastError, &e.DeadLetteredAt, &e.ReplayedAt, &e.ReplayJobID)
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("list dlq: %w", err)
	}
	return entries, nil
}

// ReplayKey is the idempotency key of the job created by replaying a DLQ entry.
func ReplayKey(jobID uuid.UUID) string { return "dlq-replay:" + jobID.String() }

// ReplayDLQJob re-queues a dead-lettered job as a new job (replay_of = original). Repeat calls
// return the job created by the first replay (created=false). ErrNotFound if there is no DLQ
// entry; ErrDraining if a new job would have to be created while draining.
func (s *Store) ReplayDLQJob(ctx context.Context, jobID, newID uuid.UUID) (job domain.Job, created bool, err error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	err = s.withTx(ctx, func(tx pgx.Tx) error {
		var e DLQEntry
		err := tx.QueryRow(ctx, `
			SELECT job_id, type, payload, priority, max_retries, timeout_seconds, replay_job_id
			  FROM dead_letter_jobs WHERE job_id = $1 FOR UPDATE`, jobID).
			Scan(&e.JobID, &e.Type, &e.Payload, &e.Priority, &e.MaxRetries, &e.TimeoutSeconds, &e.ReplayJobID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("load dlq entry: %w", err)
		}
		if e.ReplayJobID != nil {
			j, err := scanJob(tx.QueryRow(ctx, "SELECT "+jobCols+" FROM jobs WHERE id = $1", *e.ReplayJobID))
			if err != nil {
				return fmt.Errorf("load replayed job: %w", err)
			}
			job, created = j, false
			return nil
		}

		var draining bool
		if err := tx.QueryRow(ctx, "SELECT draining FROM queue_state WHERE id").Scan(&draining); err != nil {
			return fmt.Errorf("read queue_state: %w", err)
		}
		if draining {
			return ErrDraining
		}
		j, err := scanJob(tx.QueryRow(ctx, `
			INSERT INTO jobs (id, type, payload, priority, max_retries, timeout_seconds, run_at, idempotency_key, replay_of)
			VALUES ($1, $2, $3, $4, $5, $6, now(), $7, $8)
			RETURNING `+jobCols,
			newID, e.Type, e.Payload, e.Priority, e.MaxRetries, e.TimeoutSeconds, ReplayKey(jobID), jobID))
		if err != nil {
			return fmt.Errorf("insert replay job: %w", err)
		}
		if _, err := tx.Exec(ctx, "UPDATE dead_letter_jobs SET replayed_at = now(), replay_job_id = $2 WHERE job_id = $1", jobID, j.ID); err != nil {
			return fmt.Errorf("mark dlq entry replayed: %w", err)
		}
		job, created = j, true
		return nil
	})
	if err != nil {
		return domain.Job{}, false, err
	}
	return job, created, nil
}

// ListSchedules returns schedules, newest first.
func (s *Store) ListSchedules(ctx context.Context, limit int) ([]Schedule, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, "SELECT "+scheduleCols+" FROM schedules ORDER BY created_at DESC, id DESC LIMIT $1", limit)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Schedule, error) { return scanSchedule(r) })
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	return out, nil
}

// DeleteSchedule removes a schedule; jobs it enqueued keep running with schedule_id = NULL.
func (s *Store) DeleteSchedule(ctx context.Context, id uuid.UUID) error {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, "DELETE FROM schedules WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// PriorityDepth is the queued/running count for one priority.
type PriorityDepth struct {
	Priority int
	Queued   int64
	Running  int64
}

// QueueStats is the snapshot behind GET /v1/queue/stats and /metrics (SPEC §8.9).
type QueueStats struct {
	GeneratedAt       time.Time
	Window            time.Duration
	Draining          bool
	DrainMode         *string
	ByStatus          map[domain.Status]int64
	ByPriority        []PriorityDepth
	Ready             int64
	Delayed           int64
	ActiveWorkers     int64
	TotalConcurrency  int64
	ActiveJobs        int64
	WorkerUtilization float64
	P50LatencyMS      *float64
	P95LatencyMS      *float64
	DeadLetterRate    float64
	DeadLetterCount   int64
	AttemptsInWindow  map[domain.AttemptStatus]int64
}

// GetQueueStats computes queue statistics from one REPEATABLE READ snapshot. Worker aggregates
// count only workers whose last heartbeat is within staleAfter; latency and DLQ rate use window.
func (s *Store) GetQueueStats(ctx context.Context, staleAfter, window time.Duration) (QueueStats, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	st := QueueStats{
		Window:           window,
		ByStatus:         make(map[domain.Status]int64, len(domain.AllStatuses)),
		AttemptsInWindow: make(map[domain.AttemptStatus]int64, len(domain.AllAttemptStatuses)),
	}
	for _, status := range domain.AllStatuses {
		st.ByStatus[status] = 0
	}
	for _, a := range domain.AllAttemptStatuses {
		st.AttemptsInWindow[a] = 0
	}

	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT now(), draining, drain_mode FROM queue_state WHERE id").
			Scan(&st.GeneratedAt, &st.Draining, &st.DrainMode); err != nil {
			return fmt.Errorf("queue state: %w", err)
		}

		rows, err := tx.Query(ctx, "SELECT status, count(*) FROM jobs GROUP BY status")
		if err != nil {
			return fmt.Errorf("depth by status: %w", err)
		}
		type statusCount struct {
			status string
			n      int64
		}
		counts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (statusCount, error) {
			var c statusCount
			err := r.Scan(&c.status, &c.n)
			return c, err
		})
		if err != nil {
			return fmt.Errorf("depth by status: %w", err)
		}
		for _, c := range counts {
			st.ByStatus[domain.Status(c.status)] = c.n
		}

		rows, err = tx.Query(ctx, `
			SELECT priority, count(*) FILTER (WHERE status = 'queued'), count(*) FILTER (WHERE status = 'running')
			  FROM jobs WHERE status IN ('queued', 'running')
			 GROUP BY priority ORDER BY priority DESC`)
		if err != nil {
			return fmt.Errorf("depth by priority: %w", err)
		}
		st.ByPriority, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (PriorityDepth, error) {
			var p PriorityDepth
			err := r.Scan(&p.Priority, &p.Queued, &p.Running)
			return p, err
		})
		if err != nil {
			return fmt.Errorf("depth by priority: %w", err)
		}

		if err := tx.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE run_at <= now()), count(*) FILTER (WHERE run_at > now())
			  FROM jobs WHERE status = 'queued'`).Scan(&st.Ready, &st.Delayed); err != nil {
			return fmt.Errorf("ready/delayed: %w", err)
		}

		if err := tx.QueryRow(ctx, `
			SELECT count(*), COALESCE(sum(concurrency), 0), COALESCE(sum(active_jobs), 0)
			  FROM workers
			 WHERE status <> 'stopped' AND last_heartbeat_at > now() - $1::bigint * interval '1 microsecond'`,
			micros(staleAfter)).Scan(&st.ActiveWorkers, &st.TotalConcurrency, &st.ActiveJobs); err != nil {
			return fmt.Errorf("workers: %w", err)
		}

		if err := tx.QueryRow(ctx, `
			SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms),
			       percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
			  FROM job_attempts
			 WHERE finished_at > now() - $1::bigint * interval '1 microsecond' AND duration_ms IS NOT NULL`,
			micros(window)).Scan(&st.P50LatencyMS, &st.P95LatencyMS); err != nil {
			return fmt.Errorf("latency percentiles: %w", err)
		}

		var dead, terminal int64
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE status = 'dead-lettered'), count(*)
			  FROM jobs
			 WHERE finished_at > now() - $1::bigint * interval '1 microsecond'
			   AND status IN ('succeeded', 'failed', 'dead-lettered')`,
			micros(window)).Scan(&dead, &terminal); err != nil {
			return fmt.Errorf("dead letter rate: %w", err)
		}
		if terminal > 0 {
			st.DeadLetterRate = float64(dead) / float64(terminal)
		}

		if err := tx.QueryRow(ctx, "SELECT count(*) FROM dead_letter_jobs").Scan(&st.DeadLetterCount); err != nil {
			return fmt.Errorf("dead letter count: %w", err)
		}

		rows, err = tx.Query(ctx, `
			SELECT status, count(*) FROM job_attempts
			 WHERE finished_at > now() - $1::bigint * interval '1 microsecond'
			 GROUP BY status`, micros(window))
		if err != nil {
			return fmt.Errorf("attempts in window: %w", err)
		}
		attempts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (statusCount, error) {
			var c statusCount
			err := r.Scan(&c.status, &c.n)
			return c, err
		})
		if err != nil {
			return fmt.Errorf("attempts in window: %w", err)
		}
		for _, c := range attempts {
			st.AttemptsInWindow[domain.AttemptStatus(c.status)] = c.n
		}
		return nil
	})
	if err != nil {
		return QueueStats{}, err
	}
	if st.TotalConcurrency > 0 {
		st.WorkerUtilization = float64(st.ActiveJobs) / float64(st.TotalConcurrency)
	}
	return st, nil
}
