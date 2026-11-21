package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Fence is the ownership token for one attempt. Every worker write about a job must match it.
type Fence struct {
	JobID    uuid.UUID
	Attempt  int
	WorkerID string
}

// AttemptOutcome describes how an attempt ended, for the job_attempts row.
type AttemptOutcome struct {
	Status   domain.AttemptStatus
	Error    string
	Duration time.Duration // negative = unknown (stored as NULL, excluded from latency stats)
}

func micros(d time.Duration) int64 { return d.Microseconds() }

func durationMS(d time.Duration) any {
	if d < 0 {
		return nil
	}
	return d.Milliseconds()
}

// ClaimJobs atomically claims up to limit runnable jobs for workerID (SPEC §6.1): queued, due,
// queue not draining, ordered priority DESC, run_at, created_at. Each claim increments
// attempt_count, sets the lease from Postgres now(), and inserts a job_attempts row.
func (s *Store) ClaimJobs(ctx context.Context, workerID string, limit int, leaseGrace time.Duration) ([]domain.Job, error) {
	if limit <= 0 {
		return nil, nil
	}
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		WITH cand AS (
		  SELECT j.id FROM jobs j
		   WHERE j.status = 'queued' AND j.run_at <= now()
		     AND NOT (SELECT draining FROM queue_state WHERE id)
		   ORDER BY j.priority DESC, j.run_at, j.created_at, j.id
		   LIMIT $1
		   FOR UPDATE OF j SKIP LOCKED
		), claimed AS (
		  UPDATE jobs j
		     SET status = 'running', attempt_count = j.attempt_count + 1, worker_id = $2,
		         lease_expires_at = now() + make_interval(secs => j.timeout_seconds) + $3::bigint * interval '1 microsecond',
		         started_at = COALESCE(j.started_at, now()), updated_at = now()
		    FROM cand WHERE j.id = cand.id
		  RETURNING j.*
		), att AS (
		  INSERT INTO job_attempts (job_id, attempt_number, worker_id, status, started_at)
		  SELECT id, attempt_count, $2, 'running', now() FROM claimed
		)
		SELECT `+jobCols+` FROM claimed ORDER BY priority DESC, run_at, created_at, id`,
		limit, workerID, micros(leaseGrace))
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Job, error) { return scanJob(r) })
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	return jobs, nil
}

// CompleteJob is T3: running → succeeded.
func (s *Store) CompleteJob(ctx context.Context, f Fence, result json.RawMessage, dur time.Duration) error {
	return s.fencedTransition(ctx, f, AttemptOutcome{Status: domain.AttemptSucceeded, Duration: dur}, "succeeded",
		func(ctx context.Context, tx pgx.Tx) (int64, error) {
			tag, err := tx.Exec(ctx, `
				UPDATE jobs SET status = 'succeeded', result = $4, last_error = NULL, lease_expires_at = NULL,
				                finished_at = now(), updated_at = now()
				 WHERE id = $1 AND status = 'running' AND attempt_count = $2 AND worker_id = $3`,
				f.JobID, f.Attempt, f.WorkerID, jsonParam(result))
			return tag.RowsAffected(), err
		})
}

// FailJob is T4: running → failed (permanent error, no retry).
func (s *Store) FailJob(ctx context.Context, f Fence, out AttemptOutcome) error {
	return s.fencedTransition(ctx, f, out, "failed", func(ctx context.Context, tx pgx.Tx) (int64, error) {
		return failTx(ctx, tx, f, out.Error)
	})
}

// RetryJob is T5: running → queued with run_at = now() + backoff. The SQL guard
// attempt_count <= max_retries rejects a retry once the budget is spent.
func (s *Store) RetryJob(ctx context.Context, f Fence, out AttemptOutcome, backoff time.Duration) error {
	return s.fencedTransition(ctx, f, out, "retry", func(ctx context.Context, tx pgx.Tx) (int64, error) {
		return retryTx(ctx, tx, f, out.Error, backoff)
	})
}

// DeadLetterJob is T6: running → dead-lettered plus the dead_letter_jobs snapshot, in one statement.
func (s *Store) DeadLetterJob(ctx context.Context, f Fence, out AttemptOutcome) error {
	return s.fencedTransition(ctx, f, out, "dead-letter", func(ctx context.Context, tx pgx.Tx) (int64, error) {
		return deadLetterTx(ctx, tx, f, out.Error)
	})
}

func failTx(ctx context.Context, q querier, f Fence, errMsg string) (int64, error) {
	tag, err := q.Exec(ctx, `
		UPDATE jobs SET status = 'failed', lease_expires_at = NULL, last_error = $4,
		                finished_at = now(), updated_at = now()
		 WHERE id = $1 AND status = 'running' AND attempt_count = $2 AND worker_id = $3`,
		f.JobID, f.Attempt, f.WorkerID, truncateErr(errMsg))
	return tag.RowsAffected(), err
}

func retryTx(ctx context.Context, q querier, f Fence, errMsg string, backoff time.Duration) (int64, error) {
	tag, err := q.Exec(ctx, `
		UPDATE jobs SET status = 'queued', run_at = now() + $4::bigint * interval '1 microsecond',
		                worker_id = NULL, lease_expires_at = NULL, last_error = $5, updated_at = now()
		 WHERE id = $1 AND status = 'running' AND attempt_count = $2 AND worker_id = $3
		   AND attempt_count <= max_retries`,
		f.JobID, f.Attempt, f.WorkerID, micros(backoff), truncateErr(errMsg))
	return tag.RowsAffected(), err
}

func deadLetterTx(ctx context.Context, q querier, f Fence, errMsg string) (int64, error) {
	tag, err := q.Exec(ctx, `
		WITH upd AS (
		  UPDATE jobs SET status = 'dead-lettered', lease_expires_at = NULL, last_error = $4,
		                  finished_at = now(), updated_at = now()
		   WHERE id = $1 AND status = 'running' AND attempt_count = $2 AND worker_id = $3
		     AND attempt_count > max_retries
		  RETURNING *
		)
		INSERT INTO dead_letter_jobs (job_id, type, payload, priority, max_retries, timeout_seconds,
		                              attempt_count, last_error, snapshot)
		SELECT id, type, payload, priority, max_retries, timeout_seconds, attempt_count, last_error, to_jsonb(upd)
		  FROM upd`,
		f.JobID, f.Attempt, f.WorkerID, truncateErr(errMsg))
	return tag.RowsAffected(), err
}

func finishAttemptTx(ctx context.Context, q querier, f Fence, out AttemptOutcome) error {
	var errParam any
	if out.Error != "" {
		errParam = truncateErr(out.Error)
	}
	tag, err := q.Exec(ctx, `
		UPDATE job_attempts SET status = $4, error = $5, finished_at = now(), duration_ms = $6
		 WHERE job_id = $1 AND attempt_number = $2 AND worker_id = $3`,
		f.JobID, f.Attempt, f.WorkerID, string(out.Status), errParam, durationMS(out.Duration))
	if err != nil {
		return fmt.Errorf("update attempt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("update attempt: job %s attempt %d by %s: %d rows, want 1", f.JobID, f.Attempt, f.WorkerID, tag.RowsAffected())
	}
	return nil
}

// fencedTransition runs jobUpdate and the matching attempt update in one tx. If the fence does
// not match (0 rows), the job is left untouched, the attempt row is marked fenced (committed as
// evidence of a duplicate execution), and ErrFenceLost is returned.
func (s *Store) fencedTransition(ctx context.Context, f Fence, out AttemptOutcome, label string,
	jobUpdate func(context.Context, pgx.Tx) (int64, error)) error {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	fenced := false
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		n, err := jobUpdate(ctx, tx)
		if err != nil {
			return fmt.Errorf("%s job: %w", label, err)
		}
		if n == 0 {
			fenced = true
			_, err := tx.Exec(ctx, `
				UPDATE job_attempts SET status = 'fenced', error = $4, finished_at = now(), duration_ms = $5
				 WHERE job_id = $1 AND attempt_number = $2 AND worker_id = $3 AND status IN ('running', 'lease_expired')`,
				f.JobID, f.Attempt, f.WorkerID,
				truncateErr(fmt.Sprintf("stale attempt rejected; discarded outcome %s: %s", out.Status, out.Error)),
				durationMS(out.Duration))
			if err != nil {
				return fmt.Errorf("mark attempt fenced: %w", err)
			}
			return nil
		}
		return finishAttemptTx(ctx, tx, f, out)
	})
	if err != nil {
		return err
	}
	if fenced {
		return fmt.Errorf("%s job %s attempt %d by %s: %w", label, f.JobID, f.Attempt, f.WorkerID, ErrFenceLost)
	}
	return nil
}

// ReapResult counts what one reaper pass did.
type ReapResult struct {
	Requeued     int
	DeadLettered int
}

// ReapExpiredLeases recovers running jobs whose lease expired (crashed or partitioned workers),
// using each locked row's own (attempt_count, worker_id) as the fence (SPEC §6.6).
func (s *Store) ReapExpiredLeases(ctx context.Context, batch int, backoff func(attempt int) time.Duration) (ReapResult, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	var res ReapResult
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		res = ReapResult{}
		rows, err := tx.Query(ctx, `
			SELECT id, attempt_count, max_retries, worker_id FROM jobs
			 WHERE status = 'running' AND lease_expires_at < now()
			 ORDER BY lease_expires_at LIMIT $1
			 FOR UPDATE SKIP LOCKED`, batch)
		if err != nil {
			return fmt.Errorf("select expired leases: %w", err)
		}
		type stuck struct {
			fence      Fence
			maxRetries int
		}
		stuckJobs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (stuck, error) {
			var st stuck
			err := r.Scan(&st.fence.JobID, &st.fence.Attempt, &st.maxRetries, &st.fence.WorkerID)
			return st, err
		})
		if err != nil {
			return fmt.Errorf("scan expired leases: %w", err)
		}

		for _, st := range stuckJobs {
			msg := fmt.Sprintf("lease expired (worker %s presumed dead)", st.fence.WorkerID)
			next := domain.NextStatus(domain.OutcomeTransient, st.fence.Attempt, st.maxRetries)
			var n int64
			if next == domain.StatusQueued {
				n, err = retryTx(ctx, tx, st.fence, msg, backoff(st.fence.Attempt))
			} else {
				n, err = deadLetterTx(ctx, tx, st.fence, msg)
			}
			if err != nil {
				return fmt.Errorf("reap job %s: %w", st.fence.JobID, err)
			}
			if n != 1 {
				return fmt.Errorf("reap job %s: locked row not updated (%d rows)", st.fence.JobID, n)
			}
			if err := finishAttemptTx(ctx, tx, st.fence, AttemptOutcome{Status: domain.AttemptLeaseExpired, Error: msg, Duration: -1}); err != nil {
				return fmt.Errorf("reap job %s: %w", st.fence.JobID, err)
			}
			if next == domain.StatusQueued {
				res.Requeued++
			} else {
				res.DeadLettered++
			}
		}
		return nil
	})
	if err != nil {
		return ReapResult{}, err
	}
	return res, nil
}
