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

// Schedule mirrors a row of the schedules table.
type Schedule struct {
	ID             uuid.UUID
	Name           *string
	CronExpr       string
	Type           string
	Payload        json.RawMessage
	Priority       int
	MaxRetries     int
	TimeoutSeconds int
	Enabled        bool
	NextRunAt      time.Time
	LastRunAt      *time.Time
	LastJobID      *uuid.UUID
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

const scheduleCols = `id, name, cron_expr, type, payload, priority, max_retries, timeout_seconds, enabled,
	next_run_at, last_run_at, last_job_id, created_at, updated_at`

func scanSchedule(row pgx.Row) (Schedule, error) {
	var sc Schedule
	err := row.Scan(&sc.ID, &sc.Name, &sc.CronExpr, &sc.Type, &sc.Payload, &sc.Priority, &sc.MaxRetries,
		&sc.TimeoutSeconds, &sc.Enabled, &sc.NextRunAt, &sc.LastRunAt, &sc.LastJobID, &sc.CreatedAt, &sc.UpdatedAt)
	return sc, err
}

// NewSchedule is the input to CreateSchedule. The cron expression must already be validated.
type NewSchedule struct {
	ID             uuid.UUID
	Name           *string
	CronExpr       string
	Type           string
	Payload        json.RawMessage
	Priority       int
	MaxRetries     int
	TimeoutSeconds int
	Enabled        bool
}

// CreateSchedule inserts a schedule whose first next_run_at is computed from Postgres now().
func (s *Store) CreateSchedule(ctx context.Context, ns NewSchedule) (Schedule, error) {
	sched, err := domain.ParseCron(ns.CronExpr)
	if err != nil {
		return Schedule{}, err
	}
	payload := ns.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	var out Schedule
	err = s.withTx(ctx, func(tx pgx.Tx) error {
		var now time.Time
		if err := tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
			return fmt.Errorf("read db clock: %w", err)
		}
		sc, err := scanSchedule(tx.QueryRow(ctx, `
			INSERT INTO schedules (id, name, cron_expr, type, payload, priority, max_retries, timeout_seconds, enabled, next_run_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			RETURNING `+scheduleCols,
			ns.ID, ns.Name, ns.CronExpr, ns.Type, payload, ns.Priority, ns.MaxRetries, ns.TimeoutSeconds, ns.Enabled,
			domain.NextCronRun(sched, now)))
		if err != nil {
			return fmt.Errorf("insert schedule: %w", err)
		}
		out = sc
		return nil
	})
	if err != nil {
		return Schedule{}, err
	}
	return out, nil
}

// CronKey is the deterministic idempotency key for one schedule tick.
func CronKey(scheduleID uuid.UUID, scheduledAt time.Time) string {
	return fmt.Sprintf("cron:%s:%d", scheduleID, scheduledAt.Unix())
}

// TickResult counts what one cron pass did.
type TickResult struct {
	Fired      int // jobs inserted
	Duplicates int // ticks whose key already existed
	Skipped    int // ticks advanced without enqueue (draining or invalid expression)
}

// TickSchedules claims due schedules with FOR UPDATE SKIP LOCKED, enqueues one job per due tick
// under key cron:<id>:<scheduled_unix>, and advances next_run_at to the first activation after
// Postgres now(), all in one tx (SPEC §6.7). Missed ticks are not backfilled.
func (s *Store) TickSchedules(ctx context.Context, batch int, newID func() (uuid.UUID, error)) (TickResult, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	var res TickResult
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		res = TickResult{}
		var now time.Time
		var draining bool
		if err := tx.QueryRow(ctx, "SELECT now(), (SELECT draining FROM queue_state WHERE id)").Scan(&now, &draining); err != nil {
			return fmt.Errorf("read db clock: %w", err)
		}
		rows, err := tx.Query(ctx, `
			SELECT `+scheduleCols+` FROM schedules
			 WHERE enabled AND next_run_at <= now()
			 ORDER BY next_run_at LIMIT $1
			 FOR UPDATE SKIP LOCKED`, batch)
		if err != nil {
			return fmt.Errorf("select due schedules: %w", err)
		}
		due, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Schedule, error) { return scanSchedule(r) })
		if err != nil {
			return fmt.Errorf("scan due schedules: %w", err)
		}

		for _, sc := range due {
			parsed, perr := domain.ParseCron(sc.CronExpr)
			if perr != nil {
				s.log.Error("disabling schedule with invalid cron expression", "schedule_id", sc.ID, "err", perr)
				if _, err := tx.Exec(ctx, "UPDATE schedules SET enabled = false, updated_at = now() WHERE id = $1", sc.ID); err != nil {
					return fmt.Errorf("disable schedule %s: %w", sc.ID, err)
				}
				res.Skipped++
				continue
			}

			var jobID *uuid.UUID
			if draining {
				res.Skipped++
			} else {
				id, err := newID()
				if err != nil {
					return fmt.Errorf("generate job id: %w", err)
				}
				var inserted uuid.UUID
				err = tx.QueryRow(ctx, `
					INSERT INTO jobs (id, type, payload, priority, max_retries, timeout_seconds, run_at, idempotency_key, schedule_id)
					VALUES ($1, $2, $3, $4, $5, $6, now(), $7, $8)
					ON CONFLICT (idempotency_key) DO NOTHING
					RETURNING id`,
					id, sc.Type, sc.Payload, sc.Priority, sc.MaxRetries, sc.TimeoutSeconds, CronKey(sc.ID, sc.NextRunAt), sc.ID).Scan(&inserted)
				switch {
				case err == nil:
					jobID = &inserted
					res.Fired++
				case errors.Is(err, pgx.ErrNoRows):
					res.Duplicates++
				default:
					return fmt.Errorf("enqueue tick for schedule %s: %w", sc.ID, err)
				}
			}

			if _, err := tx.Exec(ctx, `
				UPDATE schedules SET last_run_at = next_run_at, next_run_at = $2,
				                     last_job_id = COALESCE($3, last_job_id), updated_at = now()
				 WHERE id = $1`,
				sc.ID, domain.NextCronRun(parsed, now), jobID); err != nil {
				return fmt.Errorf("advance schedule %s: %w", sc.ID, err)
			}
		}
		return nil
	})
	if err != nil {
		return TickResult{}, err
	}
	return res, nil
}
