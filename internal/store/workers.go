package store

import (
	"context"
	"fmt"
	"time"
)

// Worker statuses (must match the workers.status CHECK constraint).
const (
	WorkerRunning  = "running"
	WorkerDraining = "draining"
	WorkerStopped  = "stopped"
)

// Heartbeat is one worker's self-report.
type Heartbeat struct {
	WorkerID    string
	Hostname    string
	Concurrency int
	ActiveJobs  int
	Status      string
	Version     string
}

// UpsertWorker records a heartbeat with last_heartbeat_at = now() (Postgres clock).
func (s *Store) UpsertWorker(ctx context.Context, hb Heartbeat) error {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (worker_id, hostname, concurrency, active_jobs, status, version, started_at, last_heartbeat_at)
		VALUES ($1, $2, $3, $4, $5, $6, now(), now())
		ON CONFLICT (worker_id) DO UPDATE
		   SET hostname = EXCLUDED.hostname, concurrency = EXCLUDED.concurrency, active_jobs = EXCLUDED.active_jobs,
		       status = EXCLUDED.status, version = EXCLUDED.version, last_heartbeat_at = now()`,
		hb.WorkerID, hb.Hostname, hb.Concurrency, hb.ActiveJobs, hb.Status, hb.Version)
	if err != nil {
		return fmt.Errorf("upsert worker heartbeat: %w", err)
	}
	return nil
}

// PruneWorkers deletes worker rows whose last heartbeat is older than olderThan.
func (s *Store) PruneWorkers(ctx context.Context, olderThan time.Duration) (int64, error) {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		"DELETE FROM workers WHERE last_heartbeat_at < now() - $1::bigint * interval '1 microsecond'", micros(olderThan))
	if err != nil {
		return 0, fmt.Errorf("prune workers: %w", err)
	}
	return tag.RowsAffected(), nil
}
