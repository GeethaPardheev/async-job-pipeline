// Package dataplane runs job execution: poller, executor, retries, lease reaper, cron ticks and
// heartbeats (SPEC §10).
package dataplane

import (
	"context"
	"log/slog"
)

// Config holds worker-process settings.
type Config struct {
	WorkerID    string
	Concurrency int
	Version     string
}

// Worker owns the data-plane loops for one process.
type Worker struct {
	cfg Config
	log *slog.Logger
}

// New builds a worker.
func New(cfg Config, log *slog.Logger) *Worker {
	return &Worker{cfg: cfg, log: log}
}

// Run blocks until ctx is cancelled. The poller, executor, reaper, cron and heartbeat loops are
// added in Step 2.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker started", "concurrency", w.cfg.Concurrency)
	<-ctx.Done()
	w.log.Info("worker stopped")
	return nil
}
