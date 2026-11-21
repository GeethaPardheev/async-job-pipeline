// Package dataplane runs job execution: poller, executor, retries, lease reaper, cron ticks and
// heartbeats (SPEC §10).
package dataplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/dataplane/handlers"
	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/google/uuid"
)

const (
	dbWriteTimeout    = 5 * time.Second
	cronBatch         = 20
	workerRetention   = time.Hour
	finalHeartbeatTTL = 5 * time.Second
)

// Config holds worker-process settings.
type Config struct {
	WorkerID          string
	Hostname          string
	Version           string
	Concurrency       int
	ClaimBatchSize    int
	PollInterval      time.Duration
	LeaseGrace        time.Duration
	ReaperInterval    time.Duration
	ReaperBatch       int
	HeartbeatInterval time.Duration
	CronTickInterval  time.Duration
	ShutdownTimeout   time.Duration
	Backoff           Backoff
}

// Worker owns the data-plane loops for one process.
type Worker struct {
	cfg      Config
	store    *store.Store
	registry *handlers.Registry
	log      *slog.Logger

	active   atomic.Int64
	draining atomic.Bool
	inflight sync.WaitGroup
}

// New builds a worker.
func New(cfg Config, st *store.Store, reg *handlers.Registry, log *slog.Logger) *Worker {
	return &Worker{cfg: cfg, store: st, registry: reg, log: log}
}

// ActiveJobs reports the number of attempts currently executing.
func (w *Worker) ActiveJobs() int { return int(w.active.Load()) }

// Run registers the worker, starts all loops, and blocks until ctx is cancelled. Then it stops
// claiming, reports draining, waits for in-flight attempts up to ShutdownTimeout, cancels any
// still running (their outcomes are persisted as transient failures), reports stopped, and returns.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.heartbeat(ctx, store.WorkerRunning); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	w.log.Info("worker started", "concurrency", w.cfg.Concurrency, "claim_batch", w.cfg.ClaimBatchSize)

	// Handlers run under execCtx, detached from the signal ctx so SIGTERM does not abort them;
	// it is cancelled only when the shutdown grace period runs out.
	execCtx, cancelExec := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelExec()
	hbCtx, stopHeartbeat := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHeartbeat()

	var loops, hb sync.WaitGroup
	loops.Add(3)
	go func() { defer loops.Done(); w.pollLoop(ctx, execCtx) }()
	go func() { defer loops.Done(); w.reaperLoop(ctx) }()
	go func() { defer loops.Done(); w.cronLoop(ctx) }()
	hb.Add(1)
	go func() { defer hb.Done(); w.heartbeatLoop(hbCtx) }()

	<-ctx.Done()
	w.draining.Store(true)
	w.log.Info("worker draining", "active_jobs", w.ActiveJobs(), "grace", w.cfg.ShutdownTimeout.String())
	if err := w.heartbeat(hbCtx, store.WorkerDraining); err != nil {
		w.log.Warn("draining heartbeat failed", "err", err)
	}
	loops.Wait()

	drained := make(chan struct{})
	go func() { w.inflight.Wait(); close(drained) }()
	grace := time.NewTimer(w.cfg.ShutdownTimeout)
	defer grace.Stop()
	select {
	case <-drained:
		w.log.Info("in-flight jobs finished")
	case <-grace.C:
		w.log.Warn("shutdown grace period elapsed; cancelling in-flight jobs", "active_jobs", w.ActiveJobs())
		cancelExec()
		<-drained
	}

	stopHeartbeat()
	hb.Wait()
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalHeartbeatTTL)
	defer cancel()
	if err := w.heartbeat(fctx, store.WorkerStopped); err != nil {
		w.log.Warn("final heartbeat failed", "err", err)
	}
	w.log.Info("worker stopped")
	return nil
}

func (w *Worker) heartbeat(ctx context.Context, status string) error {
	return w.store.UpsertWorker(ctx, store.Heartbeat{
		WorkerID: w.cfg.WorkerID, Hostname: w.cfg.Hostname, Concurrency: w.cfg.Concurrency,
		ActiveJobs: w.ActiveJobs(), Status: status, Version: w.cfg.Version,
	})
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		status := store.WorkerRunning
		if w.draining.Load() {
			status = store.WorkerDraining
		}
		if err := w.heartbeat(ctx, status); err != nil && ctx.Err() == nil {
			w.log.Warn("heartbeat failed", "err", err)
		}
	}
}

// sleepCtx waits d or until ctx ends; reports false if ctx ended.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (w *Worker) pollLoop(ctx, execCtx context.Context) {
	sem := make(chan struct{}, w.cfg.Concurrency)
	release := func(n int) {
		for i := 0; i < n; i++ {
			<-sem
		}
	}
	for {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		slots := 1
	fill:
		for slots < w.cfg.ClaimBatchSize {
			select {
			case sem <- struct{}{}:
				slots++
			default:
				break fill
			}
		}

		// A claim that reached the DB must finish so its rows are executed, not stranded until
		// lease expiry; hence WithoutCancel plus the store's own query timeout.
		jobs, err := w.store.ClaimJobs(context.WithoutCancel(ctx), w.cfg.WorkerID, slots, w.cfg.LeaseGrace)
		if err != nil {
			release(slots)
			w.log.Error("claim failed", "err", err)
			if !sleepCtx(ctx, jitter(w.cfg.PollInterval)) {
				return
			}
			continue
		}
		release(slots - len(jobs))
		for _, j := range jobs {
			w.inflight.Add(1)
			w.active.Add(1)
			go func(j domain.Job) {
				defer func() {
					w.active.Add(-1)
					<-sem
					w.inflight.Done()
				}()
				w.process(execCtx, j)
			}(j)
		}
		if len(jobs) < slots && !sleepCtx(ctx, jitter(w.cfg.PollInterval)) {
			return
		}
	}
}

func (w *Worker) process(execCtx context.Context, j domain.Job) {
	log := w.log.With("job_id", j.ID, "type", j.Type, "attempt", j.AttemptCount)
	var res ExecResult
	if h, ok := w.registry.Get(j.Type); ok {
		res = Execute(execCtx, log, h, handlers.Job{
			ID: j.ID, Type: j.Type, Payload: j.Payload, Attempt: j.AttemptCount,
			MaxRetries: j.MaxRetries, TimeoutSeconds: j.TimeoutSeconds,
		}, time.Duration(j.TimeoutSeconds)*time.Second)
	} else {
		res = ExecResult{
			Err:     domain.Permanent(fmt.Errorf("no handler registered for type %q", j.Type)),
			Outcome: domain.OutcomePermanent, AttemptStatus: domain.AttemptFailed,
		}
	}

	fence := store.Fence{JobID: j.ID, Attempt: j.AttemptCount, WorkerID: w.cfg.WorkerID}
	out := store.AttemptOutcome{Status: res.AttemptStatus, Error: res.ErrorMessage(), Duration: res.Duration}
	next := domain.NextStatus(res.Outcome, j.AttemptCount, j.MaxRetries)

	wctx, cancel := context.WithTimeout(context.WithoutCancel(execCtx), dbWriteTimeout)
	defer cancel()
	var err error
	var backoff time.Duration
	switch next {
	case domain.StatusSucceeded:
		err = w.store.CompleteJob(wctx, fence, res.Output, res.Duration)
	case domain.StatusFailed:
		err = w.store.FailJob(wctx, fence, out)
	case domain.StatusQueued:
		backoff = w.cfg.Backoff.Delay(j.AttemptCount)
		err = w.store.RetryJob(wctx, fence, out, backoff)
	case domain.StatusDeadLettered:
		err = w.store.DeadLetterJob(wctx, fence, out)
	default:
		err = fmt.Errorf("unexpected next status %q", next)
	}

	switch {
	case errors.Is(err, store.ErrFenceLost):
		log.Warn("outcome discarded: fence lost (job was reclaimed)", "outcome", next, "err", err)
	case err != nil:
		log.Error("failed to persist outcome; lease reaper will recover the job", "outcome", next, "err", err)
	default:
		attrs := []any{"status", next, "attempt_status", res.AttemptStatus, "duration_ms", res.Duration.Milliseconds()}
		if res.Err != nil {
			attrs = append(attrs, "err", res.ErrorMessage())
		}
		if next == domain.StatusQueued {
			attrs = append(attrs, "backoff_ms", backoff.Milliseconds())
		}
		if next == domain.StatusDeadLettered {
			log.Warn("job dead-lettered: retries exhausted", attrs...)
		} else {
			log.Info("attempt finished", attrs...)
		}
	}
}

func (w *Worker) reaperLoop(ctx context.Context) {
	for sleepCtx(ctx, jitter(w.cfg.ReaperInterval)) {
		res, err := w.store.ReapExpiredLeases(ctx, w.cfg.ReaperBatch, w.cfg.Backoff.Delay)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Error("lease reaper failed", "err", err)
			}
			continue
		}
		if res.Requeued+res.DeadLettered > 0 {
			w.log.Warn("lease reaper recovered jobs", "requeued", res.Requeued, "dead_lettered", res.DeadLettered)
		}
		if n, err := w.store.PruneWorkers(ctx, workerRetention); err != nil {
			if ctx.Err() == nil {
				w.log.Warn("prune workers failed", "err", err)
			}
		} else if n > 0 {
			w.log.Info("pruned stale worker rows", "count", n)
		}
	}
}

func (w *Worker) cronLoop(ctx context.Context) {
	for sleepCtx(ctx, jitter(w.cfg.CronTickInterval)) {
		res, err := w.store.TickSchedules(ctx, cronBatch, uuid.NewV7)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Error("cron tick failed", "err", err)
			}
			continue
		}
		if res.Fired+res.Duplicates+res.Skipped > 0 {
			w.log.Info("cron tick", "fired", res.Fired, "duplicates", res.Duplicates, "skipped", res.Skipped)
		}
	}
}
