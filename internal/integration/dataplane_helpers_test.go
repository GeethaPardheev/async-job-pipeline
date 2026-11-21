//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/dataplane"
	"github.com/GeethaPardheev/async-job-pipeline/internal/dataplane/handlers"
	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/google/uuid"
)

func bg(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func fastWorkerConfig(id string) dataplane.Config {
	return dataplane.Config{
		WorkerID:          id,
		Hostname:          "test-host",
		Version:           "test",
		Concurrency:       4,
		ClaimBatchSize:    4,
		PollInterval:      20 * time.Millisecond,
		LeaseGrace:        time.Second,
		ReaperInterval:    50 * time.Millisecond,
		ReaperBatch:       100,
		HeartbeatInterval: 100 * time.Millisecond,
		CronTickInterval:  50 * time.Millisecond,
		ShutdownTimeout:   5 * time.Second,
		Backoff:           dataplane.Backoff{Base: 20 * time.Millisecond, Cap: 200 * time.Millisecond},
	}
}

func testRegistry(t *testing.T) *handlers.Registry {
	t.Helper()
	reg, err := handlers.Builtin(handlers.NewWebhookClient(), "jobq/test")
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return reg
}

// startWorker runs a worker until the returned stop func (also registered as cleanup) is called.
// stop returns Run's error and how long shutdown took.
func startWorker(t *testing.T, st *store.Store, cfg dataplane.Config) func() (error, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := dataplane.New(cfg, st, testRegistry(t), testLogger())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	var once sync.Once
	var runErr error
	var took time.Duration
	stop := func() (error, time.Duration) {
		once.Do(func() {
			start := time.Now()
			cancel()
			select {
			case runErr = <-done:
			case <-time.After(30 * time.Second):
				t.Errorf("worker %s did not stop within 30s", cfg.WorkerID)
			}
			took = time.Since(start)
		})
		return runErr, took
	}
	t.Cleanup(func() {
		if err, _ := stop(); err != nil {
			t.Errorf("worker %s Run: %v", cfg.WorkerID, err)
		}
	})
	return stop
}

type jobOpt func(*store.NewJob)

func withPriority(p int) jobOpt     { return func(n *store.NewJob) { n.Priority = p } }
func withMaxRetries(r int) jobOpt   { return func(n *store.NewJob) { n.MaxRetries = r } }
func withTimeout(s int) jobOpt      { return func(n *store.NewJob) { n.TimeoutSeconds = s } }
func withRunAt(at time.Time) jobOpt { return func(n *store.NewJob) { n.RunAt = &at } }
func withKey(k, hash string) jobOpt {
	return func(n *store.NewJob) { n.IdempotencyKey, n.RequestHash = &k, &hash }
}

func enqueue(t *testing.T, st *store.Store, typ, payload string, opts ...jobOpt) domain.Job {
	t.Helper()
	nj := store.NewJob{
		ID: uuid.Must(uuid.NewV7()), Type: typ, Payload: json.RawMessage(payload),
		MaxRetries: domain.DefaultMaxRetries, TimeoutSeconds: domain.DefaultTimeoutSeconds,
	}
	for _, o := range opts {
		o(&nj)
	}
	res, err := st.EnqueueJob(bg(t), nj)
	if err != nil {
		t.Fatalf("enqueue %s: %v", typ, err)
	}
	return res.Job
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func getJob(t *testing.T, st *store.Store, id uuid.UUID) domain.Job {
	t.Helper()
	j, err := st.GetJob(bg(t), id)
	if err != nil {
		t.Fatalf("get job %s: %v", id, err)
	}
	return j
}

func waitStatus(t *testing.T, st *store.Store, id uuid.UUID, want domain.Status, timeout time.Duration) domain.Job {
	t.Helper()
	var j domain.Job
	eventually(t, timeout, "job "+id.String()+" to reach "+string(want), func() bool {
		j = getJob(t, st, id)
		return j.Status == want
	})
	return j
}

// attemptsAsc returns all attempts oldest first.
func attemptsAsc(t *testing.T, st *store.Store, id uuid.UUID) []domain.Attempt {
	t.Helper()
	as, err := st.ListAttempts(bg(t), id, 100)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	for i, j := 0, len(as)-1; i < j; i, j = i+1, j-1 {
		as[i], as[j] = as[j], as[i]
	}
	return as
}

func attemptStatuses(as []domain.Attempt) []domain.AttemptStatus {
	out := make([]domain.AttemptStatus, len(as))
	for i, a := range as {
		out[i] = a.Status
	}
	return out
}

func expireLease(t *testing.T, st *store.Store, id uuid.UUID) {
	t.Helper()
	tag, err := st.Pool().Exec(bg(t), "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1 AND status = 'running'", id)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("expire lease: rows=%d err=%v", tag.RowsAffected(), err)
	}
}

func setDraining(t *testing.T, st *store.Store, draining bool) {
	t.Helper()
	if _, err := st.Pool().Exec(bg(t), "UPDATE queue_state SET draining = $1, updated_at = now() WHERE id", draining); err != nil {
		t.Fatalf("set draining: %v", err)
	}
}

func countRows(t *testing.T, st *store.Store, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := st.Pool().QueryRow(bg(t), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func noBackoff(int) time.Duration { return 0 }
