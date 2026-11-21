//go:build integration

package integration

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/google/uuid"
)

func TestHappyPath(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	stop := startWorker(t, st, fastWorkerConfig("w-happy"))

	j := enqueue(t, st, "echo", `{"hello":"world"}`)
	if j.Status != domain.StatusQueued || j.AttemptCount != 0 {
		t.Fatalf("enqueued job = %+v", j)
	}
	done := waitStatus(t, st, j.ID, domain.StatusSucceeded, 10*time.Second)
	var got map[string]string
	if err := json.Unmarshal(done.Result, &got); err != nil || got["hello"] != "world" {
		t.Fatalf("result = %s (%v)", done.Result, err)
	}
	if done.AttemptCount != 1 || done.FinishedAt == nil || done.LeaseExpiresAt != nil || done.LastError != nil {
		t.Fatalf("succeeded job fields wrong: %+v", done)
	}
	as := attemptsAsc(t, st, j.ID)
	if len(as) != 1 || as[0].Status != domain.AttemptSucceeded || as[0].WorkerID != "w-happy" ||
		as[0].DurationMS == nil || as[0].FinishedAt == nil {
		t.Fatalf("attempts = %+v", as)
	}

	eventually(t, 5*time.Second, "worker heartbeat row", func() bool {
		return countRows(t, st, "SELECT count(*) FROM workers WHERE worker_id = 'w-happy' AND status = 'running' AND concurrency = 4") == 1
	})
	if err, _ := stop(); err != nil {
		t.Fatalf("worker stop: %v", err)
	}
	if n := countRows(t, st, "SELECT count(*) FROM workers WHERE worker_id = 'w-happy' AND status = 'stopped' AND active_jobs = 0"); n != 1 {
		t.Fatalf("worker not marked stopped")
	}
}

func TestPriorityOrdering(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	sameRunAt := time.Now().Add(-time.Minute)
	low := enqueue(t, st, "echo", `{}`, withPriority(0))
	highA := enqueue(t, st, "echo", `{}`, withPriority(10), withRunAt(sameRunAt))
	mid := enqueue(t, st, "echo", `{}`, withPriority(5))
	highB := enqueue(t, st, "echo", `{}`, withPriority(10), withRunAt(sameRunAt)) // same run_at: created_at breaks the tie
	highOld := enqueue(t, st, "echo", `{}`, withPriority(10), withRunAt(sameRunAt.Add(-time.Hour)))

	want := []uuid.UUID{highOld.ID, highA.ID, highB.ID, mid.ID, low.ID}
	var got []uuid.UUID
	for range want {
		jobs, err := st.ClaimJobs(bg(t), "w-prio", 1, time.Second)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("claim: %v (%d jobs)", err, len(jobs))
		}
		got = append(got, jobs[0].ID)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("claim order = %v, want %v", got, want)
	}
	if jobs, err := st.ClaimJobs(bg(t), "w-prio", 1, time.Second); err != nil || len(jobs) != 0 {
		t.Fatalf("expected empty queue, got %d jobs (%v)", len(jobs), err)
	}
}

func TestBatchClaimReturnsPriorityOrder(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	for _, p := range []int{1, 7, 3, 9} {
		enqueue(t, st, "echo", `{}`, withPriority(p))
	}
	jobs, err := st.ClaimJobs(bg(t), "w-batch", 10, time.Second)
	if err != nil || len(jobs) != 4 {
		t.Fatalf("claim: %v (%d)", err, len(jobs))
	}
	for i, wantP := range []int{9, 7, 3, 1} {
		if jobs[i].Priority != wantP || jobs[i].Status != domain.StatusRunning || jobs[i].AttemptCount != 1 ||
			jobs[i].WorkerID == nil || *jobs[i].WorkerID != "w-batch" || jobs[i].LeaseExpiresAt == nil {
			t.Fatalf("claimed[%d] = %+v", i, jobs[i])
		}
	}
	if n := countRows(t, st, "SELECT count(*) FROM job_attempts WHERE status = 'running' AND worker_id = 'w-batch'"); n != 4 {
		t.Fatalf("attempt rows = %d, want 4", n)
	}
}

func TestDelayedRunAt(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	runAt := time.Now().Add(1500 * time.Millisecond)
	j := enqueue(t, st, "echo", `{}`, withRunAt(runAt))

	if jobs, err := st.ClaimJobs(bg(t), "w-delay", 5, time.Second); err != nil || len(jobs) != 0 {
		t.Fatalf("delayed job claimed early: %d jobs, err %v", len(jobs), err)
	}
	stop := startWorker(t, st, fastWorkerConfig("w-delay"))
	defer stop()
	done := waitStatus(t, st, j.ID, domain.StatusSucceeded, 10*time.Second)
	if done.StartedAt == nil || done.StartedAt.Before(done.RunAt) {
		t.Fatalf("started_at %v before run_at %v", done.StartedAt, done.RunAt)
	}
}

func TestFlakyRetryToSuccess(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	cfg := fastWorkerConfig("w-flaky")
	startWorker(t, st, cfg)

	j := enqueue(t, st, "flaky", `{"fail_times":2}`, withMaxRetries(3))
	done := waitStatus(t, st, j.ID, domain.StatusSucceeded, 15*time.Second)
	if done.AttemptCount != 3 || string(done.Result) != `{"attempt": 3}` {
		t.Fatalf("job = attempts %d result %s", done.AttemptCount, done.Result)
	}
	as := attemptsAsc(t, st, j.ID)
	want := []domain.AttemptStatus{domain.AttemptFailed, domain.AttemptFailed, domain.AttemptSucceeded}
	if got := attemptStatuses(as); !reflect.DeepEqual(got, want) {
		t.Fatalf("attempt statuses = %v, want %v", got, want)
	}
	for i := 1; i < len(as); i++ {
		gap := as[i].StartedAt.Sub(*as[i-1].FinishedAt)
		if floor := cfg.Backoff.Floor(i); gap < floor {
			t.Fatalf("gap before attempt %d = %s < backoff floor %s", i+1, gap, floor)
		}
	}
}

func TestTimeoutToRetry(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	startWorker(t, st, fastWorkerConfig("w-timeout"))

	j := enqueue(t, st, "sleep", `{"duration_ms":10000}`, withTimeout(1), withMaxRetries(1))
	done := waitStatus(t, st, j.ID, domain.StatusDeadLettered, 15*time.Second)
	as := attemptsAsc(t, st, j.ID)
	want := []domain.AttemptStatus{domain.AttemptTimedOut, domain.AttemptTimedOut}
	if got := attemptStatuses(as); !reflect.DeepEqual(got, want) {
		t.Fatalf("attempt statuses = %v, want %v", got, want)
	}
	if done.LastError == nil || !strings.Contains(*done.LastError, "timed out") {
		t.Fatalf("last_error = %v", done.LastError)
	}
	for _, a := range as {
		if a.DurationMS == nil || *a.DurationMS < 1000 || *a.DurationMS > 5000 {
			t.Fatalf("timed-out attempt duration = %v, want ~1000ms", a.DurationMS)
		}
	}
}

func TestMaxRetriesToDLQ(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	startWorker(t, st, fastWorkerConfig("w-dlq"))

	j := enqueue(t, st, "always_fail", `{"x":1}`, withMaxRetries(2), withPriority(4))
	done := waitStatus(t, st, j.ID, domain.StatusDeadLettered, 15*time.Second)
	if done.AttemptCount != 3 || done.FinishedAt == nil {
		t.Fatalf("job = %+v", done)
	}
	if got := attemptStatuses(attemptsAsc(t, st, j.ID)); len(got) != 3 {
		t.Fatalf("attempts = %v, want 3", got)
	}

	var dlType, snapStatus string
	var dlAttempts, dlPriority int
	var payload json.RawMessage
	err := st.Pool().QueryRow(bg(t), `SELECT type, attempt_count, priority, payload, snapshot->>'status'
		FROM dead_letter_jobs WHERE job_id = $1`, j.ID).Scan(&dlType, &dlAttempts, &dlPriority, &payload, &snapStatus)
	if err != nil {
		t.Fatalf("dlq row: %v", err)
	}
	if dlType != "always_fail" || dlAttempts != 3 || dlPriority != 4 || snapStatus != "dead-lettered" || string(payload) != `{"x": 1}` {
		t.Fatalf("dlq row = type %s attempts %d prio %d status %s payload %s", dlType, dlAttempts, dlPriority, snapStatus, payload)
	}
	assertDLQInvariant(t, st)
}

func assertDLQInvariant(t *testing.T, st *store.Store) {
	t.Helper()
	a := countRows(t, st, "SELECT count(*) FROM jobs WHERE status = 'dead-lettered'")
	b := countRows(t, st, "SELECT count(*) FROM dead_letter_jobs")
	if a != b {
		t.Fatalf("I4 violated: %d dead-lettered jobs vs %d dead_letter_jobs rows", a, b)
	}
}

func TestPermanentErrorToFailed(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	startWorker(t, st, fastWorkerConfig("w-perm"))

	j := enqueue(t, st, "permanent_fail", `{}`, withMaxRetries(5))
	done := waitStatus(t, st, j.ID, domain.StatusFailed, 10*time.Second)
	if done.AttemptCount != 1 || done.LastError == nil || !strings.Contains(*done.LastError, "permanent") {
		t.Fatalf("job = %+v", done)
	}
	if n := countRows(t, st, "SELECT count(*) FROM dead_letter_jobs WHERE job_id = $1", j.ID); n != 0 {
		t.Fatalf("permanent failure must not dead-letter")
	}
	bad := enqueue(t, st, "sleep", `{"duration_ms":"soon"}`, withMaxRetries(5))
	if got := waitStatus(t, st, bad.ID, domain.StatusFailed, 10*time.Second); got.AttemptCount != 1 {
		t.Fatalf("invalid payload should fail on first attempt, got %d", got.AttemptCount)
	}
}

func TestZombieWorkerFencing(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	ctx := bg(t)
	j := enqueue(t, st, "echo", `{}`, withMaxRetries(3))

	claimedA, err := st.ClaimJobs(ctx, "w-zombie", 1, time.Second)
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("A claim: %v", err)
	}
	expireLease(t, st, j.ID)
	if res, err := st.ReapExpiredLeases(ctx, 10, noBackoff); err != nil || res.Requeued != 1 {
		t.Fatalf("reap: %+v %v", res, err)
	}
	claimedB, err := st.ClaimJobs(ctx, "w-live", 1, time.Second)
	if err != nil || len(claimedB) != 1 || claimedB[0].AttemptCount != 2 {
		t.Fatalf("B claim: %v %+v", err, claimedB)
	}

	stale := store.Fence{JobID: j.ID, Attempt: 1, WorkerID: "w-zombie"}
	out := store.AttemptOutcome{Status: domain.AttemptFailed, Error: "late", Duration: time.Millisecond}
	staleWrites := map[string]error{
		"complete":    st.CompleteJob(ctx, stale, json.RawMessage(`{"zombie":true}`), time.Millisecond),
		"fail":        st.FailJob(ctx, stale, out),
		"retry":       st.RetryJob(ctx, stale, out, 0),
		"dead-letter": st.DeadLetterJob(ctx, stale, out),
		"wrong-worker": st.CompleteJob(ctx, store.Fence{JobID: j.ID, Attempt: 2, WorkerID: "w-zombie"},
			nil, time.Millisecond),
	}
	for name, err := range staleWrites {
		if !errors.Is(err, store.ErrFenceLost) {
			t.Fatalf("stale %s: err = %v, want ErrFenceLost", name, err)
		}
	}

	mid := getJob(t, st, j.ID)
	if mid.Status != domain.StatusRunning || mid.AttemptCount != 2 || *mid.WorkerID != "w-live" || mid.Result != nil {
		t.Fatalf("zombie changed the job: %+v", mid)
	}
	if err := st.CompleteJob(ctx, store.Fence{JobID: j.ID, Attempt: 2, WorkerID: "w-live"}, json.RawMessage(`{"live":true}`), time.Millisecond); err != nil {
		t.Fatalf("live complete: %v", err)
	}
	final := getJob(t, st, j.ID)
	if final.Status != domain.StatusSucceeded || string(final.Result) != `{"live": true}` {
		t.Fatalf("final = %+v", final)
	}
	if err := st.CompleteJob(ctx, store.Fence{JobID: j.ID, Attempt: 2, WorkerID: "w-live"}, nil, 0); !errors.Is(err, store.ErrFenceLost) {
		t.Fatalf("write to terminal job: err = %v, want ErrFenceLost", err)
	}
	as := attemptsAsc(t, st, j.ID)
	want := []domain.AttemptStatus{domain.AttemptFenced, domain.AttemptSucceeded}
	if got := attemptStatuses(as); !reflect.DeepEqual(got, want) {
		t.Fatalf("attempt statuses = %v, want %v", got, want)
	}
}

func TestLeaseReaperRecoversStuckJob(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	j := enqueue(t, st, "echo", `{"n":1}`, withMaxRetries(1))
	if claimed, err := st.ClaimJobs(bg(t), "w-crashed", 1, time.Second); err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v", err)
	}
	expireLease(t, st, j.ID)

	startWorker(t, st, fastWorkerConfig("w-rescuer"))
	done := waitStatus(t, st, j.ID, domain.StatusSucceeded, 10*time.Second)
	if done.AttemptCount != 2 || *done.WorkerID != "w-rescuer" {
		t.Fatalf("job = %+v", done)
	}
	as := attemptsAsc(t, st, j.ID)
	if len(as) != 2 || as[0].Status != domain.AttemptLeaseExpired || as[0].WorkerID != "w-crashed" ||
		as[0].DurationMS != nil || as[1].Status != domain.AttemptSucceeded {
		t.Fatalf("attempts = %+v", as)
	}
}

func TestLeaseReaperDeadLettersWhenExhausted(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	j := enqueue(t, st, "echo", `{}`, withMaxRetries(0))
	if claimed, err := st.ClaimJobs(bg(t), "w-crashed", 1, time.Second); err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v", err)
	}
	if res, err := st.ReapExpiredLeases(bg(t), 10, noBackoff); err != nil || res.Requeued+res.DeadLettered != 0 {
		t.Fatalf("reaper touched a live lease: %+v %v", res, err)
	}
	expireLease(t, st, j.ID)
	res, err := st.ReapExpiredLeases(bg(t), 10, noBackoff)
	if err != nil || res.DeadLettered != 1 || res.Requeued != 0 {
		t.Fatalf("reap: %+v %v", res, err)
	}
	if got := getJob(t, st, j.ID); got.Status != domain.StatusDeadLettered {
		t.Fatalf("status = %s", got.Status)
	}
	assertDLQInvariant(t, st)
}

func TestConcurrentWorkersNoDoubleClaim(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	const jobs, workers = 300, 6
	ids := make([]uuid.UUID, jobs)
	for i := range ids {
		ids[i] = enqueue(t, st, "echo", fmt.Sprintf(`{"i":%d}`, i)).ID
	}
	for i := 0; i < workers; i++ {
		cfg := fastWorkerConfig(fmt.Sprintf("w-conc-%d", i))
		cfg.Concurrency, cfg.ClaimBatchSize = 8, 8
		startWorker(t, st, cfg)
	}
	eventually(t, 30*time.Second, "all jobs succeeded", func() bool {
		return countRows(t, st, "SELECT count(*) FROM jobs WHERE status = 'succeeded'") == jobs
	})
	if n := countRows(t, st, "SELECT count(*) FROM job_attempts"); n != jobs {
		t.Fatalf("I1 violated: %d attempt rows for %d jobs", n, jobs)
	}
	if n := countRows(t, st, "SELECT count(*) FROM jobs WHERE attempt_count <> 1"); n != 0 {
		t.Fatalf("I1 violated: %d jobs with attempt_count != 1", n)
	}
	if n := countRows(t, st, "SELECT count(*) FROM job_attempts WHERE status = 'fenced'"); n != 0 {
		t.Fatalf("%d fenced attempts", n)
	}
	if n := countRows(t, st, "SELECT count(DISTINCT worker_id) FROM job_attempts"); n < 2 {
		t.Fatalf("work not distributed: %d distinct workers", n)
	}
}

func TestCronTickFiresExactlyOnceAcrossWorkers(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	ctx := bg(t)
	sc, err := st.CreateSchedule(ctx, store.NewSchedule{
		ID: uuid.Must(uuid.NewV7()), CronExpr: "@every 1h", Type: "echo", Payload: json.RawMessage(`{"cron":true}`),
		MaxRetries: 1, TimeoutSeconds: 5, Enabled: true,
	})
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	var scheduledAt time.Time
	if err := st.Pool().QueryRow(ctx,
		"UPDATE schedules SET next_run_at = date_trunc('second', now()) - interval '1 second' WHERE id = $1 RETURNING next_run_at",
		sc.ID).Scan(&scheduledAt); err != nil {
		t.Fatalf("make schedule due: %v", err)
	}

	const tickers, rounds = 8, 10
	var mu sync.Mutex
	var total store.TickResult
	var wg sync.WaitGroup
	for i := 0; i < tickers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				res, err := st.TickSchedules(ctx, 20, uuid.NewV7)
				if err != nil {
					t.Errorf("tick: %v", err)
					return
				}
				mu.Lock()
				total.Fired += res.Fired
				total.Duplicates += res.Duplicates
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if total.Fired != 1 || total.Duplicates != 0 {
		t.Fatalf("I7 violated: fired=%d duplicates=%d, want exactly 1 fire", total.Fired, total.Duplicates)
	}
	key := store.CronKey(sc.ID, scheduledAt)
	if n := countRows(t, st, "SELECT count(*) FROM jobs WHERE schedule_id = $1 AND idempotency_key = $2", sc.ID, key); n != 1 {
		t.Fatalf("jobs for tick %s = %d", key, n)
	}
	var next time.Time
	var lastJob *uuid.UUID
	if err := st.Pool().QueryRow(ctx, "SELECT next_run_at, last_job_id FROM schedules WHERE id = $1", sc.ID).Scan(&next, &lastJob); err != nil {
		t.Fatal(err)
	}
	if !next.After(time.Now()) || lastJob == nil {
		t.Fatalf("schedule not advanced: next=%s last_job=%v", next, lastJob)
	}

	// A replica that still sees the old next_run_at must not enqueue the same tick again.
	if _, err := st.Pool().Exec(ctx, "UPDATE schedules SET next_run_at = $2 WHERE id = $1", sc.ID, scheduledAt); err != nil {
		t.Fatal(err)
	}
	res, err := st.TickSchedules(ctx, 20, uuid.NewV7)
	if err != nil || res.Fired != 0 || res.Duplicates != 1 {
		t.Fatalf("replayed tick: %+v %v", res, err)
	}
	if n := countRows(t, st, "SELECT count(*) FROM jobs WHERE schedule_id = $1", sc.ID); n != 1 {
		t.Fatalf("duplicate tick created a job: %d", n)
	}
}

func TestCronWithRunningWorkers(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	sc, err := st.CreateSchedule(bg(t), store.NewSchedule{
		ID: uuid.Must(uuid.NewV7()), CronExpr: "@every 1s", Type: "echo", MaxRetries: 0, TimeoutSeconds: 5, Enabled: true,
	})
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	start := time.Now()
	for i := 0; i < 3; i++ {
		startWorker(t, st, fastWorkerConfig(fmt.Sprintf("w-cron-%d", i)))
	}
	eventually(t, 10*time.Second, "3 cron jobs succeeded", func() bool {
		return countRows(t, st, "SELECT count(*) FROM jobs WHERE schedule_id = $1 AND status = 'succeeded'", sc.ID) >= 3
	})
	elapsed := time.Since(start)
	n := countRows(t, st, "SELECT count(*) FROM jobs WHERE schedule_id = $1", sc.ID)
	if maxTicks := int(elapsed/time.Second) + 1; n > maxTicks {
		t.Fatalf("%d cron jobs in %s: more than one per tick", n, elapsed)
	}
	if d := countRows(t, st, "SELECT count(DISTINCT idempotency_key) FROM jobs WHERE schedule_id = $1", sc.ID); d != n {
		t.Fatalf("distinct keys %d != jobs %d", d, n)
	}
}

func TestDrainingBlocksClaimsAndEnqueue(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	j := enqueue(t, st, "echo", `{}`)
	setDraining(t, st, true)

	if jobs, err := st.ClaimJobs(bg(t), "w-drain", 5, time.Second); err != nil || len(jobs) != 0 {
		t.Fatalf("claimed %d jobs while draining (%v)", len(jobs), err)
	}
	_, err := st.EnqueueJob(bg(t), store.NewJob{ID: uuid.Must(uuid.NewV7()), Type: "echo", MaxRetries: 1, TimeoutSeconds: 5})
	if !errors.Is(err, store.ErrDraining) {
		t.Fatalf("enqueue while draining: err = %v, want ErrDraining", err)
	}
	setDraining(t, st, false)
	jobs, err := st.ClaimJobs(bg(t), "w-drain", 5, time.Second)
	if err != nil || len(jobs) != 1 || jobs[0].ID != j.ID {
		t.Fatalf("claim after resume: %v %d", err, len(jobs))
	}
}

func TestIdempotentEnqueue(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	first := enqueue(t, st, "echo", `{"a":1}`, withKey("order-42", "h1"))

	res, err := st.EnqueueJob(bg(t), store.NewJob{ID: uuid.Must(uuid.NewV7()), Type: "echo", MaxRetries: 1, TimeoutSeconds: 5,
		IdempotencyKey: ptr("order-42"), RequestHash: ptr("h1")})
	if err != nil || res.Created || res.Job.ID != first.ID {
		t.Fatalf("duplicate: created=%v id=%s err=%v", res.Created, res.Job.ID, err)
	}
	_, err = st.EnqueueJob(bg(t), store.NewJob{ID: uuid.Must(uuid.NewV7()), Type: "echo", MaxRetries: 1, TimeoutSeconds: 5,
		IdempotencyKey: ptr("order-42"), RequestHash: ptr("h2")})
	if !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("mismatched body: err = %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := st.EnqueueJob(bg(t), store.NewJob{ID: uuid.Must(uuid.NewV7()), Type: "echo", MaxRetries: 1, TimeoutSeconds: 5,
				IdempotencyKey: ptr("race-key"), RequestHash: ptr("h")})
			if err != nil {
				t.Errorf("concurrent enqueue: %v", err)
				return
			}
			if r.Created {
				mu.Lock()
				created++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if created != 1 || countRows(t, st, "SELECT count(*) FROM jobs WHERE idempotency_key = 'race-key'") != 1 {
		t.Fatalf("I9 violated: created=%d", created)
	}
}

func TestGracefulShutdownWaitsForInFlight(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	stop := startWorker(t, st, fastWorkerConfig("w-graceful"))
	j := enqueue(t, st, "sleep", `{"duration_ms":700}`)
	waitStatus(t, st, j.ID, domain.StatusRunning, 5*time.Second)

	if err, took := stop(); err != nil || took < 300*time.Millisecond {
		t.Fatalf("stop: err=%v took=%s (should wait for the in-flight job)", err, took)
	}
	if got := getJob(t, st, j.ID); got.Status != domain.StatusSucceeded {
		t.Fatalf("in-flight job not finished on shutdown: %s", got.Status)
	}
}

func TestGracefulShutdownTimeoutRequeues(t *testing.T) {
	t.Parallel()
	st := newMigratedStore(t)
	cfg := fastWorkerConfig("w-impatient")
	cfg.ShutdownTimeout = 200 * time.Millisecond
	stop := startWorker(t, st, cfg)
	j := enqueue(t, st, "sleep", `{"duration_ms":20000}`, withTimeout(60), withMaxRetries(2))
	waitStatus(t, st, j.ID, domain.StatusRunning, 5*time.Second)

	if err, took := stop(); err != nil || took > 3*time.Second {
		t.Fatalf("stop: err=%v took=%s", err, took)
	}
	got := getJob(t, st, j.ID)
	as := attemptsAsc(t, st, j.ID)
	if got.Status != domain.StatusQueued || len(as) != 1 || as[0].Status != domain.AttemptFailed ||
		as[0].Error == nil || !strings.Contains(*as[0].Error, "worker shutdown") {
		t.Fatalf("job=%s attempts=%+v", got.Status, as)
	}
	if n := countRows(t, st, "SELECT count(*) FROM workers WHERE worker_id = 'w-impatient' AND status = 'stopped'"); n != 1 {
		t.Fatal("worker not marked stopped")
	}
}

func ptr[T any](v T) *T { return &v }
