//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/google/uuid"
)

func TestAPIAuthAndValidation(t *testing.T) {
	st := newMigratedStore(t)
	a := startAPI(t, st)

	for _, p := range []string{"/healthz", "/readyz", "/metrics"} {
		expectStatus(t, a.do("GET", p, nil, map[string]string{"Authorization": noAuth}), http.StatusOK, p+" without token")
	}
	expectError(t, a.do("GET", "/v1/jobs", nil, map[string]string{"Authorization": noAuth}), 401, "unauthorized", "no token")
	expectError(t, a.do("GET", "/v1/jobs", nil, map[string]string{"Authorization": "Bearer wrong"}), 401, "unauthorized", "wrong token")
	expectError(t, a.do("GET", "/v1/jobs", nil, map[string]string{"Authorization": "Basic " + testToken}), 401, "unauthorized", "wrong scheme")
	expectError(t, a.do("POST", "/v1/jobs", `{"type":"echo"}`, map[string]string{"Authorization": noAuth}), 401, "unauthorized", "submit without token")
	expectStatus(t, a.do("GET", "/v1/jobs", nil, nil), 200, "valid token")

	r := a.do("GET", "/v1/jobs/"+uuid.NewString(), nil, map[string]string{"X-Request-ID": "req-abc"})
	expectError(t, r, 404, "not_found", "unknown job")
	if r.header.Get("X-Request-ID") != "req-abc" {
		t.Fatalf("X-Request-ID not echoed: %q", r.header.Get("X-Request-ID"))
	}

	big := `{"type":"echo","payload":"` + strings.Repeat("a", 300<<10) + `"}`
	huge := `{"type":"echo","payload":"` + strings.Repeat("a", 2<<20) + `"}`
	bad := []struct {
		name, body, code string
		hdr              map[string]string
	}{
		{"empty body", ``, "invalid_request", nil},
		{"malformed json", `{"type":`, "invalid_request", nil},
		{"missing type", `{}`, "invalid_request", nil},
		{"unknown type", `{"type":"nope"}`, "unknown_job_type", nil},
		{"unknown field", `{"type":"echo","bogus":1}`, "invalid_request", nil},
		{"trailing data", `{"type":"echo"}{}`, "invalid_request", nil},
		{"priority range", `{"type":"echo","priority":1001}`, "invalid_request", nil},
		{"retries range", `{"type":"echo","max_retries":26}`, "invalid_request", nil},
		{"timeout range", `{"type":"echo","timeout_seconds":0}`, "invalid_request", nil},
		{"wrong field type", `{"type":"echo","priority":"high"}`, "invalid_request", nil},
		{"bad run_at", `{"type":"echo","run_at":"soon"}`, "invalid_request", nil},
		{"payload > 256KiB", big, "invalid_request", nil},
		{"body > 1MiB", huge, "invalid_request", nil},
		{"reserved key cron", `{"type":"echo"}`, "invalid_request", map[string]string{"Idempotency-Key": "cron:x"}},
		{"reserved key replay", `{"type":"echo"}`, "invalid_request", map[string]string{"Idempotency-Key": "dlq-replay:x"}},
		{"key too long", `{"type":"echo"}`, "invalid_request", map[string]string{"Idempotency-Key": strings.Repeat("k", 256)}},
	}
	for _, tc := range bad {
		expectError(t, a.do("POST", "/v1/jobs", tc.body, tc.hdr), 400, tc.code, tc.name)
	}

	for _, p := range []string{"/v1/jobs/not-a-uuid", "/v1/jobs?status=bogus", "/v1/jobs?limit=0", "/v1/jobs?limit=501", "/v1/dlq?limit=x", "/v1/schedules?limit=-1"} {
		expectError(t, a.do("GET", p, nil, nil), 400, "invalid_request", p)
	}
	expectError(t, a.do("POST", "/v1/jobs/not-a-uuid/cancel", nil, nil), 400, "invalid_request", "cancel bad uuid")
	expectError(t, a.do("POST", "/v1/jobs/"+uuid.NewString()+"/cancel", nil, nil), 404, "not_found", "cancel unknown")
	expectError(t, a.do("POST", "/v1/dlq/"+uuid.NewString()+"/replay", nil, nil), 404, "not_found", "replay unknown")
	expectError(t, a.do("POST", "/v1/queue/drain", `{"mode":"nope"}`, nil), 400, "invalid_request", "drain bad mode")
	expectError(t, a.do("POST", "/v1/queue/drain", ``, nil), 400, "invalid_request", "drain empty body")
	expectError(t, a.do("POST", "/v1/queue/resume", `{"x":1}`, nil), 400, "invalid_request", "resume unknown field")
	expectError(t, a.do("POST", "/v1/schedules", `{"cron_expr":"61 * * * *","type":"echo"}`, nil), 400, "invalid_request", "bad cron")
	expectError(t, a.do("POST", "/v1/schedules", `{"cron_expr":"@hourly","type":"nope"}`, nil), 400, "unknown_job_type", "schedule unknown type")
	expectError(t, a.do("DELETE", "/v1/schedules/"+uuid.NewString(), nil, nil), 404, "not_found", "delete unknown schedule")
	expectStatus(t, a.do("DELETE", "/v1/jobs", nil, nil), http.StatusMethodNotAllowed, "wrong method")

	if n := countRows(t, st, "SELECT count(*) FROM jobs"); n != 0 {
		t.Fatalf("rejected requests created %d jobs", n)
	}
	if n := countRows(t, st, "SELECT count(*) FROM schedules"); n != 0 {
		t.Fatalf("rejected requests created %d schedules", n)
	}
}

func TestAPIIdempotency(t *testing.T) {
	st := newMigratedStore(t)
	a := startAPI(t, st)
	key := map[string]string{"Idempotency-Key": "order-42"}

	r1 := a.do("POST", "/v1/jobs", `{"type":"echo","payload":{"a":1,"b":[1,2]},"priority":5}`, key)
	expectStatus(t, r1, 202, "first submit")
	var s1 submitted
	r1.decode(t, &s1)
	if r1.header.Get("Idempotent-Replayed") != "" {
		t.Fatal("first submit must not be marked replayed")
	}

	r2 := a.do("POST", "/v1/jobs", ` { "priority":5, "payload":{"b":[1, 2],"a":1}, "type":"echo" } `, key)
	expectStatus(t, r2, 200, "replayed submit")
	var s2 submitted
	r2.decode(t, &s2)
	if s2.JobID != s1.JobID || r2.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay = %s (header %q), want job %s", r2.body, r2.header.Get("Idempotent-Replayed"), s1.JobID)
	}

	expectError(t, a.do("POST", "/v1/jobs", `{"type":"echo","payload":{"a":2,"b":[1,2]},"priority":5}`, key), 409, "idempotency_key_conflict", "different payload")
	expectError(t, a.do("POST", "/v1/jobs", `{"type":"echo","payload":{"a":1,"b":[1,2]},"priority":6}`, key), 409, "idempotency_key_conflict", "different priority")

	if n := countRows(t, st, "SELECT count(*) FROM jobs WHERE idempotency_key = 'order-42'"); n != 1 {
		t.Fatalf("jobs with key = %d, want 1", n)
	}
	if j := a.job(s1.JobID); j.IdempotencyKey == nil || *j.IdempotencyKey != "order-42" || j.Priority != 5 {
		t.Fatalf("stored job = %+v", j)
	}
	other := a.do("POST", "/v1/jobs", `{"type":"echo"}`, map[string]string{"Idempotency-Key": "order-43"})
	expectStatus(t, other, 202, "different key")
}

func TestAPIJobLifecycle(t *testing.T) {
	st := newMigratedStore(t)
	a := startAPI(t, st)
	startWorker(t, st, fastWorkerConfig("w-life-1"))
	startWorker(t, st, fastWorkerConfig("w-life-2"))

	echo := a.submit(map[string]any{"type": "echo", "payload": map[string]any{"msg": "hi"}})
	flaky := a.submit(map[string]any{"type": "flaky", "payload": map[string]any{"fail_times": 2}, "max_retries": 3})
	timeout := a.submit(map[string]any{"type": "sleep", "payload": map[string]any{"duration_ms": 3000}, "timeout_seconds": 1, "max_retries": 1})
	perm := a.submit(map[string]any{"type": "permanent_fail"})
	always := a.submit(map[string]any{"type": "always_fail", "max_retries": 2})

	j := a.waitJob(echo.JobID, domain.StatusSucceeded, 10*time.Second)
	if !jsonEqual(t, j.Result, `{"msg":"hi"}`) || j.AttemptCount != 1 || len(j.Attempts) != 1 || j.FinishedAt == nil {
		t.Fatalf("echo = %+v result=%s", j, j.Result)
	}
	if at := j.Attempts[0]; at.Status != domain.AttemptSucceeded || at.DurationMS == nil || at.FinishedAt == nil || at.WorkerID == "" {
		t.Fatalf("echo attempt = %+v", at)
	}

	j = a.waitJob(flaky.JobID, domain.StatusSucceeded, 15*time.Second)
	want := []domain.AttemptStatus{domain.AttemptSucceeded, domain.AttemptFailed, domain.AttemptFailed}
	if j.AttemptCount != 3 || !sameStatuses(attemptStatusesNewestFirst(j), want) || !jsonEqual(t, j.Result, `{"attempt":3}`) {
		t.Fatalf("flaky = count %d attempts %v result %s", j.AttemptCount, attemptStatusesNewestFirst(j), j.Result)
	}
	if j.Attempts[0].AttemptNumber != 3 || j.Attempts[2].AttemptNumber != 1 {
		t.Fatalf("attempts must be newest first: %+v", j.Attempts)
	}

	j = a.waitJob(perm.JobID, domain.StatusFailed, 10*time.Second)
	if j.AttemptCount != 1 || j.LastError == nil || !strings.Contains(*j.LastError, "permanent") {
		t.Fatalf("permanent_fail = %+v", j)
	}

	j = a.waitJob(timeout.JobID, domain.StatusDeadLettered, 20*time.Second)
	want = []domain.AttemptStatus{domain.AttemptTimedOut, domain.AttemptTimedOut}
	if j.AttemptCount != 2 || !sameStatuses(attemptStatusesNewestFirst(j), want) {
		t.Fatalf("sleep timeout = count %d attempts %v", j.AttemptCount, attemptStatusesNewestFirst(j))
	}

	j = a.waitJob(always.JobID, domain.StatusDeadLettered, 15*time.Second)
	if j.AttemptCount != 3 || len(j.Attempts) != 3 {
		t.Fatalf("always_fail = %+v", j)
	}

	r := a.do("GET", "/v1/dlq", nil, nil)
	expectStatus(t, r, 200, "list dlq")
	var dlq struct {
		Entries []struct {
			JobID        uuid.UUID       `json:"job_id"`
			Type         string          `json:"type"`
			Payload      json.RawMessage `json:"payload"`
			AttemptCount int             `json:"attempt_count"`
			MaxRetries   int             `json:"max_retries"`
			LastError    *string         `json:"last_error"`
			ReplayJobID  *uuid.UUID      `json:"replay_job_id"`
		} `json:"entries"`
		Count int `json:"count"`
	}
	r.decode(t, &dlq)
	if dlq.Count != 2 || len(dlq.Entries) != 2 {
		t.Fatalf("dlq = %s", r.body)
	}
	found := false
	for _, e := range dlq.Entries {
		if e.JobID == always.JobID {
			found = true
			if e.Type != "always_fail" || e.AttemptCount != 3 || e.MaxRetries != 2 || e.LastError == nil || e.ReplayJobID != nil {
				t.Fatalf("dlq entry = %+v", e)
			}
		}
	}
	if !found {
		t.Fatalf("always_fail job missing from dlq: %s", r.body)
	}

	r = a.do("POST", "/v1/dlq/"+always.JobID.String()+"/replay", nil, nil)
	expectStatus(t, r, 202, "first replay")
	var rep struct {
		JobID    uuid.UUID     `json:"job_id"`
		ReplayOf uuid.UUID     `json:"replay_of"`
		Status   domain.Status `json:"status"`
	}
	r.decode(t, &rep)
	if rep.JobID == always.JobID || rep.ReplayOf != always.JobID || rep.Status != domain.StatusQueued {
		t.Fatalf("replay = %s", r.body)
	}
	r = a.do("POST", "/v1/dlq/"+always.JobID.String()+"/replay", nil, nil)
	expectStatus(t, r, 200, "repeat replay")
	var rep2 struct {
		JobID uuid.UUID `json:"job_id"`
	}
	r.decode(t, &rep2)
	if rep2.JobID != rep.JobID || r.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("repeat replay = %s, want job %s", r.body, rep.JobID)
	}

	nj := a.waitJob(rep.JobID, domain.StatusDeadLettered, 15*time.Second)
	if nj.ReplayOf == nil || *nj.ReplayOf != always.JobID || nj.AttemptCount != 3 || nj.IdempotencyKey == nil || *nj.IdempotencyKey != store.ReplayKey(always.JobID) {
		t.Fatalf("replayed job = %+v", nj)
	}
	if orig := a.job(always.JobID); orig.Status != domain.StatusDeadLettered || len(orig.Attempts) != 3 {
		t.Fatalf("original must keep its history: %+v", orig)
	}
	expectError(t, a.do("POST", "/v1/dlq/"+echo.JobID.String()+"/replay", nil, nil), 404, "not_found", "replay non-dlq job")

	r = a.do("GET", "/v1/jobs?status=succeeded", nil, nil)
	var list struct {
		Jobs  []apiJob `json:"jobs"`
		Count int      `json:"count"`
	}
	r.decode(t, &list)
	if list.Count != 2 || len(list.Jobs) != 2 || list.Jobs[0].Attempts != nil {
		t.Fatalf("succeeded list = %s", r.body)
	}
	r = a.do("GET", "/v1/jobs?type=always_fail&status=dead-lettered&limit=1", nil, nil)
	r.decode(t, &list)
	if list.Count != 1 || list.Jobs[0].ID != rep.JobID {
		t.Fatalf("filtered list (newest first, limit 1) = %s", r.body)
	}
}

func TestAPICancel(t *testing.T) {
	st := newMigratedStore(t)
	a := startAPI(t, st)
	startWorker(t, st, fastWorkerConfig("w-cancel"))

	future := a.submit(map[string]any{"type": "echo", "run_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
	for i, want := range []int{200, 200} {
		r := a.do("POST", "/v1/jobs/"+future.JobID.String()+"/cancel", nil, nil)
		expectStatus(t, r, want, "cancel queued")
		var body struct {
			JobID  uuid.UUID     `json:"job_id"`
			Status domain.Status `json:"status"`
		}
		r.decode(t, &body)
		if body.JobID != future.JobID || body.Status != domain.StatusCancelled {
			t.Fatalf("cancel #%d = %s", i+1, r.body)
		}
	}
	if j := a.job(future.JobID); j.Status != domain.StatusCancelled || j.FinishedAt == nil {
		t.Fatalf("cancelled job = %+v", j)
	}

	running := a.submit(map[string]any{"type": "sleep", "payload": map[string]any{"duration_ms": 1500}})
	a.waitJob(running.JobID, domain.StatusRunning, 5*time.Second)
	r := a.do("POST", "/v1/jobs/"+running.JobID.String()+"/cancel", nil, nil)
	expectError(t, r, 409, "conflict", "cancel running")
	var e apiError
	r.decode(t, &e)
	if e.Error.Message != "job is running" {
		t.Fatalf("cancel running message = %q", e.Error.Message)
	}

	a.waitJob(running.JobID, domain.StatusSucceeded, 10*time.Second)
	r = a.do("POST", "/v1/jobs/"+running.JobID.String()+"/cancel", nil, nil)
	expectError(t, r, 409, "conflict", "cancel terminal")
	r.decode(t, &e)
	if e.Error.Message != "job is succeeded" {
		t.Fatalf("cancel terminal message = %q", e.Error.Message)
	}
}

type statsBody struct {
	WindowSeconds int64   `json:"window_seconds"`
	Draining      bool    `json:"draining"`
	DrainMode     *string `json:"drain_mode"`
	Depth         struct {
		ByStatus   map[domain.Status]int64 `json:"by_status"`
		ByPriority []struct {
			Priority int   `json:"priority"`
			Queued   int64 `json:"queued"`
			Running  int64 `json:"running"`
		} `json:"by_priority"`
		Ready   int64 `json:"ready"`
		Delayed int64 `json:"delayed"`
	} `json:"depth"`
	ActiveWorkers     int64    `json:"active_workers"`
	TotalConcurrency  int64    `json:"total_concurrency"`
	ActiveJobs        int64    `json:"active_jobs"`
	WorkerUtilization float64  `json:"worker_utilization"`
	P50LatencyMS      *float64 `json:"p50_latency_ms"`
	P95LatencyMS      *float64 `json:"p95_latency_ms"`
	DeadLetterRate    float64  `json:"dead_letter_rate"`
	DeadLetterCount   int64    `json:"dead_letter_count"`
}

func (a *apiClient) stats() statsBody {
	a.t.Helper()
	r := a.do("GET", "/v1/queue/stats", nil, nil)
	expectStatus(a.t, r, 200, "stats")
	var s statsBody
	r.decode(a.t, &s)
	return s
}

func TestAPIDrainAndResume(t *testing.T) {
	st := newMigratedStore(t)
	a := startAPI(t, st)
	startWorker(t, st, fastWorkerConfig("w-drain"))

	inflight := a.submit(map[string]any{"type": "sleep", "payload": map[string]any{"duration_ms": 1200}})
	a.waitJob(inflight.JobID, domain.StatusRunning, 5*time.Second)
	soon := a.submit(map[string]any{"type": "echo", "run_at": time.Now().Add(300 * time.Millisecond).UTC().Format(time.RFC3339Nano)})

	r := a.do("POST", "/v1/queue/drain", `{"mode":"pause"}`, nil)
	expectStatus(t, r, 200, "drain pause")
	var d map[string]any
	r.decode(t, &d)
	if d["draining"] != true || d["mode"] != "pause" || d["queued"] != float64(1) || d["running"] != float64(1) {
		t.Fatalf("drain pause = %s", r.body)
	}
	r = a.do("POST", "/v1/jobs", `{"type":"echo"}`, nil)
	expectError(t, r, 503, "queue_draining", "submit while draining")
	if r.header.Get("Retry-After") != "30" {
		t.Fatalf("Retry-After = %q", r.header.Get("Retry-After"))
	}
	if s := a.stats(); !s.Draining || s.DrainMode == nil || *s.DrainMode != "pause" {
		t.Fatalf("stats while paused = %+v", s)
	}

	a.waitJob(inflight.JobID, domain.StatusSucceeded, 10*time.Second)
	time.Sleep(time.Second)
	if j := a.job(soon.JobID); j.Status != domain.StatusQueued || j.AttemptCount != 0 {
		t.Fatalf("ready job claimed while paused: %+v", j)
	}

	r = a.do("POST", "/v1/queue/resume", nil, nil)
	expectStatus(t, r, 200, "resume")
	if strings.TrimSpace(string(r.body)) != `{"draining":false}` {
		t.Fatalf("resume body = %s", r.body)
	}
	a.waitJob(soon.JobID, domain.StatusSucceeded, 5*time.Second)
	after := a.submit(map[string]any{"type": "echo"})
	a.waitJob(after.JobID, domain.StatusSucceeded, 5*time.Second)
	if s := a.stats(); s.Draining || s.DrainMode != nil {
		t.Fatalf("stats after resume = %+v", s)
	}

	var pending []uuid.UUID
	for i := 0; i < 3; i++ {
		pending = append(pending, a.submit(map[string]any{"type": "echo", "run_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}).JobID)
	}
	r = a.do("POST", "/v1/queue/drain", `{"mode":"cancel_pending"}`, nil)
	expectStatus(t, r, 200, "drain cancel_pending")
	d = nil
	r.decode(t, &d)
	if d["draining"] != true || d["mode"] != "cancel_pending" || d["cancelled"] != float64(3) || d["running"] != float64(0) {
		t.Fatalf("drain cancel_pending = %s", r.body)
	}
	if _, has := d["queued"]; has {
		t.Fatalf("cancel_pending body must not include queued: %s", r.body)
	}
	for _, id := range pending {
		if j := a.job(id); j.Status != domain.StatusCancelled {
			t.Fatalf("pending job %s = %s, want cancelled", id, j.Status)
		}
	}
	expectError(t, a.do("POST", "/v1/jobs", `{"type":"echo"}`, nil), 503, "queue_draining", "submit while cancel_pending")
	expectStatus(t, a.do("POST", "/v1/queue/resume", `{}`, nil), 200, "resume with {}")
	a.submit(map[string]any{"type": "echo"})
}

func TestAPIDrainBlocksReplay(t *testing.T) {
	st := newMigratedStore(t)
	a := startAPI(t, st)
	stop := startWorker(t, st, fastWorkerConfig("w-replay-drain"))
	dead := a.submit(map[string]any{"type": "always_fail", "max_retries": 0})
	a.waitJob(dead.JobID, domain.StatusDeadLettered, 10*time.Second)
	if err, _ := stop(); err != nil {
		t.Fatalf("stop worker: %v", err)
	}

	expectStatus(t, a.do("POST", "/v1/queue/drain", `{"mode":"pause"}`, nil), 200, "drain")
	r := a.do("POST", "/v1/dlq/"+dead.JobID.String()+"/replay", nil, nil)
	expectError(t, r, 503, "queue_draining", "replay while draining")
	if n := countRows(t, st, "SELECT count(*) FROM dead_letter_jobs WHERE replay_job_id IS NOT NULL"); n != 0 {
		t.Fatalf("replay while draining recorded %d replays", n)
	}
	expectStatus(t, a.do("POST", "/v1/queue/resume", nil, nil), 200, "resume")
	expectStatus(t, a.do("POST", "/v1/dlq/"+dead.JobID.String()+"/replay", nil, nil), 202, "replay after resume")
}

func TestAPIStatsAndMetrics(t *testing.T) {
	st := newMigratedStore(t)
	a := startAPI(t, st)

	s := a.stats()
	if s.WindowSeconds != 300 || s.P50LatencyMS != nil || s.P95LatencyMS != nil || s.ActiveWorkers != 0 || s.WorkerUtilization != 0 || s.DeadLetterRate != 0 {
		t.Fatalf("empty stats = %+v", s)
	}
	for _, status := range domain.AllStatuses {
		if v, ok := s.Depth.ByStatus[status]; !ok || v != 0 {
			t.Fatalf("by_status[%s] = %d (present %v), want 0", status, v, ok)
		}
	}

	startWorker(t, st, fastWorkerConfig("w-stats-1"))
	startWorker(t, st, fastWorkerConfig("w-stats-2"))

	var ids []uuid.UUID
	for i := 0; i < 8; i++ {
		ids = append(ids, a.submit(map[string]any{"type": "echo", "payload": map[string]any{"i": i}}).JobID)
	}
	dead := a.submit(map[string]any{"type": "always_fail", "max_retries": 0})
	for _, id := range ids {
		a.waitJob(id, domain.StatusSucceeded, 10*time.Second)
	}
	a.waitJob(dead.JobID, domain.StatusDeadLettered, 10*time.Second)

	sleepers := []uuid.UUID{
		a.submit(map[string]any{"type": "sleep", "payload": map[string]any{"duration_ms": 1500}, "priority": 7}).JobID,
		a.submit(map[string]any{"type": "sleep", "payload": map[string]any{"duration_ms": 1500}, "priority": 7}).JobID,
	}
	a.submit(map[string]any{"type": "echo", "run_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "priority": -3})
	for _, id := range sleepers {
		a.waitJob(id, domain.StatusRunning, 5*time.Second)
	}

	eventually(t, 3*time.Second, "heartbeats to report active jobs", func() bool {
		s = a.stats()
		return s.ActiveWorkers == 2 && s.ActiveJobs == 2
	})
	if s.TotalConcurrency != 8 || s.WorkerUtilization != 0.25 {
		t.Fatalf("utilization = %d/%d → %v, want 2/8 → 0.25", s.ActiveJobs, s.TotalConcurrency, s.WorkerUtilization)
	}
	if s.Depth.ByStatus[domain.StatusSucceeded] != 8 || s.Depth.ByStatus[domain.StatusDeadLettered] != 1 ||
		s.Depth.ByStatus[domain.StatusRunning] != 2 || s.Depth.ByStatus[domain.StatusQueued] != 1 {
		t.Fatalf("by_status = %v", s.Depth.ByStatus)
	}
	if s.Depth.Ready != 0 || s.Depth.Delayed != 1 {
		t.Fatalf("ready/delayed = %d/%d, want 0/1", s.Depth.Ready, s.Depth.Delayed)
	}
	if len(s.Depth.ByPriority) != 2 || s.Depth.ByPriority[0].Priority != 7 || s.Depth.ByPriority[0].Running != 2 ||
		s.Depth.ByPriority[1].Priority != -3 || s.Depth.ByPriority[1].Queued != 1 {
		t.Fatalf("by_priority = %+v", s.Depth.ByPriority)
	}
	if s.P50LatencyMS == nil || s.P95LatencyMS == nil || *s.P50LatencyMS < 0 || *s.P95LatencyMS < *s.P50LatencyMS {
		t.Fatalf("latency p50=%v p95=%v", s.P50LatencyMS, s.P95LatencyMS)
	}
	if s.DeadLetterCount != 1 || s.DeadLetterRate < 0.11 || s.DeadLetterRate > 0.12 {
		t.Fatalf("dead letters = count %d rate %v, want 1 and 1/9", s.DeadLetterCount, s.DeadLetterRate)
	}

	r := a.do("GET", "/metrics", nil, map[string]string{"Authorization": noAuth})
	expectStatus(t, r, 200, "metrics")
	if ct := r.header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("metrics Content-Type = %q", ct)
	}
	m, err := parseProm(r.body)
	if err != nil {
		t.Fatalf("invalid exposition: %v\n%s", err, r.body)
	}
	checks := map[string]func(float64) bool{
		`jobq_active_workers`:                                                       func(v float64) bool { return v >= 1 },
		`jobq_total_concurrency`:                                                    func(v float64) bool { return v >= 4 },
		`jobq_jobs_by_status{status="succeeded"}`:                                   func(v float64) bool { return v >= 8 },
		`jobq_jobs_by_status{status="dead-lettered"}`:                               func(v float64) bool { return v == 1 },
		`jobq_jobs{status="queued",priority="-3"}`:                                  func(v float64) bool { return v == 1 },
		`jobq_queue_delayed`:                                                        func(v float64) bool { return v == 1 },
		`jobq_attempt_latency_ms{quantile="0.5"}`:                                   func(v float64) bool { return v >= 0 },
		`jobq_attempt_latency_ms{quantile="0.95"}`:                                  func(v float64) bool { return v >= 0 },
		`jobq_dead_letter_rate`:                                                     func(v float64) bool { return v > 0 && v < 1 },
		`jobq_dead_letter_jobs`:                                                     func(v float64) bool { return v == 1 },
		`jobq_attempts_window{status="succeeded"}`:                                  func(v float64) bool { return v >= 8 },
		`jobq_attempts_window{status="failed"}`:                                     func(v float64) bool { return v == 1 },
		`jobq_draining`:                                                             func(v float64) bool { return v == 0 },
		`jobq_http_requests_total{method="POST",route="/v1/jobs",code="202"}`:       func(v float64) bool { return v == 12 },
		`jobq_http_requests_total{method="GET",route="/v1/queue/stats",code="200"}`: func(v float64) bool { return v >= 2 },
	}
	for series, ok := range checks {
		v, present := m[series]
		if !present || !ok(v) {
			t.Errorf("metric %s = %v (present %v)\n%s", series, v, present, r.body)
		}
	}
	for _, id := range sleepers {
		a.waitJob(id, domain.StatusSucceeded, 10*time.Second)
	}
}

func TestAPISchedules(t *testing.T) {
	st := newMigratedStore(t)
	a := startAPI(t, st)

	r := a.do("POST", "/v1/schedules", map[string]any{"name": "tick", "cron_expr": "@every 1s", "type": "echo", "payload": map[string]any{"hi": 1}, "priority": 2}, nil)
	expectStatus(t, r, 201, "create schedule")
	var sc struct {
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
	}
	r.decode(t, &sc)
	if sc.Name == nil || *sc.Name != "tick" || sc.CronExpr != "@every 1s" || sc.Type != "echo" || !jsonEqual(t, sc.Payload, `{"hi":1}`) ||
		sc.Priority != 2 || sc.MaxRetries != 3 || sc.TimeoutSeconds != 30 || !sc.Enabled || sc.NextRunAt.IsZero() || sc.LastRunAt != nil {
		t.Fatalf("schedule = %s", r.body)
	}
	if r.header.Get("Location") != "/v1/schedules/"+sc.ID.String() {
		t.Fatalf("Location = %q", r.header.Get("Location"))
	}
	expectStatus(t, a.do("POST", "/v1/schedules", `{"cron_expr":"0 3 * * *","type":"sleep","enabled":false,"payload":{"duration_ms":1}}`, nil), 201, "disabled schedule")

	r = a.do("GET", "/v1/schedules", nil, nil)
	var list struct {
		Schedules []struct {
			ID      uuid.UUID `json:"id"`
			Enabled bool      `json:"enabled"`
		} `json:"schedules"`
		Count int `json:"count"`
	}
	r.decode(t, &list)
	if list.Count != 2 || len(list.Schedules) != 2 {
		t.Fatalf("schedules = %s", r.body)
	}

	startWorker(t, st, fastWorkerConfig("w-cron-1"))
	startWorker(t, st, fastWorkerConfig("w-cron-2"))
	var fired apiJob
	eventually(t, 5*time.Second, "schedule to enqueue and finish a job", func() bool {
		var jl struct {
			Jobs []apiJob `json:"jobs"`
		}
		a.do("GET", "/v1/jobs?status=succeeded&type=echo", nil, nil).decode(t, &jl)
		for _, j := range jl.Jobs {
			if j.ScheduleID != nil && *j.ScheduleID == sc.ID {
				fired = j
				return true
			}
		}
		return false
	})
	if fired.IdempotencyKey == nil || !strings.HasPrefix(*fired.IdempotencyKey, "cron:"+sc.ID.String()+":") || fired.Priority != 2 {
		t.Fatalf("cron job = %+v", fired)
	}
	if n := countRows(t, st, "SELECT count(*) - count(DISTINCT idempotency_key) FROM jobs WHERE schedule_id = $1", sc.ID); n != 0 {
		t.Fatalf("%d duplicate cron enqueues", n)
	}

	expectStatus(t, a.do("DELETE", "/v1/schedules/"+sc.ID.String(), nil, nil), 204, "delete schedule")
	expectError(t, a.do("DELETE", "/v1/schedules/"+sc.ID.String(), nil, nil), 404, "not_found", "delete again")
	if j := a.job(fired.ID); j.ScheduleID != nil {
		t.Fatalf("job keeps deleted schedule_id: %+v", j)
	}
	r = a.do("GET", "/v1/schedules", nil, nil)
	r.decode(t, &list)
	if list.Count != 1 || list.Schedules[0].Enabled {
		t.Fatalf("schedules after delete = %s", r.body)
	}
}
