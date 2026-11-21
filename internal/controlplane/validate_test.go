package controlplane

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
)

var known = map[string]bool{"echo": true, "sleep": true}

func ip(v int) *int       { return &v }
func sp(v string) *string { return &v }

func TestValidateSubmitDefaults(t *testing.T) {
	v, err := validateSubmit(submitRequest{Type: "echo"}, known)
	if err != nil {
		t.Fatal(err)
	}
	if string(v.Payload) != `{}` || v.Priority != 0 || v.MaxRetries != 3 || v.TimeoutSeconds != 30 || v.RunAt != nil {
		t.Fatalf("defaults = %+v", v)
	}
	v, err = validateSubmit(submitRequest{Type: "echo", Payload: json.RawMessage(" null "), RunAt: sp("2026-11-21T10:00:00+05:30")}, known)
	if err != nil || string(v.Payload) != `{}` || !v.RunAt.Equal(time.Date(2026, 11, 21, 4, 30, 0, 0, time.UTC)) {
		t.Fatalf("null payload / run_at = %+v %v", v, err)
	}
}

func TestValidateSubmitErrors(t *testing.T) {
	tests := []struct {
		name        string
		req         submitRequest
		unknownType bool
		substr      string
	}{
		{"missing type", submitRequest{}, false, "type is required"},
		{"unknown type", submitRequest{Type: "nope"}, true, "unknown job type"},
		{"long type", submitRequest{Type: strings.Repeat("x", 65)}, false, "at most 64"},
		{"priority high", submitRequest{Type: "echo", Priority: ip(1001)}, false, "priority"},
		{"priority low", submitRequest{Type: "echo", Priority: ip(-1001)}, false, "priority"},
		{"retries negative", submitRequest{Type: "echo", MaxRetries: ip(-1)}, false, "max_retries"},
		{"retries high", submitRequest{Type: "echo", MaxRetries: ip(26)}, false, "max_retries"},
		{"timeout zero", submitRequest{Type: "echo", TimeoutSeconds: ip(0)}, false, "timeout_seconds"},
		{"timeout high", submitRequest{Type: "echo", TimeoutSeconds: ip(3601)}, false, "timeout_seconds"},
		{"bad run_at", submitRequest{Type: "echo", RunAt: sp("tomorrow")}, false, "RFC3339"},
		{"big payload", submitRequest{Type: "echo", Payload: json.RawMessage(`"` + strings.Repeat("a", maxPayloadBytes) + `"`)}, false, "payload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateSubmit(tt.req, known)
			fe, ok := asFieldError(err)
			if !ok || fe.isUnknownType != tt.unknownType || !strings.Contains(err.Error(), tt.substr) {
				t.Fatalf("err = %v (unknownType=%v), want %q unknownType=%v", err, ok && fe.isUnknownType, tt.substr, tt.unknownType)
			}
		})
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	for _, ok := range []string{"order-42", "a", strings.Repeat("k", 255), "with space"} {
		if err := validateIdempotencyKey(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{strings.Repeat("k", 256), "cron:abc", "dlq-replay:x", "tab\tkey", "ünicode"} {
		if err := validateIdempotencyKey(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestRequestHashCanonical(t *testing.T) {
	mk := func(payload string, prio int) validSubmit {
		v, err := validateSubmit(submitRequest{Type: "echo", Payload: json.RawMessage(payload), Priority: &prio}, known)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	h := func(v validSubmit) string {
		s, err := requestHash(v)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	a := h(mk(`{"a":1,"b":[1,2]}`, 1))
	if b := h(mk(` { "b":[1, 2], "a":1 } `, 1)); a != b {
		t.Fatal("key order / whitespace must not change the hash")
	}
	if c := h(mk(`{"a":2,"b":[1,2]}`, 1)); a == c {
		t.Fatal("different payload must change the hash")
	}
	if d := h(mk(`{"a":1,"b":[1,2]}`, 2)); a == d {
		t.Fatal("different priority must change the hash")
	}
	if h(mk(`{"n":12345678901234567890}`, 0)) == h(mk(`{"n":12345678901234567891}`, 0)) {
		t.Fatal("large integers must keep full precision")
	}
}

func TestValidateSchedule(t *testing.T) {
	v, err := validateSchedule(scheduleRequest{CronExpr: " @every 30s ", Type: "echo"}, known)
	if err != nil || v.CronExpr != "@every 30s" || !v.Enabled || v.MaxRetries != 3 {
		t.Fatalf("valid schedule = %+v %v", v, err)
	}
	for name, req := range map[string]scheduleRequest{
		"missing cron": {Type: "echo"},
		"bad cron":     {CronExpr: "61 * * * *", Type: "echo"},
		"unknown type": {CronExpr: "@hourly", Type: "nope"},
		"bad timeout":  {CronExpr: "@hourly", Type: "echo", TimeoutSeconds: ip(0)},
		"long name":    {Name: sp(strings.Repeat("n", 129)), CronExpr: "@hourly", Type: "echo"},
	} {
		if _, err := validateSchedule(req, known); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestDecodeJSON(t *testing.T) {
	var v submitRequest
	cases := map[string]string{
		"empty":    "",
		"bad json": `{"type":`,
		"unknown":  `{"type":"echo","bogus":1}`,
		"trailing": `{"type":"echo"} {"x":1}`,
		"garbage":  `{"type":"echo"}x`,
		"array":    `[1]`,
	}
	for name, body := range cases {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		if err := decodeJSON(r, &v, false); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	r := httptest.NewRequest("POST", "/", strings.NewReader(` {"type":"echo"} `))
	if err := decodeJSON(r, &v, false); err != nil || v.Type != "echo" {
		t.Fatalf("valid body: %v", err)
	}
	r = httptest.NewRequest("POST", "/", strings.NewReader(""))
	if err := decodeJSON(r, &struct{}{}, true); err != nil {
		t.Fatalf("empty allowed: %v", err)
	}
}

var promLine = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{([a-zA-Z_][a-zA-Z0-9_]*="([^"\\]|\\.)*",?)*\})? [-+0-9.eEInfNa]+$`)

// assertPromText checks every line is a HELP/TYPE comment or a valid sample whose metric has a TYPE.
func assertPromText(t *testing.T, body []byte) map[string]bool {
	t.Helper()
	typed := map[string]bool{}
	seen := map[string]bool{}
	sc := bufio.NewScanner(bytes.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "# TYPE "):
			f := strings.Fields(line)
			if len(f) != 4 {
				t.Fatalf("bad TYPE line %q", line)
			}
			typed[f[2]] = true
		case strings.HasPrefix(line, "# HELP "):
		case line == "":
			t.Fatal("blank line in exposition")
		default:
			if !promLine.MatchString(line) {
				t.Fatalf("invalid sample line %q", line)
			}
			name := line[:strings.IndexAny(line, "{ ")]
			if !typed[name] {
				t.Fatalf("sample %q has no TYPE", name)
			}
			seen[name] = true
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return seen
}

func TestRenderMetrics(t *testing.T) {
	p50, p95 := 12.5, 99.0
	st := store.QueueStats{
		ByStatus:         map[domain.Status]int64{domain.StatusQueued: 3, domain.StatusSucceeded: 10},
		ByPriority:       []store.PriorityDepth{{Priority: 10, Queued: 2, Running: 1}, {Priority: -5, Queued: 1}},
		ActiveWorkers:    2,
		TotalConcurrency: 16,
		ActiveJobs:       4, WorkerUtilization: 0.25, P50LatencyMS: &p50, P95LatencyMS: &p95,
		DeadLetterRate: 0.1, DeadLetterCount: 1, Draining: true,
		AttemptsInWindow: map[domain.AttemptStatus]int64{domain.AttemptFenced: 2},
	}
	body := renderMetrics(st, []requestCount{{requestKey{"GET", `/v1/jobs/{id}`, 200}, 3}})
	seen := assertPromText(t, body)
	for _, m := range []string{"jobq_jobs", "jobq_jobs_by_status", "jobq_active_workers", "jobq_worker_utilization",
		"jobq_attempt_latency_ms", "jobq_dead_letter_rate", "jobq_draining", "jobq_http_requests_total", "jobq_attempts_window"} {
		if !seen[m] {
			t.Errorf("metric %s missing", m)
		}
	}
	for _, want := range []string{
		`jobq_jobs{status="queued",priority="-5"} 1`,
		`jobq_attempt_latency_ms{quantile="0.95"} 99`,
		`jobq_worker_utilization 0.25`,
		`jobq_draining 1`,
		`jobq_attempts_window{status="fenced"} 2`,
		`jobq_http_requests_total{method="GET",route="/v1/jobs/{id}",code="200"} 3`,
	} {
		if !bytes.Contains(body, []byte(want+"\n")) {
			t.Errorf("missing line %q in:\n%s", want, body)
		}
	}
	if got := escapeLabel("a\"b\\c\nd"); got != `a\"b\\c\nd` {
		t.Fatalf("escapeLabel = %q", got)
	}
}
