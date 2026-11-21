//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/controlplane"
	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/google/uuid"
)

const testToken = "test-token"

type apiClient struct {
	t      *testing.T
	base   string
	token  string
	client *http.Client
}

// startAPI serves the control plane on a loopback port with bearer auth and waits for /readyz.
func startAPI(t *testing.T, st *store.Store) *apiClient {
	t.Helper()
	srv := controlplane.New(controlplane.Config{
		APIToken:         testToken,
		Version:          "test",
		JobTypes:         testRegistry(t).Names(),
		WorkerStaleAfter: 5 * time.Second,
		StatsWindow:      5 * time.Minute,
	}, st, testLogger())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("api Serve: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Error("api did not shut down within 20s")
		}
	})

	a := &apiClient{t: t, base: "http://" + ln.Addr().String(), token: testToken, client: &http.Client{Timeout: 10 * time.Second}}
	eventually(t, 5*time.Second, "api /readyz to return 200", func() bool {
		return a.do("GET", "/readyz", nil, nil).status == http.StatusOK
	})
	return a
}

type apiResp struct {
	status int
	header http.Header
	body   []byte
}

// noAuth as a header value removes the default Authorization header.
const noAuth = "\x00"

// do sends a request. body may be nil, a string (sent raw) or a value to JSON-encode.
func (a *apiClient) do(method, path string, body any, hdr map[string]string) apiResp {
	a.t.Helper()
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(b)
	case []byte:
		rd = bytes.NewReader(b)
	default:
		enc, err := json.Marshal(b)
		if err != nil {
			a.t.Fatalf("marshal body: %v", err)
		}
		rd = bytes.NewReader(enc)
	}
	req, err := http.NewRequest(method, a.base+path, rd)
	if err != nil {
		a.t.Fatalf("new request: %v", err)
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	for k, v := range hdr {
		if v == noAuth {
			req.Header.Del(k)
			continue
		}
		req.Header.Set(k, v)
	}
	res, err := a.client.Do(req)
	if err != nil {
		a.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		a.t.Fatalf("read %s %s: %v", method, path, err)
	}
	if res.Header.Get("X-Request-ID") == "" {
		a.t.Fatalf("%s %s: response has no X-Request-ID", method, path)
	}
	return apiResp{status: res.StatusCode, header: res.Header, body: b}
}

func (r apiResp) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (r apiResp) errCode(t *testing.T) string {
	t.Helper()
	var e apiError
	r.decode(t, &e)
	if e.Error.Code == "" || e.Error.Message == "" {
		t.Fatalf("not an error envelope: %s", r.body)
	}
	return e.Error.Code
}

func expectStatus(t *testing.T, r apiResp, want int, what string) {
	t.Helper()
	if r.status != want {
		t.Fatalf("%s: status %d, want %d; body %s", what, r.status, want, r.body)
	}
}

func expectError(t *testing.T, r apiResp, status int, code, what string) {
	t.Helper()
	expectStatus(t, r, status, what)
	if got := r.errCode(t); got != code {
		t.Fatalf("%s: code %q, want %q; body %s", what, got, code, r.body)
	}
}

type submitted struct {
	JobID  uuid.UUID     `json:"job_id"`
	Status domain.Status `json:"status"`
	RunAt  time.Time     `json:"run_at"`
}

func (a *apiClient) submit(body map[string]any) submitted {
	a.t.Helper()
	r := a.do("POST", "/v1/jobs", body, nil)
	expectStatus(a.t, r, http.StatusAccepted, fmt.Sprintf("submit %v", body))
	var s submitted
	r.decode(a.t, &s)
	if s.Status != domain.StatusQueued || r.header.Get("Location") != "/v1/jobs/"+s.JobID.String() {
		a.t.Fatalf("submit response %s Location=%q", r.body, r.header.Get("Location"))
	}
	return s
}

type apiAttempt struct {
	AttemptNumber int                  `json:"attempt_number"`
	WorkerID      string               `json:"worker_id"`
	Status        domain.AttemptStatus `json:"status"`
	Error         *string              `json:"error"`
	FinishedAt    *time.Time           `json:"finished_at"`
	DurationMS    *int64               `json:"duration_ms"`
}

type apiJob struct {
	ID             uuid.UUID       `json:"id"`
	Type           string          `json:"type"`
	Status         domain.Status   `json:"status"`
	Priority       int             `json:"priority"`
	Payload        json.RawMessage `json:"payload"`
	AttemptCount   int             `json:"attempt_count"`
	MaxRetries     int             `json:"max_retries"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	LastError      *string         `json:"last_error"`
	Result         json.RawMessage `json:"result"`
	IdempotencyKey *string         `json:"idempotency_key"`
	ScheduleID     *uuid.UUID      `json:"schedule_id"`
	ReplayOf       *uuid.UUID      `json:"replay_of"`
	FinishedAt     *time.Time      `json:"finished_at"`
	Attempts       []apiAttempt    `json:"attempts"`
}

func (a *apiClient) job(id uuid.UUID) apiJob {
	a.t.Helper()
	r := a.do("GET", "/v1/jobs/"+id.String(), nil, nil)
	expectStatus(a.t, r, http.StatusOK, "get job")
	var j apiJob
	r.decode(a.t, &j)
	return j
}

func (a *apiClient) waitJob(id uuid.UUID, want domain.Status, timeout time.Duration) apiJob {
	a.t.Helper()
	var j apiJob
	eventually(a.t, timeout, "job "+id.String()+" to reach "+string(want)+" via API", func() bool {
		j = a.job(id)
		return j.Status == want
	})
	return j
}

func attemptStatusesNewestFirst(j apiJob) []domain.AttemptStatus {
	out := make([]domain.AttemptStatus, len(j.Attempts))
	for i, at := range j.Attempts {
		out[i] = at.Status
	}
	return out
}

// jsonEqual compares JSON semantically; jsonb normalizes whitespace and key order.
func jsonEqual(t *testing.T, got json.RawMessage, want string) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("decode %s: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("decode %s: %v", want, err)
	}
	gb, err := json.Marshal(g)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	wb, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	return bytes.Equal(gb, wb)
}

func sameStatuses(a, b []domain.AttemptStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var promSample = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(?:[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*",?)*\})? (\S+)$`)

// parseProm validates Prometheus text format 0.0.4 and returns sample values keyed by "name{labels}".
func parseProm(body []byte) (map[string]float64, error) {
	typed := map[string]bool{}
	samples := map[string]float64{}
	sc := bufio.NewScanner(bytes.NewReader(body))
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "# TYPE "):
			f := strings.Fields(line)
			if len(f) != 4 || !map[string]bool{"gauge": true, "counter": true}[f[3]] {
				return nil, fmt.Errorf("line %d: bad TYPE %q", n, line)
			}
			if typed[f[2]] {
				return nil, fmt.Errorf("line %d: duplicate TYPE for %s", n, f[2])
			}
			typed[f[2]] = true
		case strings.HasPrefix(line, "# HELP "):
		default:
			m := promSample.FindStringSubmatch(line)
			if m == nil {
				return nil, fmt.Errorf("line %d: invalid sample %q", n, line)
			}
			if !typed[m[1]] {
				return nil, fmt.Errorf("line %d: sample %s before its TYPE", n, m[1])
			}
			v, err := strconv.ParseFloat(m[3], 64)
			if err != nil {
				return nil, fmt.Errorf("line %d: value %q: %w", n, m[3], err)
			}
			key := m[1] + m[2]
			if _, dup := samples[key]; dup {
				return nil, fmt.Errorf("line %d: duplicate series %s", n, key)
			}
			samples[key] = v
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(samples) == 0 {
		return nil, errors.New("no samples")
	}
	return samples, nil
}
