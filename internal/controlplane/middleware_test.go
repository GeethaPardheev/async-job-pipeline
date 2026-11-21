package controlplane

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(s.b.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("log line %q: %v", l, err)
		}
		out = append(out, m)
	}
	return out
}

func TestPanicRecoveryReturnsJSON500(t *testing.T) {
	var buf syncBuffer
	s := newServer(Config{}, fakePinger{}, nil, slog.New(slog.NewJSONHandler(&buf, nil)))
	s.mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("kaboom") })

	rec := do(t, s.Handler(), "GET", "/boom", "")
	if rec.Code != 500 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("status=%d content-type=%q", rec.Code, rec.Header().Get("Content-Type"))
	}
	var e struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error.Code != "internal" || strings.Contains(rec.Body.String(), "kaboom") {
		t.Fatalf("body = %s (%v)", rec.Body, err)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("X-Request-ID missing on panic response")
	}
	found := false
	for _, l := range buf.lines(t) {
		if l["msg"] == "panic in http handler" && l["panic"] == "kaboom" && l["request_id"] == rec.Header().Get("X-Request-ID") {
			found = true
		}
	}
	if !found {
		t.Fatal("panic was not logged with its request_id")
	}
}

func TestAccessLogFields(t *testing.T) {
	var buf syncBuffer
	s := newServer(Config{}, fakePinger{}, nil, slog.New(slog.NewJSONHandler(&buf, nil)))
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("X-Request-ID", "rid-1")
	s.Handler().ServeHTTP(httptest.NewRecorder(), req)
	do(t, s.Handler(), "GET", "/v1/jobs/abc", "")

	var access []map[string]any
	for _, l := range buf.lines(t) {
		if l["msg"] == "http request" {
			access = append(access, l)
		}
	}
	if len(access) != 2 {
		t.Fatalf("access lines = %v", access)
	}
	a := access[0]
	for _, k := range []string{"request_id", "method", "path", "status", "latency_ms", "route"} {
		if _, ok := a[k]; !ok {
			t.Fatalf("access log missing %q: %v", k, a)
		}
	}
	if a["request_id"] != "rid-1" || a["method"] != "GET" || a["path"] != "/healthz" || a["status"] != float64(200) || a["route"] != "/healthz" {
		t.Fatalf("access log = %v", a)
	}
	if access[1]["route"] != "/v1/jobs/{id}" {
		t.Fatalf("route label must drop the method: %v", access[1]["route"])
	}
	snap := s.reqs.snapshot()
	if len(snap) != 2 || snap[0].route != "/healthz" || snap[0].n != 1 {
		t.Fatalf("request counters = %+v", snap)
	}
}

func TestBodyLimit(t *testing.T) {
	s := newServer(Config{}, fakePinger{}, nil, discardLogger())
	var got error
	s.mux.HandleFunc("POST /echo", func(w http.ResponseWriter, r *http.Request) {
		got = decodeJSON(r, &map[string]any{}, false)
		w.WriteHeader(204)
	})
	body := `{"a":"` + strings.Repeat("x", maxBodyBytes) + `"}`
	s.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/echo", strings.NewReader(body)))
	if got == nil || !strings.Contains(got.Error(), "exceeds") {
		t.Fatalf("oversized body error = %v", got)
	}
}
