package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func discardLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func do(t *testing.T, h http.Handler, method, path, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthEndpointsBypassAuth(t *testing.T) {
	s := newServer(Config{APIToken: "s3cret"}, fakePinger{}, nil, discardLogger())
	s.ready.Store(true)
	h := s.Handler()

	for _, path := range []string{"/healthz", "/readyz"} {
		if rec := do(t, h, http.MethodGet, path, ""); rec.Code != http.StatusOK {
			t.Fatalf("%s without token = %d, want 200 (body %s)", path, rec.Code, rec.Body)
		}
	}
}

func TestV1RequiresToken(t *testing.T) {
	s := newServer(Config{APIToken: "s3cret"}, fakePinger{}, nil, discardLogger())
	h := s.Handler()

	for _, auth := range []string{"", "Bearer wrong", "s3cret", "Basic s3cret"} {
		rec := do(t, h, http.MethodGet, "/v1/__nope", auth)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("auth %q = %d, want 401", auth, rec.Code)
		}
		var body errorBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error.Code != codeUnauthorized {
			t.Fatalf("bad 401 envelope %q: %v", rec.Body, err)
		}
	}
	if rec := do(t, h, http.MethodGet, "/v1/__nope", "Bearer s3cret"); rec.Code == http.StatusUnauthorized {
		t.Fatalf("valid token rejected")
	}
}

func TestNoTokenConfiguredMeansOpen(t *testing.T) {
	h := newServer(Config{}, fakePinger{}, nil, discardLogger()).Handler()
	if rec := do(t, h, http.MethodGet, "/v1/__nope", ""); rec.Code == http.StatusUnauthorized {
		t.Fatalf("auth enforced without API_TOKEN")
	}
}

func TestReadyz(t *testing.T) {
	s := newServer(Config{}, fakePinger{err: errors.New("down")}, nil, discardLogger())
	s.ready.Store(true)
	if rec := do(t, s.Handler(), http.MethodGet, "/readyz", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with failing db = %d, want 503", rec.Code)
	}

	s = newServer(Config{}, fakePinger{}, nil, discardLogger())
	if rec := do(t, s.Handler(), http.MethodGet, "/readyz", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before serving = %d, want 503", rec.Code)
	}
}

func TestRequestIDEchoed(t *testing.T) {
	h := newServer(Config{}, fakePinger{}, nil, discardLogger()).Handler()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "abc-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "abc-123" {
		t.Fatalf("X-Request-ID = %q", got)
	}
	if rec := do(t, h, http.MethodGet, "/healthz", ""); rec.Header().Get("X-Request-ID") == "" {
		t.Fatalf("X-Request-ID not generated")
	}
}

func TestServeGracefulShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := newServer(Config{}, fakePinger{}, nil, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()

	url := "http://" + ln.Addr().String() + "/readyz"
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			code := resp.StatusCode
			if cerr := resp.Body.Close(); cerr != nil {
				t.Fatal(cerr)
			}
			if code == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never became ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
	if s.ready.Load() {
		t.Fatal("ready flag still set after shutdown")
	}
}
