package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/google/uuid"
)

func job(payload string, attempt int) Job {
	return Job{ID: uuid.New(), Payload: json.RawMessage(payload), Attempt: attempt, MaxRetries: 3, TimeoutSeconds: 5}
}

func TestRegistry(t *testing.T) {
	r, err := Builtin(NewWebhookClient(), "test")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"always_fail", "echo", "flaky", "http_webhook", "permanent_fail", "sleep"}
	if got := strings.Join(r.Names(), ","); got != strings.Join(want, ",") {
		t.Fatalf("Names() = %s", got)
	}
	if _, ok := r.Get("echo"); !ok {
		t.Fatal("echo missing")
	}
	if _, ok := r.Get("nope"); ok {
		t.Fatal("unexpected handler")
	}
	if err := r.Register("echo", Echo); err == nil {
		t.Fatal("duplicate register must fail")
	}
	if err := r.Register("", Echo); err == nil {
		t.Fatal("empty name must fail")
	}
}

func TestEcho(t *testing.T) {
	out, err := Echo(context.Background(), job(`{"a":1}`, 1))
	if err != nil || string(out) != `{"a":1}` {
		t.Fatalf("Echo = %s, %v", out, err)
	}
	out, err = Echo(context.Background(), job(``, 1))
	if err != nil || string(out) != `{}` {
		t.Fatalf("Echo empty = %s, %v", out, err)
	}
}

func TestSleep(t *testing.T) {
	out, err := Sleep(context.Background(), job(`{"duration_ms":5}`, 1))
	if err != nil || string(out) != `{"slept_ms":5}` {
		t.Fatalf("Sleep = %s, %v", out, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := Sleep(ctx, job(`{"duration_ms":10000}`, 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Sleep past deadline err = %v", err)
	}
	for _, bad := range []string{`{}`, `{"duration_ms":-1}`, `{"duration_ms":"x"}`, `{"duration_ms":1,"x":1}`, `[`} {
		if _, err := Sleep(context.Background(), job(bad, 1)); !domain.IsPermanent(err) {
			t.Errorf("Sleep(%s) err = %v, want permanent", bad, err)
		}
	}
}

func TestFlaky(t *testing.T) {
	for attempt := 1; attempt <= 2; attempt++ {
		_, err := Flaky(context.Background(), job(`{"fail_times":2}`, attempt))
		if err == nil || domain.IsPermanent(err) {
			t.Fatalf("attempt %d: want transient error, got %v", attempt, err)
		}
	}
	out, err := Flaky(context.Background(), job(`{"fail_times":2}`, 3))
	if err != nil || string(out) != `{"attempt":3}` {
		t.Fatalf("attempt 3 = %s, %v", out, err)
	}
	if _, err := Flaky(context.Background(), job(`{}`, 1)); !domain.IsPermanent(err) {
		t.Fatalf("missing fail_times must be permanent, got %v", err)
	}
}

func TestFailHandlers(t *testing.T) {
	if _, err := AlwaysFail(context.Background(), job(`{}`, 1)); err == nil || domain.IsPermanent(err) {
		t.Fatalf("AlwaysFail err = %v, want transient", err)
	}
	if _, err := PermanentFail(context.Background(), job(`{}`, 1)); !domain.IsPermanent(err) {
		t.Fatalf("PermanentFail err = %v, want permanent", err)
	}
}

func TestHTTPWebhookPropagatesHeadersAndBody(t *testing.T) {
	var gotJobID, gotAttempt, gotMethod, gotCustom, gotCT, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotJobID, gotAttempt, gotMethod = r.Header.Get("X-Job-ID"), r.Header.Get("X-Attempt-Number"), r.Method
		gotCustom, gotCT = r.Header.Get("X-Custom"), r.Header.Get("Content-Type")
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		gotBody = string(b)
		w.WriteHeader(http.StatusCreated)
		if _, err := w.Write([]byte("created")); err != nil {
			t.Errorf("write: %v", err)
		}
	}))
	defer srv.Close()

	h := NewHTTPWebhook(srv.Client(), "jobq/test")
	j := job(`{"url":"`+srv.URL+`/hook","method":"put","headers":{"X-Custom":"v","X-Job-ID":"spoofed"},"body":{"k":1}}`, 2)
	out, err := h(context.Background(), j)
	if err != nil {
		t.Fatalf("webhook: %v", err)
	}
	if gotJobID != j.ID.String() || gotAttempt != "2" || gotMethod != http.MethodPut || gotCustom != "v" ||
		gotCT != "application/json" || gotBody != `{"k":1}` {
		t.Fatalf("request mismatch: id=%s attempt=%s method=%s custom=%s ct=%s body=%s", gotJobID, gotAttempt, gotMethod, gotCustom, gotCT, gotBody)
	}
	var res struct {
		StatusCode int    `json:"status_code"`
		Body       string `json:"body"`
	}
	if err := json.Unmarshal(out, &res); err != nil || res.StatusCode != 201 || res.Body != "created" {
		t.Fatalf("result = %s (%v)", out, err)
	}
}

func TestHTTPWebhookStatusClassification(t *testing.T) {
	tests := []struct {
		code          int
		wantPermanent bool
	}{
		{500, false}, {503, false}, {408, false}, {429, false}, {400, true}, {404, true}, {422, true},
	}
	for _, tt := range tests {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tt.code) }))
		_, err := NewHTTPWebhook(srv.Client(), "t")(context.Background(), job(`{"url":"`+srv.URL+`"}`, 1))
		srv.Close()
		if err == nil || domain.IsPermanent(err) != tt.wantPermanent {
			t.Errorf("status %d: err=%v permanent=%v, want permanent=%v", tt.code, err, domain.IsPermanent(err), tt.wantPermanent)
		}
	}
}

func TestHTTPWebhookRejectsBadInput(t *testing.T) {
	h := NewHTTPWebhook(NewWebhookClient(), "t")
	for _, p := range []string{
		`{"url":"file:///etc/passwd"}`, `{"url":"ftp://x/y"}`, `{"url":"gopher://x"}`, `{"url":"http://"}`,
		`{"url":"::bad"}`, `{"url":"http://x","method":"TRACE"}`, `{}`, `{"url":"http://x","extra":1}`,
	} {
		if _, err := h(context.Background(), job(p, 1)); !domain.IsPermanent(err) {
			t.Errorf("payload %s: err=%v, want permanent", p, err)
		}
	}
}

func TestHTTPWebhookCapsResponseBody(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write([]byte(big)); err != nil {
			return // client stops reading at the cap; a broken pipe here is expected
		}
	}))
	defer srv.Close()
	out, err := NewHTTPWebhook(srv.Client(), "t")(context.Background(), job(`{"url":"`+srv.URL+`","method":"GET"}`, 1))
	if err != nil {
		t.Fatalf("webhook: %v", err)
	}
	var res struct {
		Body string `json:"body"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Body) != maxWebhookBodyKept {
		t.Fatalf("kept body len = %d, want %d", len(res.Body), maxWebhookBodyKept)
	}
}

func TestHTTPWebhookHonorsContextDeadline(t *testing.T) {
	var hits atomic.Int32
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(block)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := NewHTTPWebhook(srv.Client(), "t")(ctx, job(`{"url":"`+srv.URL+`"}`, 1))
	if err == nil || domain.IsPermanent(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want transient deadline exceeded", err)
	}
	if time.Since(start) > 2*time.Second || hits.Load() != 1 {
		t.Fatalf("deadline not honored (elapsed %s, hits %d)", time.Since(start), hits.Load())
	}
}

func TestHTTPWebhookRedirectLimit(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srv.URL+"/loop", http.StatusFound)
	}))
	defer srv.Close()
	client := NewWebhookClient()
	client.Transport = srv.Client().Transport
	_, err := NewHTTPWebhook(client, "t")(context.Background(), job(`{"url":"`+srv.URL+`","method":"GET"}`, 1))
	if err == nil || !strings.Contains(err.Error(), "redirects") {
		t.Fatalf("err = %v, want redirect limit error", err)
	}
}
