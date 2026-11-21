package dataplane

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/dataplane/handlers"
	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
	"github.com/google/uuid"
)

func quietLog() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func testJob() handlers.Job {
	return handlers.Job{ID: uuid.New(), Type: "t", Payload: json.RawMessage(`{}`), Attempt: 1, MaxRetries: 3, TimeoutSeconds: 1}
}

func TestExecuteClassification(t *testing.T) {
	tests := []struct {
		name          string
		h             handlers.Handler
		timeout       time.Duration
		wantOutcome   domain.Outcome
		wantAttempt   domain.AttemptStatus
		wantErrSubstr string
	}{
		{
			name: "success",
			h: func(context.Context, handlers.Job) (json.RawMessage, error) {
				return json.RawMessage(`{"ok":true}`), nil
			},
			timeout:     time.Second,
			wantOutcome: domain.OutcomeSuccess, wantAttempt: domain.AttemptSucceeded,
		},
		{
			name:        "nil result is success",
			h:           func(context.Context, handlers.Job) (json.RawMessage, error) { return nil, nil },
			timeout:     time.Second,
			wantOutcome: domain.OutcomeSuccess, wantAttempt: domain.AttemptSucceeded,
		},
		{
			name:        "transient error",
			h:           func(context.Context, handlers.Job) (json.RawMessage, error) { return nil, errors.New("boom") },
			timeout:     time.Second,
			wantOutcome: domain.OutcomeTransient, wantAttempt: domain.AttemptFailed, wantErrSubstr: "boom",
		},
		{
			name: "permanent error",
			h: func(context.Context, handlers.Job) (json.RawMessage, error) {
				return nil, domain.Permanent(errors.New("bad input"))
			},
			timeout:     time.Second,
			wantOutcome: domain.OutcomePermanent, wantAttempt: domain.AttemptFailed, wantErrSubstr: "bad input",
		},
		{
			name: "timeout honored by handler",
			h: func(ctx context.Context, _ handlers.Job) (json.RawMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
			timeout:     30 * time.Millisecond,
			wantOutcome: domain.OutcomeTransient, wantAttempt: domain.AttemptTimedOut, wantErrSubstr: "timed out",
		},
		{
			name: "permanent error returned after deadline is still a timeout",
			h: func(ctx context.Context, _ handlers.Job) (json.RawMessage, error) {
				<-ctx.Done()
				return nil, domain.Permanent(errors.New("late"))
			},
			timeout:     30 * time.Millisecond,
			wantOutcome: domain.OutcomeTransient, wantAttempt: domain.AttemptTimedOut,
		},
		{
			name:        "panic becomes transient",
			h:           func(context.Context, handlers.Job) (json.RawMessage, error) { panic("kaboom") },
			timeout:     time.Second,
			wantOutcome: domain.OutcomeTransient, wantAttempt: domain.AttemptFailed, wantErrSubstr: "panic: kaboom",
		},
		{
			name: "invalid JSON result is permanent",
			h: func(context.Context, handlers.Job) (json.RawMessage, error) {
				return json.RawMessage(`{not json`), nil
			},
			timeout:     time.Second,
			wantOutcome: domain.OutcomePermanent, wantAttempt: domain.AttemptFailed, wantErrSubstr: "invalid JSON",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := Execute(context.Background(), quietLog(), tt.h, testJob(), tt.timeout)
			if res.Outcome != tt.wantOutcome || res.AttemptStatus != tt.wantAttempt {
				t.Fatalf("got outcome=%v attempt=%s err=%v; want %v %s", res.Outcome, res.AttemptStatus, res.Err, tt.wantOutcome, tt.wantAttempt)
			}
			if tt.wantErrSubstr != "" && (res.Err == nil || !strings.Contains(res.Err.Error(), tt.wantErrSubstr)) {
				t.Fatalf("err = %v, want substring %q", res.Err, tt.wantErrSubstr)
			}
			if tt.wantOutcome == domain.OutcomeSuccess && res.Err != nil {
				t.Fatalf("success with err %v", res.Err)
			}
		})
	}
}

func TestExecuteAbandonsHandlerIgnoringContext(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	h := func(context.Context, handlers.Job) (json.RawMessage, error) {
		<-release
		return nil, nil
	}
	start := time.Now()
	res := Execute(context.Background(), quietLog(), h, testJob(), 20*time.Millisecond)
	if !res.Abandoned || res.AttemptStatus != domain.AttemptTimedOut {
		t.Fatalf("want abandoned timed_out, got %+v", res)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("executor blocked %s on a handler ignoring ctx", elapsed)
	}
}

func TestExecuteParentCancelIsShutdown(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	h := func(ctx context.Context, _ handlers.Job) (json.RawMessage, error) {
		cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	res := Execute(parent, quietLog(), h, testJob(), time.Minute)
	if res.Outcome != domain.OutcomeTransient || res.AttemptStatus != domain.AttemptFailed ||
		res.Err == nil || !strings.Contains(res.Err.Error(), "worker shutdown") {
		t.Fatalf("want transient worker shutdown, got %+v", res)
	}
}

func TestExecuteMeasuresDuration(t *testing.T) {
	h := func(context.Context, handlers.Job) (json.RawMessage, error) {
		time.Sleep(20 * time.Millisecond)
		return nil, nil
	}
	if res := Execute(context.Background(), quietLog(), h, testJob(), time.Second); res.Duration < 20*time.Millisecond {
		t.Fatalf("duration %s < 20ms", res.Duration)
	}
}
