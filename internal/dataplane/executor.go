package dataplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/dataplane/handlers"
	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
)

// abandonGrace is how long the executor waits for a handler to return after its ctx ends before
// abandoning the goroutine (handlers that ignore ctx).
const abandonGrace = 250 * time.Millisecond

// ExecResult is the classified outcome of one attempt.
type ExecResult struct {
	Output        json.RawMessage
	Err           error
	Outcome       domain.Outcome
	AttemptStatus domain.AttemptStatus
	Duration      time.Duration
	Abandoned     bool
}

// ErrorMessage returns the error text for persistence ("" on success).
func (r ExecResult) ErrorMessage() string {
	if r.Err == nil {
		return ""
	}
	return r.Err.Error()
}

type handlerReturn struct {
	out json.RawMessage
	err error
}

// Execute runs h under context.WithTimeout(parent, timeout) with panic recovery and classifies
// the result (SPEC §4.4). parent cancellation means worker shutdown.
func Execute(parent context.Context, log *slog.Logger, h handlers.Handler, job handlers.Job, timeout time.Duration) ExecResult {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	start := time.Now()
	done := make(chan handlerReturn, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("handler panicked", "job_id", job.ID, "type", job.Type, "attempt", job.Attempt,
					"panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
				done <- handlerReturn{err: fmt.Errorf("panic: %v", rec)}
			}
		}()
		out, err := h(ctx, job)
		done <- handlerReturn{out: out, err: err}
	}()

	var ret handlerReturn
	abandoned := false
	select {
	case ret = <-done:
	case <-ctx.Done():
		grace := time.NewTimer(abandonGrace)
		select {
		case ret = <-done:
		case <-grace.C:
			abandoned = true
			log.Warn("handler ignored context cancellation; goroutine abandoned",
				"job_id", job.ID, "type", job.Type, "attempt", job.Attempt)
		}
		grace.Stop()
	}

	res := classify(parent, ctx, ret, timeout)
	res.Duration = time.Since(start)
	res.Abandoned = abandoned
	return res
}

func classify(parent, ctx context.Context, ret handlerReturn, timeout time.Duration) ExecResult {
	if ret.err == nil && ctx.Err() == nil {
		if len(ret.out) > 0 && !json.Valid(ret.out) {
			return ExecResult{
				Err:     domain.Permanent(errors.New("handler returned an invalid JSON result")),
				Outcome: domain.OutcomePermanent, AttemptStatus: domain.AttemptFailed,
			}
		}
		return ExecResult{Output: ret.out, Outcome: domain.OutcomeSuccess, AttemptStatus: domain.AttemptSucceeded}
	}
	if parent.Err() != nil {
		return ExecResult{
			Err:     fmt.Errorf("worker shutdown: %w", parent.Err()),
			Outcome: domain.OutcomeTransient, AttemptStatus: domain.AttemptFailed,
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ExecResult{
			Err:     fmt.Errorf("attempt timed out after %s", timeout),
			Outcome: domain.OutcomeTransient, AttemptStatus: domain.AttemptTimedOut,
		}
	}
	if domain.IsPermanent(ret.err) {
		return ExecResult{Err: ret.err, Outcome: domain.OutcomePermanent, AttemptStatus: domain.AttemptFailed}
	}
	return ExecResult{Err: ret.err, Outcome: domain.OutcomeTransient, AttemptStatus: domain.AttemptFailed}
}
