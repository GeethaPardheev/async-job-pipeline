package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
)

const maxSleep = time.Hour

// decodePayload strictly decodes payload into v; any decode error is permanent.
func decodePayload(payload json.RawMessage, v any) error {
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return domain.Permanent(fmt.Errorf("invalid payload: %w", err))
	}
	return nil
}

func marshalResult(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}
	return b, nil
}

// Echo returns the payload as the result.
func Echo(_ context.Context, job Job) (json.RawMessage, error) {
	if len(job.Payload) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return job.Payload, nil
}

// Sleep waits duration_ms, honoring ctx; exceeding timeout_seconds surfaces as a timed-out attempt.
func Sleep(ctx context.Context, job Job) (json.RawMessage, error) {
	var p struct {
		DurationMS *int64 `json:"duration_ms"`
	}
	if err := decodePayload(job.Payload, &p); err != nil {
		return nil, err
	}
	if p.DurationMS == nil || *p.DurationMS < 0 || time.Duration(*p.DurationMS)*time.Millisecond > maxSleep {
		return nil, domain.Permanent(fmt.Errorf("invalid payload: duration_ms must be between 0 and %d", maxSleep.Milliseconds()))
	}
	t := time.NewTimer(time.Duration(*p.DurationMS) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("sleep interrupted: %w", ctx.Err())
	case <-t.C:
		return marshalResult(map[string]int64{"slept_ms": *p.DurationMS})
	}
}

// Flaky fails transiently while attempt <= fail_times, then succeeds.
func Flaky(_ context.Context, job Job) (json.RawMessage, error) {
	var p struct {
		FailTimes *int `json:"fail_times"`
	}
	if err := decodePayload(job.Payload, &p); err != nil {
		return nil, err
	}
	if p.FailTimes == nil || *p.FailTimes < 0 {
		return nil, domain.Permanent(errors.New("invalid payload: fail_times must be >= 0"))
	}
	if job.Attempt <= *p.FailTimes {
		return nil, fmt.Errorf("flaky: simulated transient failure on attempt %d (fails first %d)", job.Attempt, *p.FailTimes)
	}
	return marshalResult(map[string]int{"attempt": job.Attempt})
}

// AlwaysFail always returns a transient error, exhausting retries into the DLQ.
func AlwaysFail(_ context.Context, job Job) (json.RawMessage, error) {
	return nil, fmt.Errorf("always_fail: simulated transient failure on attempt %d", job.Attempt)
}

// PermanentFail returns a permanent error, so the job goes straight to failed.
func PermanentFail(context.Context, Job) (json.RawMessage, error) {
	return nil, domain.Permanent(errors.New("permanent failure requested"))
}
