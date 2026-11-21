package controlplane

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/domain"
)

const (
	maxPayloadBytes      = 256 << 10
	maxIdempotencyKeyLen = 255
	maxScheduleNameLen   = 128
	maxCronExprLen       = 128
)

var reservedKeyPrefixes = []string{"cron:", "dlq-replay:"}

// submitRequest is the POST /v1/jobs body.
type submitRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int            `json:"priority"`
	MaxRetries     *int            `json:"max_retries"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
	RunAt          *string         `json:"run_at"`
}

// validSubmit is a submitRequest with defaults applied and every field checked.
type validSubmit struct {
	Type           string
	Payload        json.RawMessage
	Priority       int
	MaxRetries     int
	TimeoutSeconds int
	RunAt          *time.Time
}

// fieldError is a validation failure; isUnknownType selects the unknown_job_type code.
type fieldError struct {
	msg           string
	isUnknownType bool
}

func (e *fieldError) Error() string { return e.msg }

func invalid(format string, args ...any) error { return &fieldError{msg: fmt.Sprintf(format, args...)} }

func validateType(t string, known map[string]bool) error {
	switch {
	case t == "":
		return invalid("type is required")
	case len(t) > domain.MaxTypeLen:
		return invalid("type must be at most %d characters", domain.MaxTypeLen)
	case !known[t]:
		return &fieldError{msg: fmt.Sprintf("unknown job type %q", t), isUnknownType: true}
	}
	return nil
}

func normalizePayload(p json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(p)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return json.RawMessage(`{}`), nil
	}
	if len(trimmed) > maxPayloadBytes {
		return nil, invalid("payload must be at most %d bytes", maxPayloadBytes)
	}
	return json.RawMessage(trimmed), nil
}

func intField(name string, v *int, def, min, max int) (int, error) {
	if v == nil {
		return def, nil
	}
	if *v < min || *v > max {
		return 0, invalid("%s must be between %d and %d", name, min, max)
	}
	return *v, nil
}

func validateJobOptions(priority, maxRetries, timeout *int) (p, r, t int, err error) {
	if p, err = intField("priority", priority, domain.DefaultPriority, domain.MinPriority, domain.MaxPriority); err != nil {
		return
	}
	if r, err = intField("max_retries", maxRetries, domain.DefaultMaxRetries, 0, domain.MaxMaxRetries); err != nil {
		return
	}
	t, err = intField("timeout_seconds", timeout, domain.DefaultTimeoutSeconds, domain.MinTimeoutSeconds, domain.MaxTimeoutSeconds)
	return
}

func validateSubmit(req submitRequest, known map[string]bool) (validSubmit, error) {
	var v validSubmit
	if err := validateType(req.Type, known); err != nil {
		return v, err
	}
	payload, err := normalizePayload(req.Payload)
	if err != nil {
		return v, err
	}
	p, r, t, err := validateJobOptions(req.Priority, req.MaxRetries, req.TimeoutSeconds)
	if err != nil {
		return v, err
	}
	v = validSubmit{Type: req.Type, Payload: payload, Priority: p, MaxRetries: r, TimeoutSeconds: t}
	if req.RunAt != nil {
		at, err := time.Parse(time.RFC3339, *req.RunAt)
		if err != nil {
			return validSubmit{}, invalid("run_at must be an RFC3339 timestamp (e.g. 2026-11-21T10:00:00Z)")
		}
		at = at.UTC()
		v.RunAt = &at
	}
	return v, nil
}

func validateIdempotencyKey(k string) error {
	if len(k) > maxIdempotencyKeyLen {
		return invalid("Idempotency-Key must be at most %d characters", maxIdempotencyKeyLen)
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x20 || k[i] > 0x7e {
			return invalid("Idempotency-Key must be printable ASCII")
		}
	}
	for _, p := range reservedKeyPrefixes {
		if strings.HasPrefix(k, p) {
			return invalid("Idempotency-Key prefix %q is reserved", p)
		}
	}
	return nil
}

// requestHash fingerprints the normalized submit so an Idempotency-Key reused with a different
// body is detected. Payload key order and whitespace do not matter; numbers keep full precision.
func requestHash(v validSubmit) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(v.Payload))
	dec.UseNumber()
	var canonical any
	if err := dec.Decode(&canonical); err != nil {
		return "", fmt.Errorf("canonicalize payload: %w", err)
	}
	runAt := ""
	if v.RunAt != nil {
		runAt = v.RunAt.Format(time.RFC3339Nano)
	}
	b, err := json.Marshal(struct {
		Type           string `json:"type"`
		Payload        any    `json:"payload"`
		Priority       int    `json:"priority"`
		MaxRetries     int    `json:"max_retries"`
		TimeoutSeconds int    `json:"timeout_seconds"`
		RunAt          string `json:"run_at"`
	}{v.Type, canonical, v.Priority, v.MaxRetries, v.TimeoutSeconds, runAt})
	if err != nil {
		return "", fmt.Errorf("marshal request hash input: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// scheduleRequest is the POST /v1/schedules body.
type scheduleRequest struct {
	Name           *string         `json:"name"`
	CronExpr       string          `json:"cron_expr"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int            `json:"priority"`
	MaxRetries     *int            `json:"max_retries"`
	TimeoutSeconds *int            `json:"timeout_seconds"`
	Enabled        *bool           `json:"enabled"`
}

type validSchedule struct {
	Name           *string
	CronExpr       string
	Type           string
	Payload        json.RawMessage
	Priority       int
	MaxRetries     int
	TimeoutSeconds int
	Enabled        bool
}

func validateSchedule(req scheduleRequest, known map[string]bool) (validSchedule, error) {
	var v validSchedule
	if req.Name != nil && len(*req.Name) > maxScheduleNameLen {
		return v, invalid("name must be at most %d characters", maxScheduleNameLen)
	}
	expr := strings.TrimSpace(req.CronExpr)
	if expr == "" {
		return v, invalid("cron_expr is required")
	}
	if len(expr) > maxCronExprLen {
		return v, invalid("cron_expr must be at most %d characters", maxCronExprLen)
	}
	if _, err := domain.ParseCron(expr); err != nil {
		return v, invalid("%v", err)
	}
	if err := validateType(req.Type, known); err != nil {
		return v, err
	}
	payload, err := normalizePayload(req.Payload)
	if err != nil {
		return v, err
	}
	p, r, t, err := validateJobOptions(req.Priority, req.MaxRetries, req.TimeoutSeconds)
	if err != nil {
		return v, err
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	return validSchedule{Name: req.Name, CronExpr: expr, Type: req.Type, Payload: payload,
		Priority: p, MaxRetries: r, TimeoutSeconds: t, Enabled: enabled}, nil
}

func asFieldError(err error) (*fieldError, bool) {
	var fe *fieldError
	ok := errors.As(err, &fe)
	return fe, ok
}
