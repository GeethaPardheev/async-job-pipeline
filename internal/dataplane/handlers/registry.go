// Package handlers defines the pluggable job handler registry and the built-in demo handlers (SPEC §9).
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"github.com/google/uuid"
)

// Job is what a handler sees of the claimed job.
type Job struct {
	ID             uuid.UUID
	Type           string
	Payload        json.RawMessage
	Attempt        int
	MaxRetries     int
	TimeoutSeconds int
}

// Handler executes one attempt. It must honor ctx (deadline = timeout_seconds). Returning an
// error wrapped with domain.Permanent marks the job failed without retry; any other error is
// transient. The result must be valid JSON or nil.
type Handler func(ctx context.Context, job Job) (json.RawMessage, error)

// Registry maps job types to handlers. It is built once at startup and read-only afterwards.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{handlers: make(map[string]Handler)} }

// Register adds a handler; duplicate or empty names are rejected.
func (r *Registry) Register(name string, h Handler) error {
	if name == "" || h == nil {
		return fmt.Errorf("register handler %q: name and handler are required", name)
	}
	if _, dup := r.handlers[name]; dup {
		return fmt.Errorf("register handler %q: already registered", name)
	}
	r.handlers[name] = h
	return nil
}

// Get returns the handler for name.
func (r *Registry) Get(name string) (Handler, bool) {
	h, ok := r.handlers[name]
	return h, ok
}

// Names returns registered types, sorted.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.handlers))
	for n := range r.handlers {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Builtin returns a registry with echo, sleep, flaky, always_fail, permanent_fail and http_webhook.
func Builtin(client *http.Client, userAgent string) (*Registry, error) {
	r := NewRegistry()
	for name, h := range map[string]Handler{
		"echo":           Echo,
		"sleep":          Sleep,
		"flaky":          Flaky,
		"always_fail":    AlwaysFail,
		"permanent_fail": PermanentFail,
		"http_webhook":   NewHTTPWebhook(client, userAgent),
	} {
		if err := r.Register(name, h); err != nil {
			return nil, err
		}
	}
	return r, nil
}
