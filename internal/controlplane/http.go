package controlplane

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

const maxBodyBytes = 1 << 20

type ctxKey int

const requestIDKey ctxKey = iota

// Error codes (SPEC §8).
const (
	codeInvalidRequest         = "invalid_request"
	codeUnknownJobType         = "unknown_job_type"
	codeUnauthorized           = "unauthorized"
	codeNotFound               = "not_found"
	codeConflict               = "conflict"
	codeIdempotencyKeyConflict = "idempotency_key_conflict"
	codeQueueDraining          = "queue_draining"
	codeUnavailable            = "unavailable"
	codeInternal               = "internal"
)

const drainRetryAfterSeconds = "30"

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, r *http.Request, log *slog.Logger, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Warn("write response failed", "err", err, "request_id", requestIDFrom(r.Context()))
	}
}

func writeError(w http.ResponseWriter, r *http.Request, log *slog.Logger, status int, code, msg string) {
	writeJSON(w, r, log, status, errorBody{Error: errorDetail{Code: code, Message: msg}})
}

// writeStoreError is the single mapping from store sentinel errors to HTTP responses.
func (s *Server) writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, r, s.log, http.StatusNotFound, codeNotFound, "not found")
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeError(w, r, s.log, http.StatusConflict, codeIdempotencyKeyConflict,
			"Idempotency-Key was already used with a different request body")
	case errors.Is(err, store.ErrConflict):
		writeError(w, r, s.log, http.StatusConflict, codeConflict, "conflict")
	case errors.Is(err, store.ErrDraining):
		w.Header().Set("Retry-After", drainRetryAfterSeconds)
		writeError(w, r, s.log, http.StatusServiceUnavailable, codeQueueDraining, "queue is draining; new jobs are not accepted")
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, new(*pgconn.ConnectError)):
		s.log.Warn("database unavailable", "err", err, "method", r.Method, "path", r.URL.Path, "request_id", requestIDFrom(r.Context()))
		writeError(w, r, s.log, http.StatusServiceUnavailable, codeUnavailable, "database unavailable; retry later")
	default:
		s.log.Error("request failed", "err", err, "method", r.Method, "path", r.URL.Path, "request_id", requestIDFrom(r.Context()))
		writeError(w, r, s.log, http.StatusInternalServerError, codeInternal, "internal error")
	}
}

// decodeJSON strictly decodes a single JSON object: unknown fields, trailing data and bodies over
// the 1 MiB limit are rejected. An empty body is allowed only when allowEmpty is set.
func decodeJSON(r *http.Request, v any, allowEmpty bool) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return fmt.Errorf("request body exceeds %d bytes", mbe.Limit)
		}
		return fmt.Errorf("read request body: %w", err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		if allowEmpty {
			return nil
		}
		return errors.New("request body is required")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if dec.More() {
		return errors.New("invalid JSON body: unexpected data after the JSON object")
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("invalid JSON body: unexpected data after the JSON object")
	}
	return nil
}

func parseUUIDPath(r *http.Request, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		return uuid.Nil, fmt.Errorf("%s must be a UUID", name)
	}
	return id, nil
}

const (
	defaultListLimit = 50
	maxListLimit     = 500
)

func parseLimit(r *http.Request) (int, error) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return defaultListLimit, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > maxListLimit {
		return 0, fmt.Errorf("limit must be an integer between 1 and %d", maxListLimit)
	}
	return n, nil
}

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

func requestID(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-ID"))
		if id == "" || len(id) > 128 {
			u, err := uuid.NewRandom()
			if err != nil {
				log.Warn("generate request id failed", "err", err)
				id = strconv.FormatInt(time.Now().UnixNano(), 36)
			} else {
				id = u.String()
			}
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

func recoverPanics(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				log.Error("panic in http handler", "panic", fmt.Sprint(rec), "stack", string(debug.Stack()),
					"request_id", requestIDFrom(r.Context()))
				writeError(w, r, log, http.StatusInternalServerError, codeInternal, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func accessLog(log *slog.Logger, mux *http.ServeMux, reqs *requestCounter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		_, route := mux.Handler(r)
		if _, path, hasMethod := strings.Cut(route, " "); hasMethod {
			route = path
		}
		if route == "" {
			route = "unmatched"
		}
		reqs.inc(r.Method, route, rec.status)
		log.Info("http request", "request_id", requestIDFrom(r.Context()), "method", r.Method, "path", r.URL.Path,
			"route", route, "status", rec.status, "latency_ms", time.Since(start).Milliseconds())
	})
}

// requireToken enforces bearer auth on /v1/* only, so /healthz, /readyz and /metrics stay
// reachable by platform health checks and scrapers.
func requireToken(log *slog.Logger, token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") || r.URL.Path == "/v1" {
			got := []byte(r.Header.Get("Authorization"))
			if subtle.ConstantTimeCompare(got, want) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="jobq"`)
				writeError(w, r, log, http.StatusUnauthorized, codeUnauthorized, "missing or invalid bearer token")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

type requestKey struct {
	method, route string
	code          int
}

// requestCounter backs jobq_http_requests_total.
type requestCounter struct {
	mu     sync.Mutex
	counts map[requestKey]uint64
}

func newRequestCounter() *requestCounter { return &requestCounter{counts: make(map[requestKey]uint64)} }

func (c *requestCounter) inc(method, route string, code int) {
	c.mu.Lock()
	c.counts[requestKey{method, route, code}]++
	c.mu.Unlock()
}

type requestCount struct {
	requestKey
	n uint64
}

func (c *requestCounter) snapshot() []requestCount {
	c.mu.Lock()
	out := make([]requestCount, 0, len(c.counts))
	for k, n := range c.counts {
		out = append(out, requestCount{k, n})
	}
	c.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.route != b.route {
			return a.route < b.route
		}
		if a.method != b.method {
			return a.method < b.method
		}
		return a.code < b.code
	})
	return out
}
