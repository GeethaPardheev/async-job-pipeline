package controlplane

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const maxBodyBytes = 1 << 20

type ctxKey int

const requestIDKey ctxKey = iota

// Error codes (SPEC §8).
const (
	codeUnauthorized = "unauthorized"
	codeInternal     = "internal"
)

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

func accessLog(log *slog.Logger, mux *http.ServeMux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		_, route := mux.Handler(r)
		log.Info("http request", "method", r.Method, "path", r.URL.Path, "route", route, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(), "request_id", requestIDFrom(r.Context()))
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
