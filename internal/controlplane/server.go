// Package controlplane implements the HTTP API (SPEC §8).
package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// Pinger is the readiness dependency.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Config holds API-process settings.
type Config struct {
	Port     int
	APIToken string
	Version  string
}

// Server is the control-plane HTTP server.
type Server struct {
	cfg   Config
	db    Pinger
	log   *slog.Logger
	mux   *http.ServeMux
	ready atomic.Bool
}

const (
	readyzTimeout   = 2 * time.Second
	shutdownTimeout = 15 * time.Second
)

// New builds the server and registers routes.
func New(cfg Config, db Pinger, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, db: db, log: log, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
}

// Handler returns the full middleware chain around the router.
func (s *Server) Handler() http.Handler {
	var h http.Handler = s.mux
	h = limitBody(h)
	h = requireToken(s.log, s.cfg.APIToken, h)
	h = accessLog(s.log, s.mux, h)
	h = recoverPanics(s.log, h)
	h = requestID(s.log, h)
	return h
}

// Run listens on cfg.Port until ctx is cancelled, then drains in-flight requests.
func (s *Server) Run(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", s.cfg.Port))
	if err != nil {
		return fmt.Errorf("listen on port %d: %w", s.cfg.Port, err)
	}
	return s.Serve(ctx, ln)
}

// Serve runs the server on ln (tests pass a :0 listener).
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	s.ready.Store(true)
	s.log.Info("api listening", "addr", ln.Addr().String(), "auth", s.cfg.APIToken != "")

	select {
	case err := <-errCh:
		s.ready.Store(false)
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	s.ready.Store(false)
	s.log.Info("api shutting down", "timeout", shutdownTimeout.String())
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, s.log, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, r, s.log, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "error": "server is not accepting traffic"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		s.log.Warn("readiness check failed", "err", err)
		writeJSON(w, r, s.log, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "error": "database unreachable"})
		return
	}
	writeJSON(w, r, s.log, http.StatusOK, map[string]string{"status": "ready"})
}
