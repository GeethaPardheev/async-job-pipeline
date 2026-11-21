// Package store is the only package that talks to PostgreSQL. It owns the connection pool,
// migrations, and every SQL statement used by the control plane and data plane.
package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Sentinel errors returned by store methods.
var (
	ErrNotFound            = errors.New("not found")
	ErrFenced              = errors.New("fenced: stale attempt rejected")
	ErrConflict            = errors.New("conflict")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	ErrDraining            = errors.New("queue is draining")
)

// Options configures Open.
type Options struct {
	DatabaseURL  string
	Schema       string // "public" or a validated identifier
	MaxConns     int32
	QueryTimeout time.Duration
	AppName      string
}

// Store wraps a pgx pool bound to one schema.
type Store struct {
	pool         *pgxpool.Pool
	schema       string
	queryTimeout time.Duration
	log          *slog.Logger
}

const (
	connectTimeout    = 5 * time.Second
	maxConnIdleTime   = 5 * time.Minute
	healthCheckPeriod = 30 * time.Second
)

// Open parses the DSN (sslmode and other libpq parameters are honored as given), creates the
// pool, and verifies connectivity with a ping.
func Open(ctx context.Context, opts Options, log *slog.Logger) (*Store, error) {
	if opts.Schema == "" {
		opts.Schema = "public"
	}
	cfg, err := pgxpool.ParseConfig(opts.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = opts.MaxConns
	cfg.MinConns = 0
	cfg.MaxConnIdleTime = maxConnIdleTime
	cfg.HealthCheckPeriod = healthCheckPeriod
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = connectTimeout
	}
	if opts.AppName != "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = opts.AppName
	}
	if opts.Schema != "public" {
		cfg.ConnConfig.RuntimeParams["search_path"] = opts.Schema
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	s := &Store{pool: pool, schema: opts.Schema, queryTimeout: opts.QueryTimeout, log: log}

	if err := s.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	log.Info("database connected",
		"host", cfg.ConnConfig.Host, "port", cfg.ConnConfig.Port, "database", cfg.ConnConfig.Database,
		"user", cfg.ConnConfig.User, "tls", cfg.ConnConfig.TLSConfig != nil, "schema", opts.Schema,
		"max_conns", cfg.MaxConns)
	return s, nil
}

// Ping checks connectivity within the store's query timeout.
func (s *Store) Ping(ctx context.Context) error {
	ctx, cancel := s.opCtx(ctx)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}
	return nil
}

// Close releases all pool connections.
func (s *Store) Close() { s.pool.Close() }

// Schema returns the schema this store reads and writes.
func (s *Store) Schema() string { return s.schema }

// Pool exposes the underlying pool for integration-test assertions only.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// opCtx bounds a single store operation by QueryTimeout unless the caller's deadline is sooner.
func (s *Store) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.queryTimeout)
}
