// Package config loads and validates process configuration from environment variables (SPEC §11).
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Mode selects which plane the binary runs.
type Mode string

const (
	ModeAPI     Mode = "api"
	ModeWorker  Mode = "worker"
	ModeMigrate Mode = "migrate"
)

// ParseMode validates a mode string.
func ParseMode(s string) (Mode, error) {
	switch m := Mode(strings.ToLower(strings.TrimSpace(s))); m {
	case ModeAPI, ModeWorker, ModeMigrate:
		return m, nil
	case "":
		return "", errors.New("mode is required: pass 'api', 'worker' or 'migrate' as the first argument or set MODE")
	default:
		return "", fmt.Errorf("invalid mode %q: want api, worker or migrate", s)
	}
}

// Config is the fully validated runtime configuration.
type Config struct {
	Mode Mode

	DatabaseURL     string
	DBSchema        string
	DBMaxConns      int32
	DBQueryTimeout  time.Duration
	MigrateOnStart  bool
	ShutdownTimeout time.Duration
	LogLevel        slog.Level

	Port             int
	APIToken         string
	WorkerStaleAfter time.Duration
	StatsWindow      time.Duration

	WorkerID          string
	WorkerConcurrency int
	ClaimBatchSize    int
	PollInterval      time.Duration
	LeaseGrace        time.Duration
	ReaperInterval    time.Duration
	ReaperBatch       int
	HeartbeatInterval time.Duration
	CronTickInterval  time.Duration
	BackoffBase       time.Duration
	BackoffCap        time.Duration
}

const (
	defaultAPIMaxConns    = 8
	defaultWorkerMaxConns = 5
)

var schemaRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// ValidSchemaName reports whether s is an identifier we allow as DB_SCHEMA.
func ValidSchemaName(s string) bool { return schemaRe.MatchString(s) }

// Load reads configuration for mode using getenv (os.Getenv in production). All invalid
// variables are reported together.
func Load(mode Mode, getenv func(string) string) (Config, error) {
	p := parser{getenv: getenv}
	defaultConns := defaultAPIMaxConns
	if mode == ModeWorker {
		defaultConns = defaultWorkerMaxConns
	}

	c := Config{
		Mode:            mode,
		DatabaseURL:     strings.TrimSpace(getenv("DATABASE_URL")),
		DBSchema:        p.str("DB_SCHEMA", "public"),
		DBMaxConns:      int32(p.intRange("DB_MAX_CONNS", defaultConns, 1, 100)),
		DBQueryTimeout:  p.dur("DB_QUERY_TIMEOUT", 5*time.Second),
		MigrateOnStart:  p.boolean("MIGRATE_ON_START", true),
		ShutdownTimeout: p.dur("SHUTDOWN_TIMEOUT", 25*time.Second),
		LogLevel:        p.level("LOG_LEVEL", slog.LevelInfo),

		Port:             p.intRange("PORT", 8080, 1, 65535),
		APIToken:         strings.TrimSpace(getenv("API_TOKEN")),
		WorkerStaleAfter: p.dur("WORKER_STALE_AFTER", 15*time.Second),
		StatsWindow:      p.dur("STATS_WINDOW", 5*time.Minute),

		WorkerID:          p.str("WORKER_ID", ""),
		WorkerConcurrency: p.intRange("WORKER_CONCURRENCY", 8, 1, 1024),
		ClaimBatchSize:    p.intRange("CLAIM_BATCH_SIZE", 8, 1, 1000),
		PollInterval:      p.dur("POLL_INTERVAL", 500*time.Millisecond),
		LeaseGrace:        p.dur("LEASE_GRACE", 30*time.Second),
		ReaperInterval:    p.dur("REAPER_INTERVAL", 10*time.Second),
		ReaperBatch:       p.intRange("REAPER_BATCH", 100, 1, 10000),
		HeartbeatInterval: p.dur("HEARTBEAT_INTERVAL", 5*time.Second),
		CronTickInterval:  p.dur("CRON_TICK_INTERVAL", time.Second),
		BackoffBase:       p.dur("BACKOFF_BASE", time.Second),
		BackoffCap:        p.dur("BACKOFF_CAP", 5*time.Minute),
	}

	if c.DatabaseURL == "" {
		p.fail("DATABASE_URL is required")
	}
	if !ValidSchemaName(c.DBSchema) {
		p.fail(fmt.Sprintf("DB_SCHEMA %q must match %s", c.DBSchema, schemaRe))
	}
	if c.BackoffCap < c.BackoffBase {
		p.fail(fmt.Sprintf("BACKOFF_CAP (%s) must be >= BACKOFF_BASE (%s)", c.BackoffCap, c.BackoffBase))
	}
	if c.WorkerStaleAfter <= c.HeartbeatInterval {
		p.fail(fmt.Sprintf("WORKER_STALE_AFTER (%s) must be > HEARTBEAT_INTERVAL (%s)", c.WorkerStaleAfter, c.HeartbeatInterval))
	}
	if mode == ModeWorker && c.WorkerID == "" {
		id, err := defaultWorkerID()
		if err != nil {
			p.fail(fmt.Sprintf("derive WORKER_ID: %v", err))
		}
		c.WorkerID = id
	}

	if err := errors.Join(p.errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return c, nil
}

func defaultWorkerID() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("hostname: %w", err)
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("random suffix: %w", err)
	}
	return host + "-" + hex.EncodeToString(b[:]), nil
}

type parser struct {
	getenv func(string) string
	errs   []error
}

func (p *parser) fail(msg string) { p.errs = append(p.errs, errors.New(msg)) }

func (p *parser) str(key, def string) string {
	if v := strings.TrimSpace(p.getenv(key)); v != "" {
		return v
	}
	return def
}

func (p *parser) intRange(key string, def, min, max int) int {
	v := strings.TrimSpace(p.getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.fail(fmt.Sprintf("%s=%q is not an integer", key, v))
		return def
	}
	if n < min || n > max {
		p.fail(fmt.Sprintf("%s=%d must be between %d and %d", key, n, min, max))
		return def
	}
	return n
}

func (p *parser) dur(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(p.getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail(fmt.Sprintf("%s=%q is not a duration (e.g. 500ms, 5s)", key, v))
		return def
	}
	if d <= 0 {
		p.fail(fmt.Sprintf("%s=%q must be positive", key, v))
		return def
	}
	return d
}

func (p *parser) boolean(key string, def bool) bool {
	v := strings.TrimSpace(p.getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.fail(fmt.Sprintf("%s=%q is not a boolean", key, v))
		return def
	}
	return b
}

func (p *parser) level(key string, def slog.Level) slog.Level {
	v := strings.TrimSpace(p.getenv(key))
	if v == "" {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		p.fail(fmt.Sprintf("%s=%q must be debug, info, warn or error", key, v))
		return def
	}
	return l
}
