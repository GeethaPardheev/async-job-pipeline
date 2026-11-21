package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestParseMode(t *testing.T) {
	for in, want := range map[string]Mode{"api": ModeAPI, " Worker ": ModeWorker, "migrate": ModeMigrate} {
		got, err := ParseMode(in)
		if err != nil || got != want {
			t.Fatalf("ParseMode(%q) = %q, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "server", "apiworker"} {
		if _, err := ParseMode(bad); err == nil {
			t.Fatalf("ParseMode(%q) expected error", bad)
		}
	}
}

func TestLoadDefaults(t *testing.T) {
	base := map[string]string{"DATABASE_URL": "postgres://u:p@h/db?sslmode=require"}

	api, err := Load(ModeAPI, env(base))
	if err != nil {
		t.Fatalf("Load api: %v", err)
	}
	if api.DBMaxConns != 8 || api.DBSchema != "public" || api.Port != 8080 || !api.MigrateOnStart ||
		api.LogLevel != slog.LevelInfo || api.APIToken != "" || api.WorkerID != "" {
		t.Fatalf("unexpected api defaults: %+v", api)
	}

	w, err := Load(ModeWorker, env(base))
	if err != nil {
		t.Fatalf("Load worker: %v", err)
	}
	if w.DBMaxConns != 5 || w.WorkerConcurrency != 8 || w.HeartbeatInterval != 5*time.Second ||
		w.LeaseGrace != 30*time.Second || w.BackoffBase != time.Second || w.BackoffCap != 5*time.Minute {
		t.Fatalf("unexpected worker defaults: %+v", w)
	}
	if w.WorkerID == "" || !strings.Contains(w.WorkerID, "-") {
		t.Fatalf("worker id not derived: %q", w.WorkerID)
	}
}

func TestLoadOverrides(t *testing.T) {
	c, err := Load(ModeWorker, env(map[string]string{
		"DATABASE_URL":       "postgres://x",
		"DB_SCHEMA":          "t_abc123",
		"DB_MAX_CONNS":       "3",
		"WORKER_ID":          "w-1",
		"POLL_INTERVAL":      "20ms",
		"API_TOKEN":          " secret ",
		"LOG_LEVEL":          "debug",
		"MIGRATE_ON_START":   "false",
		"WORKER_CONCURRENCY": "2",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DBSchema != "t_abc123" || c.DBMaxConns != 3 || c.WorkerID != "w-1" || c.PollInterval != 20*time.Millisecond ||
		c.APIToken != "secret" || c.LogLevel != slog.LevelDebug || c.MigrateOnStart || c.WorkerConcurrency != 2 {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestLoadErrorsAreAggregated(t *testing.T) {
	_, err := Load(ModeAPI, env(map[string]string{
		"DB_SCHEMA":    "Bad-Schema",
		"DB_MAX_CONNS": "zero",
		"PORT":         "70000",
		"BACKOFF_BASE": "10s",
		"BACKOFF_CAP":  "1s",
		"LOG_LEVEL":    "loud",
		"STATS_WINDOW": "-1s",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL", "DB_SCHEMA", "DB_MAX_CONNS", "PORT", "BACKOFF_CAP", "LOG_LEVEL", "STATS_WINDOW"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %s: %v", want, err)
		}
	}
}

func TestValidSchemaName(t *testing.T) {
	for _, ok := range []string{"public", "jobq", "t_0af3", "_x"} {
		if !ValidSchemaName(ok) {
			t.Errorf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "Public", "1abc", "a-b", "a;drop", strings.Repeat("a", 64)} {
		if ValidSchemaName(bad) {
			t.Errorf("%q should be invalid", bad)
		}
	}
}
