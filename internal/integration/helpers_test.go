//go:build integration

// Package integration exercises the system against a real PostgreSQL (TEST_DATABASE_URL).
// Every test runs in its own schema so tests are isolated and parallel without CREATE DATABASE.
package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/jackc/pgx/v5"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL must be set in CI")
		}
		t.Skip("TEST_DATABASE_URL not set; skipping integration test")
	}
	return dsn
}

func testLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func newSchemaName(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("random schema suffix: %v", err)
	}
	return "t_" + hex.EncodeToString(b[:])
}

// openStore opens a store bound to schema. It does not migrate.
func openStore(t *testing.T, schema string) *store.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := store.Open(ctx, store.Options{
		DatabaseURL:  testDSN(t),
		Schema:       schema,
		MaxConns:     10,
		QueryTimeout: 5 * time.Second,
		AppName:      "jobq-test",
	}, testLogger())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

// newMigratedStore creates an isolated schema, migrates it, and drops it when the test ends.
func newMigratedStore(t *testing.T) *store.Store {
	t.Helper()
	schema := newSchemaName(t)
	st := openStore(t, schema)
	dropSchemaOnCleanup(t, st, schema)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st
}

func dropSchemaOnCleanup(t *testing.T, st *store.Store, schema string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := st.Pool().Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
	})
}
