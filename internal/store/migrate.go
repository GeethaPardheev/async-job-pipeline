package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationLockKey namespaces the advisory lock; the second key is hashtext(schema) so isolated
// test schemas migrate in parallel while replicas sharing a schema serialize.
const migrationLockKey int32 = 727274001

const (
	migrateTimeout = 60 * time.Second
	unlockTimeout  = 5 * time.Second
)

type migration struct {
	version int
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	seen := make(map[int]string, len(entries))
	out := make([]migration, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: name must be NNNN_description.sql", e.Name())
		}
		v, err := strconv.Atoi(prefix)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("migration %q: invalid version prefix", e.Name())
		}
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("migrations %q and %q share version %d", prev, e.Name(), v)
		}
		seen[v] = e.Name()
		body, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", e.Name(), err)
		}
		out = append(out, migration{version: v, name: e.Name(), sql: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// Migrate applies pending embedded migrations. It is safe to call concurrently from any number of
// processes: a session-level advisory lock serializes them and applied versions are skipped.
// It never issues CREATE DATABASE; it only creates objects inside the configured schema.
// Returns the versions applied by this call.
func (s *Store) Migrate(ctx context.Context) (applied []int, err error) {
	migrations, err := loadMigrations()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, migrateTimeout)
	defer cancel()

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1, hashtext($2))", migrationLockKey, s.schema); err != nil {
		return nil, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		uctx, ucancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
		defer ucancel()
		if _, uerr := conn.Exec(uctx, "SELECT pg_advisory_unlock($1, hashtext($2))", migrationLockKey, s.schema); uerr != nil {
			// A session lock outlives Release, so drop the connection to free it server-side.
			if cerr := conn.Conn().Close(uctx); cerr != nil {
				uerr = errors.Join(uerr, fmt.Errorf("close connection holding lock: %w", cerr))
			}
			err = errors.Join(err, fmt.Errorf("release migration lock: %w", uerr))
		}
	}()

	if s.schema != "public" {
		if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{s.schema}.Sanitize()); err != nil {
			return nil, fmt.Errorf("create schema %s: %w", s.schema, err)
		}
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer     PRIMARY KEY,
		name       text        NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}

	done, err := appliedVersions(ctx, conn.Conn())
	if err != nil {
		return nil, err
	}

	for _, m := range migrations {
		if done[m.version] {
			continue
		}
		start := time.Now()
		err := pgx.BeginTxFunc(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.sql); err != nil {
				return fmt.Errorf("execute: %w", err)
			}
			if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", m.version, m.name); err != nil {
				return fmt.Errorf("record version: %w", err)
			}
			return nil
		})
		if err != nil {
			return applied, fmt.Errorf("migration %s: %w", m.name, err)
		}
		applied = append(applied, m.version)
		s.log.Info("migration applied", "migration_version", m.version, "migration", m.name, "schema", s.schema,
			"duration_ms", time.Since(start).Milliseconds())
	}
	if len(applied) == 0 {
		s.log.Info("migrations up to date", "schema", s.schema, "migration_count", len(migrations))
	}
	return applied, nil
}

func appliedVersions(ctx context.Context, conn *pgx.Conn) (map[int]bool, error) {
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		return nil, fmt.Errorf("scan schema_migrations: %w", err)
	}
	done := make(map[int]bool, len(versions))
	for _, v := range versions {
		done[int(v)] = true
	}
	return done, nil
}
