package db

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("db: parse config: %w", err)
	}
	config.MinConns = 16
	config.MaxConns = 128

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}
	return pool, nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool, migrationFS fs.FS) error {
	// Acquire advisory lock to prevent concurrent migration runs (blue-green, fast restart).
	// Lock ID 0x57_4A_4D_49_47 = "STMIG" in ASCII-ish hex.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("db: acquire conn for migration lock: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(5523798343)`); err != nil {
		return fmt.Errorf("db: advisory lock: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(5523798343)`) //nolint:errcheck // best-effort: release advisory lock when Migrate returns

	// Ensure schema_version table exists
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_version (
			version INTEGER NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	if err != nil {
		return fmt.Errorf("db: create schema_version: %w", err)
	}

	var current int
	err = pool.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&current)
	if err != nil {
		return fmt.Errorf("db: read schema_version: %w", err)
	}

	entries, err := fs.ReadDir(migrationFS, ".")
	if err != nil {
		return fmt.Errorf("db: read migrations dir: %w", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}

		// Extract version number from filename: "001_initial.sql" -> 1
		var version int
		if _, err := fmt.Sscanf(name, "%03d_", &version); err != nil {
			continue
		}

		if version <= current {
			continue
		}

		sql, err := fs.ReadFile(migrationFS, name)
		if err != nil {
			return fmt.Errorf("db: read migration %s: %w", name, err)
		}

		slog.Info("applying migration", "file", name, "version", version)

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("db: begin tx for migration %s: %w", name, err)
		}

		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			tx.Rollback(ctx) //nolint:errcheck // best-effort: release after failed commit
			return fmt.Errorf("db: apply migration %s: %w", name, err)
		}

		if _, err := tx.Exec(ctx, `INSERT INTO schema_version (version) VALUES ($1)`, version); err != nil {
			tx.Rollback(ctx) //nolint:errcheck // best-effort: release after failed commit
			return fmt.Errorf("db: record migration %s: %w", name, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("db: commit migration %s: %w", name, err)
		}
	}

	return nil
}
