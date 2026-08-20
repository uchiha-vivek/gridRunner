package store

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/openmic/forgerun/migrations"
)

// Migrations are plain SQL files embedded in the binary. Each `NNNN_name.sql` may
// have a matching `NNNN_name.down.sql` that undoes it. A full migration tool is
// overkill here: the mechanism is one table and a hundred lines, and it runs
// automatically when the API starts.

// Migrate applies every pending .sql file exactly once, in filename order.
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.ensureMigrationsTable(ctx); err != nil {
		return err
	}
	files, err := upMigrations()
	if err != nil {
		return err
	}
	for _, name := range files {
		var applied bool
		if err := s.db.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return err
		}
		if err := s.runMigration(ctx, name, string(body), true); err != nil {
			return err
		}
	}
	return nil
}

// Rollback undoes the last `steps` applied migrations, newest first. It exists so
// a bad deploy can be reversed without hand-written SQL; nothing calls it
// automatically, because rolling back a schema always drops data.
func (s *Store) Rollback(ctx context.Context, steps int) error {
	if err := s.ensureMigrationsTable(ctx); err != nil {
		return err
	}
	rows, err := s.db.Query(ctx,
		`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT $1`, steps)
	if err != nil {
		return err
	}
	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		versions = append(versions, v)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, version := range versions {
		down := strings.TrimSuffix(version, ".sql") + ".down.sql"
		body, err := migrations.FS.ReadFile(down)
		if err != nil {
			return fmt.Errorf("no down migration for %s: %w", version, err)
		}
		if err := s.runMigration(ctx, version, string(body), false); err != nil {
			return err
		}
	}
	return nil
}

// AppliedMigrations lists what the database currently has, newest first.
func (s *Store) AppliedMigrations(ctx context.Context) ([]string, error) {
	if err := s.ensureMigrationsTable(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) ensureMigrationsTable(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return nil
}

// runMigration applies one file and records (or removes) the bookkeeping row in
// the same transaction, so the schema and schema_migrations can never disagree.
func (s *Store) runMigration(ctx context.Context, version, body string, up bool) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, body); err != nil {
		direction := "migration"
		if !up {
			direction = "rollback"
		}
		return fmt.Errorf("apply %s %s: %w", direction, version, err)
	}
	if up {
		_, err = tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version)
	} else {
		_, err = tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, version)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// upMigrations returns the forward migrations in filename order. The `.down.sql`
// files live in the same directory and must never be applied as one.
func upMigrations() ([]string, error) {
	files, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return nil, err
	}
	up := files[:0:0]
	for _, f := range files {
		if !strings.HasSuffix(f, ".down.sql") {
			up = append(up, f)
		}
	}
	sort.Strings(up)
	return up, nil
}
