package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema for TASK-1: only the persistent users table survives offline.
// Everything ephemeral (posts, comments, DMs) lives in Redis (TASK-2+).
const schema = `
CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  username TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS users_username_unique ON users (lower(username));
`

var ErrNoDatabaseURL = errors.New("DATABASE_URL not set")

// Connect opens a pool and ensures the schema. Caller may keep a nil pool
// when the URL is missing; handlers then answer 503 (degraded, like /readyz).
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	if databaseURL == "" {
		return nil, ErrNoDatabaseURL
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 10
	cfg.MinConns = 1
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, err
	}
	// One-time: swap the ignored prototype's users table for ours.
	if err := ReplaceLegacyUsersTable(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// ReplaceLegacyUsersTable drops the pre-Talk2 prototype table (id, name,
// email, password_hash - no username column) and recreates our schema.
// Runs only when the legacy shape is detected; never touches our table.
func ReplaceLegacyUsersTable(ctx context.Context, pool *pgxpool.Pool) error {
	var tableExists bool
	err := pool.QueryRow(ctx,
		`SELECT to_regclass('public.users') IS NOT NULL`).Scan(&tableExists)
	if err != nil {
		return err
	}
	if !tableExists {
		return nil
	}
	var hasUsername int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_schema='public' AND table_name='users' AND column_name='username'`).Scan(&hasUsername)
	if err != nil {
		return err
	}
	if hasUsername > 0 {
		return nil // our table, leave it alone
	}
	_, err = pool.Exec(ctx, `DROP TABLE users`)
	return err
}
