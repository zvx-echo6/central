-- Migration 009: Add config.sessions table for auth tokens
-- Idempotent per docs/migrations.md

CREATE TABLE IF NOT EXISTS config.sessions (
    token TEXT PRIMARY KEY,
    operator_id BIGINT NOT NULL REFERENCES config.operators(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS sessions_expires_at_idx ON config.sessions(expires_at);
