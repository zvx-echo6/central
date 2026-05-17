-- Migration 008: Add config.operators table for user accounts
-- Idempotent per docs/migrations.md

CREATE TABLE IF NOT EXISTS config.operators (
    id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    password_changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
