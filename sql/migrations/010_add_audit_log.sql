-- Migration 010: Add config.audit_log table
-- Idempotent per docs/migrations.md

CREATE TABLE IF NOT EXISTS config.audit_log (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    operator_id BIGINT REFERENCES config.operators(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    target TEXT,
    before JSONB,
    after JSONB
);

CREATE INDEX IF NOT EXISTS audit_log_ts_idx ON config.audit_log(ts DESC);
CREATE INDEX IF NOT EXISTS audit_log_action_idx ON config.audit_log(action);
