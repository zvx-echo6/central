-- Migration 043: decouple adapter class-identity (kind) from instance-identity (name)
--
-- Until now config.adapters.name served two roles:
--   1. Instance primary key  — the unique name used in runtime state, logs, dedup
--      tables, NATS subjects, etc.
--   2. Registry key          — the key used to look up the adapter class in the
--      supervisor's in-memory registry (discover_adapters returns {class.name: cls}).
--
-- These roles must be split so that one adapter class can later back many
-- operator-created instances (e.g. a generic HTTP adapter).  The new `kind`
-- column holds the class identity (registry key), while `name` remains the
-- unique instance identifier.  All built-in adapters have name == kind, so
-- every existing row is back-filled with kind = name and behaviour is unchanged.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS is safe on re-run.
-- No DEFAULT is set — future INSERT paths must always supply kind explicitly.

ALTER TABLE config.adapters ADD COLUMN IF NOT EXISTS kind TEXT;

-- Back-fill: existing rows get kind = name (class identity == instance identity
-- for all 23 built-in adapters shipped before v0.15.0).
UPDATE config.adapters SET kind = name WHERE kind IS NULL;

-- Enforce non-null going forward.
ALTER TABLE config.adapters ALTER COLUMN kind SET NOT NULL;
