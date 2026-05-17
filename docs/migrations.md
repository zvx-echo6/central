# Migration policy

## Migrations are the sole source of truth

The `sql/migrations/` directory contains all schema definitions. There is
no separate schema.sql file; use `pg_dump -s central` to generate a
human-readable snapshot of the current schema when needed.

## Migrations must be idempotent

New migration files (007+) must use guards so they can be safely
re-run without error:

- `CREATE TABLE IF NOT EXISTS ...`
- `CREATE INDEX IF NOT EXISTS ...`
- `INSERT ... ON CONFLICT DO NOTHING` (or `ON CONFLICT ... DO UPDATE`
  where the intent is upsert)
- `ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...`

Migrations 003-006 predate this policy and are grandfathered. Do not
rewrite them.

## All schema changes go through migrate.py

Direct `psql` execution bypasses the `schema_migrations` tracker and
was the cause of the v0.2.0 reconcile. If a migration needs to be
applied on the live system, run:

    sudo -u central /opt/central/.venv/bin/python -m scripts.migrate

Never apply migration SQL directly via `psql`, even as a superuser,
even "just to test." If migrate.py has a bug that's blocking you, fix
migrate.py.
