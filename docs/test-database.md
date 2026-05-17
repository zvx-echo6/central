# Test Database Setup

Central's integration tests require a PostgreSQL database. This document
covers one-time setup and maintenance of the test database.

## DSN Convention

Tests default to:

```
postgresql://central_test:testpass@localhost/central_test
```

Override via the `CENTRAL_TEST_DB_DSN` environment variable:

```bash
export CENTRAL_TEST_DB_DSN="postgresql://myuser:mypass@localhost/mydb"
```

## One-Time Setup

Run these commands once on a fresh PostgreSQL installation:

```bash
# Create the test user (as postgres superuser)
sudo -u postgres createuser -s central_test
sudo -u postgres psql -c "ALTER USER central_test PASSWORD 'testpass'"

# Create the test database
sudo -u postgres createdb -O central_test central_test

# Install required extensions
sudo -u postgres psql central_test -c "CREATE EXTENSION IF NOT EXISTS postgis"
```

**Note:** The `central_test` role is created as a superuser (`-s` flag).
This allows test fixtures to self-bootstrap extensions like PostGIS via
`CREATE EXTENSION IF NOT EXISTS`. Production uses a non-superuser role.

## Required Extensions

| Extension | Version | Purpose |
|-----------|---------|---------|
| postgis | 3.4+ | Geometry types for geospatial event data |

## Why PostGIS Is Not in Migrations

PostGIS requires superuser privileges to install. The production `central`
role is intentionally not a superuser for security reasons. Therefore:

- **Production:** A DBA must run `CREATE EXTENSION postgis` before the
  first `migrate.py` run. This is a one-time bootstrap step.
- **Test:** The `central_test` role is a superuser, so test fixtures can
  self-bootstrap PostGIS via `CREATE EXTENSION IF NOT EXISTS`.

This divergence is documented rather than "fixed" because granting
superuser to production roles creates security risk, and the PostgreSQL
packaging on Ubuntu does not mark PostGIS as a trusted extension.

## Resetting the Test Database

If the test database gets into a bad state:

```bash
# Drop and recreate
sudo -u postgres dropdb central_test
sudo -u postgres createdb -O central_test central_test
sudo -u postgres psql central_test -c "CREATE EXTENSION IF NOT EXISTS postgis"
```

Test fixtures handle their own table creation and cleanup, so this is
rarely needed.

## Running Tests

```bash
cd /opt/central
uv run pytest tests/              # all tests
uv run pytest tests/test_config_store.py -v  # specific file
```

Tests that require the database will skip gracefully if the connection
fails, though most integration tests will fail without a working DB.
