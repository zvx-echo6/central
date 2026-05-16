"""Simple database migration runner.

Tracks applied migrations in a `schema_migrations` table. Migrations are
plain SQL files in `sql/migrations/` named with numeric prefixes:
    001_create_config_schema.sql
    002_add_operators_table.sql
    ...

Usage:
    central-migrate [--dry-run]
"""

import argparse
import asyncio
import sys
from pathlib import Path

import asyncpg

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "sql" / "migrations"


async def ensure_migrations_table(conn: asyncpg.Connection) -> None:
    """Create the schema_migrations table if it doesn't exist."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


async def get_applied_migrations(conn: asyncpg.Connection) -> set[str]:
    """Return set of already-applied migration versions."""
    rows = await conn.fetch("SELECT version FROM schema_migrations")
    return {row["version"] for row in rows}


def discover_migrations(migrations_dir: Path) -> list[tuple[str, Path]]:
    """Find all .sql files in migrations directory, sorted by name.

    Returns list of (version, path) tuples where version is the filename
    without extension.
    """
    if not migrations_dir.exists():
        return []

    migrations = []
    for f in sorted(migrations_dir.glob("*.sql")):
        version = f.stem  # e.g., "001_create_config_schema"
        migrations.append((version, f))
    return migrations


async def apply_migration(
    conn: asyncpg.Connection, version: str, sql_path: Path, dry_run: bool = False
) -> None:
    """Apply a single migration."""
    sql = sql_path.read_text()

    if dry_run:
        print(f"[DRY RUN] Would apply: {version}")
        print(f"  SQL: {sql[:200]}..." if len(sql) > 200 else f"  SQL: {sql}")
        return

    async with conn.transaction():
        await conn.execute(sql)
        await conn.execute(
            "INSERT INTO schema_migrations (version) VALUES ($1)", version
        )
    print(f"Applied: {version}")


async def run_migrations(dsn: str, dry_run: bool = False) -> int:
    """Run all pending migrations.

    Returns number of migrations applied.
    """
    conn = await asyncpg.connect(dsn)
    try:
        await ensure_migrations_table(conn)
        applied = await get_applied_migrations(conn)
        pending = [
            (v, p) for v, p in discover_migrations(MIGRATIONS_DIR) if v not in applied
        ]

        if not pending:
            print("No pending migrations.")
            return 0

        print(f"Found {len(pending)} pending migration(s).")
        for version, path in pending:
            await apply_migration(conn, version, path, dry_run)

        return len(pending)
    finally:
        await conn.close()


async def async_main() -> None:
    """Async entry point."""
    parser = argparse.ArgumentParser(description="Run database migrations")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be applied without executing",
    )
    args = parser.parse_args()

    from central.bootstrap_config import get_settings

    settings = get_settings()
    count = await run_migrations(settings.db_dsn, dry_run=args.dry_run)

    if count > 0 and not args.dry_run:
        print(f"Successfully applied {count} migration(s).")


def main() -> None:
    """Entry point."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
