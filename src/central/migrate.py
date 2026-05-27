"""Simple database migration runner.

Tracks applied migrations in a `schema_migrations` table. Migrations are
plain SQL files in `sql/migrations/` named with numeric prefixes:
    001_create_config_schema.sql
    002_add_operators_table.sql
    ...

Usage:
    central-migrate [--dry-run]
    central-migrate --check     # report drift; exit 1 if any file is untracked
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import asyncpg

logger = logging.getLogger(__name__)

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


def find_drift(
    applied: set[str], discovered: list[tuple[str, Path]]
) -> tuple[list[str], list[str]]:
    """Compare schema_migrations rows against migration files on disk.

    Returns (untracked, orphan):
      - untracked: migration files present on disk with NO schema_migrations row.
        This is the v0.9.18 failure mode -- migrations applied out-of-band (direct
        psql) that were never recorded, so a fresh restore would try to replay
        them and an audit of the tracking table understates what is live.
      - orphan: schema_migrations versions with NO matching .sql file (e.g. a
        migration file removed from the repo).

    Pure function (no I/O) so the drift logic is unit-testable without a database.
    """
    disk_versions = {v for v, _ in discovered}
    untracked = sorted(v for v in disk_versions if v not in applied)
    orphan = sorted(v for v in applied if v not in disk_versions)
    return untracked, orphan


def log_drift(untracked: list[str], orphan: list[str]) -> None:
    """Emit a WARN per drifted migration so divergence is visible in logs.

    Cheap insurance against drift compounding silently (v0.9.18): any file not
    recorded in schema_migrations, or any row with no file, is surfaced on every
    central-migrate run.
    """
    for version in untracked:
        logger.warning(
            "migration file %s is not recorded in schema_migrations "
            "(pending apply, or applied out-of-band -- investigate)",
            version,
        )
    for version in orphan:
        logger.warning(
            "schema_migrations records %s but no matching .sql file exists on disk",
            version,
        )


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
        discovered = discover_migrations(MIGRATIONS_DIR)

        untracked, orphan = find_drift(applied, discovered)
        log_drift(untracked, orphan)

        pending = [(v, p) for v, p in discovered if v not in applied]

        if not pending:
            print("No pending migrations.")
            return 0

        print(f"Found {len(pending)} pending migration(s).")
        for version, path in pending:
            await apply_migration(conn, version, path, dry_run)

        return len(pending)
    finally:
        await conn.close()


async def check_drift(dsn: str) -> int:
    """Report migration drift; return 1 if any file is untracked, else 0.

    The CI-assertion form of log_drift: run against a DB expected to be fully
    reconciled (after a restore, or in CI). A non-zero exit means a migration
    file was never recorded -- the v0.9.18 drift, made loud and scriptable.
    """
    conn = await asyncpg.connect(dsn)
    try:
        await ensure_migrations_table(conn)
        applied = await get_applied_migrations(conn)
        untracked, orphan = find_drift(applied, discover_migrations(MIGRATIONS_DIR))
    finally:
        await conn.close()

    for version in untracked:
        print(f"DRIFT: {version} present on disk but not in schema_migrations")
    for version in orphan:
        print(f"DRIFT: {version} in schema_migrations but no .sql file on disk")

    if untracked:
        print(f"{len(untracked)} untracked migration file(s) -- drift detected.")
        return 1
    print("No migration drift detected.")
    return 0


async def async_main() -> None:
    """Async entry point."""
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Run database migrations")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be applied without executing",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Report drift and exit 1 if any migration file is untracked "
        "(does not apply migrations)",
    )
    args = parser.parse_args()

    from central.bootstrap_config import get_settings

    settings = get_settings()

    if args.check:
        sys.exit(await check_drift(settings.db_dsn))

    count = await run_migrations(settings.db_dsn, dry_run=args.dry_run)

    if count > 0 and not args.dry_run:
        print(f"Successfully applied {count} migration(s).")


def main() -> None:
    """Entry point."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
