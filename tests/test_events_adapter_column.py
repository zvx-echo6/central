"""Tests for events.adapter column migration (011).

These tests exercise the actual migration SQL against a test database,
verifying backfill logic, FK constraints, NOT NULL enforcement, and
source column removal.

Requires CENTRAL_TEST_DB_DSN or uses default central_test database.
"""

import os
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from central.models import Event, Geo


# Test database DSN - matches test_config_store.py pattern
TEST_DB_DSN = os.environ.get(
    "CENTRAL_TEST_DB_DSN",
    "postgresql://central_test:testpass@localhost/central_test",
)

# Path to migration file
MIGRATION_011_PATH = Path(__file__).parent.parent / "sql" / "migrations" / "011_events_add_adapter_column.sql"


class TestEventModelAdapterField:
    """Test Event model has adapter field (not source)."""

    def test_event_has_adapter_field(self):
        """Event model has adapter field."""
        event = Event(
            id="test-1",
            adapter="nws",
            category="wx.alert.test",
            time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            geo=Geo(),
            data={},
        )
        assert event.adapter == "nws"

    def test_event_model_no_source_field(self):
        """Event model does not have source field."""
        assert "source" not in Event.model_fields
        assert "adapter" in Event.model_fields

    def test_event_adapter_accepts_all_adapter_names(self):
        """Event adapter field accepts all known adapter names."""
        for adapter_name in ["nws", "firms", "usgs_quake"]:
            event = Event(
                id=f"test-{adapter_name}",
                adapter=adapter_name,
                category="test.category",
                time=datetime.now(timezone.utc),
                geo=Geo(),
                data={},
            )
            assert event.adapter == adapter_name


@pytest_asyncio.fixture
async def db_conn() -> asyncpg.Connection:
    """Get a database connection for migration tests."""
    conn = await asyncpg.connect(TEST_DB_DSN)
    yield conn
    await conn.close()


@pytest_asyncio.fixture
async def pre_migration_events_table(db_conn: asyncpg.Connection) -> None:
    """Create events table with pre-migration schema (source column, no adapter).

    Also ensures config.adapters exists with test adapters.
    """
    # Ensure config schema and adapters table exist
    await db_conn.execute("CREATE SCHEMA IF NOT EXISTS config")
    await db_conn.execute("""
        CREATE TABLE IF NOT EXISTS config.adapters (
            name TEXT PRIMARY KEY,
            enabled BOOLEAN NOT NULL DEFAULT true,
            cadence_s INTEGER NOT NULL,
            settings JSONB NOT NULL DEFAULT '{}'::jsonb,
            paused_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)

    # Insert test adapters (idempotent)
    await db_conn.execute("""
        INSERT INTO config.adapters (name, cadence_s)
        VALUES ('nws', 60), ('firms', 300), ('usgs_quake', 60)
        ON CONFLICT (name) DO NOTHING
    """)

    # Drop events table if exists (clean slate)
    await db_conn.execute("DROP TABLE IF EXISTS public.events CASCADE")

    # Create events table with PRE-MIGRATION schema (has source, no adapter)
    await db_conn.execute("""
        CREATE TABLE public.events (
            id              TEXT NOT NULL,
            source          TEXT NOT NULL,
            category        TEXT NOT NULL,
            time            TIMESTAMPTZ NOT NULL,
            expires         TIMESTAMPTZ,
            severity        SMALLINT,
            geom            GEOMETRY(Geometry, 4326),
            regions         TEXT[],
            primary_region  TEXT,
            payload         JSONB NOT NULL,
            received        TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (id, time)
        )
    """)

    # Insert test rows with different source values
    test_rows = [
        ("event-nws-1", "central/adapters/nws", "wx.alert.tornado_warning"),
        ("event-nws-2", "central/adapters/nws", "wx.alert.flood_warning"),
        ("event-firms-1", "central/adapters/firms", "fire.hotspot"),
        ("event-usgs-1", "central/adapters/usgs_quake", "seismic.earthquake"),
    ]

    for event_id, source, category in test_rows:
        await db_conn.execute("""
            INSERT INTO public.events (id, source, category, time, payload)
            VALUES ($1, $2, $3, $4, $5)
        """, event_id, source, category, datetime.now(timezone.utc), "{}")

    yield

    # Cleanup
    await db_conn.execute("DROP TABLE IF EXISTS public.events CASCADE")


@pytest_asyncio.fixture
async def run_migration_011(
    db_conn: asyncpg.Connection,
    pre_migration_events_table: None,
) -> None:
    """Run migration 011 against the pre-migration events table."""
    migration_sql = MIGRATION_011_PATH.read_text()
    await db_conn.execute(migration_sql)
    yield


class TestMigration011Backfill:
    """Test migration 011 backfill logic."""

    @pytest.mark.asyncio
    async def test_backfill_nws_source_to_adapter(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """Backfill converts 'central/adapters/nws' to 'nws'."""
        rows = await db_conn.fetch(
            "SELECT id, adapter FROM public.events WHERE id LIKE 'event-nws-%'"
        )
        assert len(rows) == 2
        for row in rows:
            assert row["adapter"] == "nws"

    @pytest.mark.asyncio
    async def test_backfill_firms_source_to_adapter(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """Backfill converts 'central/adapters/firms' to 'firms'."""
        row = await db_conn.fetchrow(
            "SELECT adapter FROM public.events WHERE id = 'event-firms-1'"
        )
        assert row["adapter"] == "firms"

    @pytest.mark.asyncio
    async def test_backfill_usgs_quake_source_to_adapter(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """Backfill converts 'central/adapters/usgs_quake' to 'usgs_quake'."""
        row = await db_conn.fetchrow(
            "SELECT adapter FROM public.events WHERE id = 'event-usgs-1'"
        )
        assert row["adapter"] == "usgs_quake"

    @pytest.mark.asyncio
    async def test_backfill_all_rows_have_adapter(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """All rows have non-NULL adapter after backfill."""
        count = await db_conn.fetchval(
            "SELECT COUNT(*) FROM public.events WHERE adapter IS NULL"
        )
        assert count == 0


class TestMigration011Constraints:
    """Test migration 011 constraint enforcement."""

    @pytest.mark.asyncio
    async def test_fk_restrict_prevents_adapter_deletion(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """FK RESTRICT prevents deleting adapter with existing events."""
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await db_conn.execute(
                "DELETE FROM config.adapters WHERE name = 'nws'"
            )

    @pytest.mark.asyncio
    async def test_not_null_rejects_null_adapter(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """NOT NULL constraint rejects NULL adapter on insert."""
        with pytest.raises(asyncpg.NotNullViolationError):
            await db_conn.execute("""
                INSERT INTO public.events (id, adapter, category, time, payload)
                VALUES ('null-test', NULL, 'test', now(), '{}')
            """)

    @pytest.mark.asyncio
    async def test_fk_rejects_unknown_adapter(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """FK constraint rejects unknown adapter values."""
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await db_conn.execute("""
                INSERT INTO public.events (id, adapter, category, time, payload)
                VALUES ('bad-adapter', 'nonexistent_adapter', 'test', now(), '{}')
            """)


class TestMigration011SchemaChanges:
    """Test migration 011 schema changes."""

    @pytest.mark.asyncio
    async def test_source_column_dropped(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """Source column no longer exists after migration."""
        row = await db_conn.fetchrow("""
            SELECT COUNT(*) as count
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'events'
              AND column_name = 'source'
        """)
        assert row["count"] == 0

    @pytest.mark.asyncio
    async def test_adapter_column_exists(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """Adapter column exists after migration."""
        row = await db_conn.fetchrow("""
            SELECT COUNT(*) as count
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'events'
              AND column_name = 'adapter'
        """)
        assert row["count"] == 1

    @pytest.mark.asyncio
    async def test_adapter_column_is_not_null(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """Adapter column has NOT NULL constraint."""
        row = await db_conn.fetchrow("""
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'events'
              AND column_name = 'adapter'
        """)
        assert row["is_nullable"] == "NO"

    @pytest.mark.asyncio
    async def test_adapter_fk_constraint_exists(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """FK constraint events_adapter_fkey exists."""
        row = await db_conn.fetchrow("""
            SELECT COUNT(*) as count
            FROM information_schema.table_constraints
            WHERE table_schema = 'public'
              AND table_name = 'events'
              AND constraint_name = 'events_adapter_fkey'
              AND constraint_type = 'FOREIGN KEY'
        """)
        assert row["count"] == 1

    @pytest.mark.asyncio
    async def test_adapter_received_index_exists(
        self, db_conn: asyncpg.Connection, run_migration_011: None
    ):
        """Index events_adapter_received_idx exists."""
        row = await db_conn.fetchrow("""
            SELECT COUNT(*) as count
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'events'
              AND indexname = 'events_adapter_received_idx'
        """)
        assert row["count"] == 1
