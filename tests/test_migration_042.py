"""v0.14.0 migration 042: single config.system bbox -> config.monitoring_areas.

The suite has no live Postgres (it runs identically as zvx or central), so this
asserts the migration's shape statically: it creates the table, enforces the
union-filter invariants, seeds 'default' from the existing config.system bbox to
preserve current bounds, and -- deliberately for v0.14.0 -- does NOT drop the old
columns (that lands in v0.14.1).
"""

from pathlib import Path

_SQL = Path("sql/migrations/042_monitoring_area_to_multi_areas.sql").read_text()
_NORM = " ".join(_SQL.split())  # whitespace-insensitive matching


def test_creates_monitoring_areas_table():
    assert "CREATE TABLE IF NOT EXISTS config.monitoring_areas" in _NORM


def test_name_is_unique():
    assert "name TEXT NOT NULL UNIQUE" in _NORM


def test_check_constraints_enforce_bbox_ordering():
    assert "CHECK (north > south)" in _NORM
    assert "CHECK (east > west)" in _NORM


def test_seeds_default_from_config_system_preserving_bounds():
    assert "INSERT INTO config.monitoring_areas" in _NORM
    assert "FROM config.system" in _NORM
    assert "'default'" in _NORM
    # Idempotent re-run / already-seeded installs must not error or duplicate.
    assert "ON CONFLICT (name) DO NOTHING" in _NORM


def test_does_not_drop_old_columns_in_v0_14_0():
    upper = _NORM.upper()
    assert "DROP COLUMN" not in upper
    assert "DROP TABLE" not in upper


def test_grants_table_and_sequence_ownership_to_central():
    # v0.14.5: applied-as-postgres left the table/sequence postgres-owned; the
    # file now re-asserts central ownership so fresh installs are self-healing.
    assert "ALTER TABLE config.monitoring_areas OWNER TO central" in _NORM
    assert "ALTER SEQUENCE config.monitoring_areas_id_seq OWNER TO central" in _NORM
