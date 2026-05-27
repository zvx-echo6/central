"""Drift-detection tests for the migration runner (v0.9.18).

`find_drift` is a pure function so the divergence logic is verified without a
database -- the suite runs identically as `zvx` or `central`.
"""

from pathlib import Path

from central.migrate import find_drift


def _discovered(*versions: str) -> list[tuple[str, Path]]:
    """Build a discover_migrations-shaped list from version stems."""
    return [(v, Path(f"sql/migrations/{v}.sql")) for v in versions]


def test_find_drift_untracked_file():
    """A migration file on disk with no schema_migrations row is untracked.

    This is the v0.9.18 case: 025-029 existed on disk but were never recorded.
    """
    applied = {"001_a", "002_b"}
    discovered = _discovered("001_a", "002_b", "003_c")

    untracked, orphan = find_drift(applied, discovered)

    assert untracked == ["003_c"]
    assert orphan == []


def test_find_drift_orphan_row():
    """A schema_migrations row with no matching .sql file is an orphan."""
    applied = {"001_a", "002_b", "099_removed"}
    discovered = _discovered("001_a", "002_b")

    untracked, orphan = find_drift(applied, discovered)

    assert untracked == []
    assert orphan == ["099_removed"]


def test_find_drift_clean():
    """No drift when disk and schema_migrations match exactly."""
    applied = {"001_a", "002_b", "003_c"}
    discovered = _discovered("001_a", "002_b", "003_c")

    untracked, orphan = find_drift(applied, discovered)

    assert untracked == []
    assert orphan == []
