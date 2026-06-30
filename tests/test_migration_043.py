"""v0.15.0 migration 043: add kind column to config.adapters.

No live Postgres required — asserts the migration SQL is structurally correct:
adds the column idempotently, back-fills kind = name for pre-existing rows, and
enforces NOT NULL going forward.
"""

from pathlib import Path

_SQL = Path("sql/migrations/043_add_adapters_kind_column.sql").read_text()
_NORM = " ".join(_SQL.split())  # whitespace-insensitive matching


def test_uses_add_column_if_not_exists():
    """Must be idempotent — safe to re-run after partial application."""
    assert "ADD COLUMN IF NOT EXISTS kind TEXT" in _NORM


def test_backfills_kind_equals_name_for_null_rows():
    """Existing rows must get kind = name so built-in adapters keep working."""
    assert "UPDATE config.adapters SET kind = name WHERE kind IS NULL" in _NORM


def test_enforces_not_null_after_backfill():
    """kind must be NOT NULL once every row is back-filled."""
    assert "ALTER COLUMN kind SET NOT NULL" in _NORM


def test_does_not_add_default_clause():
    """No DEFAULT — future INSERT paths must always supply kind explicitly."""
    # The ADD COLUMN line should not contain DEFAULT
    add_line = next(
        (line for line in _SQL.splitlines() if "ADD COLUMN" in line), ""
    )
    assert "DEFAULT" not in add_line.upper()


def test_does_not_drop_any_columns():
    """Pure additive migration — must not drop anything."""
    upper = _NORM.upper()
    assert "DROP COLUMN" not in upper
    assert "DROP TABLE" not in upper


def test_explains_name_vs_kind_split_in_comment():
    """Header comment must explain the instance-vs-class identity split."""
    assert "instance" in _SQL.lower()
    assert "class" in _SQL.lower()
