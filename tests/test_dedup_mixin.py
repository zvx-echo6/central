"""Tests for the shared dedup mixin on SourceAdapter (v0.9.1 extraction).

is_published / mark_published / sweep_old_ids moved from per-adapter inline
copies onto the base; dedup_sweep_days parameterizes the retention window
(NWIS keeps 30 days, the default is 14).
"""

import sqlite3

from central.adapter import SourceAdapter
from central.adapters.inciweb import InciWebAdapter
from central.adapters.nwis import NWISAdapter
from central.adapters.wzdx import WZDxAdapter

_DDL = (
    "CREATE TABLE published_ids (adapter TEXT, event_id TEXT, "
    "first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (adapter, event_id))"
)


class _StubAdapter(SourceAdapter):
    """Minimal concrete adapter exercising only the inherited dedup mixin."""

    name = "stub"
    display_name = "Stub"
    description = "test"
    settings_schema = None
    default_cadence_s = 600

    async def poll(self):  # pragma: no cover - not exercised
        return
        yield

    async def apply_config(self, new_config):  # pragma: no cover
        ...

    def subject_for(self, event):  # pragma: no cover
        return "x"


def _adapter(tmp_path, dbname="d.db", sweep_days=None):
    a = _StubAdapter()
    a._db = sqlite3.connect(tmp_path / dbname)
    a._db.execute(_DDL)
    a._db.commit()
    if sweep_days is not None:
        a.dedup_sweep_days = sweep_days
    return a


def test_dedup_roundtrip(tmp_path):
    a = _adapter(tmp_path)
    assert a.is_published("e1") is False
    a.mark_published("e1")
    assert a.is_published("e1") is True


def test_no_db_is_safe():
    a = _StubAdapter()
    a._db = None
    assert a.is_published("e") is False
    a.mark_published("e")  # no raise
    assert a.sweep_old_ids() == 0


def test_sweep_respects_dedup_sweep_days(tmp_path):
    a = _adapter(tmp_path, "a.db", sweep_days=14)
    a._db.execute(
        "INSERT INTO published_ids (adapter, event_id, last_seen) "
        "VALUES ('stub', 'old', datetime('now','-20 days'))"
    )
    a._db.commit()
    assert a.sweep_old_ids() == 1  # 20d > 14d -> swept

    b = _adapter(tmp_path, "b.db", sweep_days=30)
    b._db.execute(
        "INSERT INTO published_ids (adapter, event_id, last_seen) "
        "VALUES ('stub', 'old', datetime('now','-20 days'))"
    )
    b._db.commit()
    assert b.sweep_old_ids() == 0  # 20d < 30d -> kept


def test_named_adapters_inherit_base():
    for cls in (InciWebAdapter, NWISAdapter, WZDxAdapter):
        for m in ("is_published", "mark_published", "sweep_old_ids"):
            assert m not in cls.__dict__, f"{cls.__name__} still overrides {m}"
            assert getattr(cls, m) is getattr(SourceAdapter, m)
    assert NWISAdapter.dedup_sweep_days == 30
    assert WZDxAdapter.dedup_sweep_days == 14
    assert InciWebAdapter.dedup_sweep_days == 14
