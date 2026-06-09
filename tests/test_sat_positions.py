"""Tests for the sat_positions adapter (v0.12.0).

Deterministic: pinned ISS TLE + pinned reference time + mock asyncpg pool.
Covers math (sub-sat point in plausible range, velocity in ISS band,
heading wrapped into [0, 360)), event-record shape (severity, geo,
dedup id), subject derivation, empty-TLE-table case, track_only_norad_ids
gate, stale-TLE skip (via SQL parameter binding).
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sgp4.api import Satrec

from central.adapters.sat_positions import (
    SatPositionsAdapter,
    SatPositionsSettings,
    _great_circle_bearing,
    _propagate_position,
)
from central.config_models import AdapterConfig
from central.models import Event, Geo


# Live TLE from the v0.11.0 stations fixture, ISS (NORAD 25544). Same as
# test_satpass_predict.py + test_sat_common.py so all three suites pin
# against the same orbital configuration.
_ISS_L1 = "1 25544U 98067A   26159.80410962  .00007129  00000+0  13425-3 0  9999"
_ISS_L2 = "2 25544  51.6336 341.5878 0006923 148.5365 211.6039 15.49672912570453"
_REF = datetime(2026, 6, 9, 7, 0, 0, tzinfo=timezone.utc)


def _row(norad_id: int = 25544, name: str = "ISS (ZARYA)") -> dict[str, Any]:
    return {
        "norad_id": norad_id,
        "satellite_name": name,
        "tle_line1": _ISS_L1,
        "tle_line2": _ISS_L2,
        "tle_epoch": "2026-06-08T19:17:55.071",
    }


def _make_adapter(
    tmp_path: Path,
    settings: dict[str, Any] | None = None,
    fetch_rows: list[dict[str, Any]] | None = None,
) -> tuple[SatPositionsAdapter, AsyncMock]:
    """Build an adapter with a mocked asyncpg pool that returns ``fetch_rows``.

    Returns (adapter, fetch_mock) so tests can also assert on what was passed
    to conn.fetch (e.g. the timedelta interval for max_tle_age_days).
    """
    cfg = AdapterConfig(
        name="sat_positions",
        enabled=True,
        cadence_s=60,
        settings=settings or {},
        updated_at=datetime.now(timezone.utc),
    )

    fetch_mock = AsyncMock(return_value=fetch_rows or [])
    conn = MagicMock()
    conn.fetch = fetch_mock
    acquire_cm = MagicMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=conn)
    acquire_cm.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acquire_cm)
    config_store = MagicMock()
    config_store.get_pool = MagicMock(return_value=pool)

    adapter = SatPositionsAdapter(cfg, config_store, tmp_path / "cursors.db")
    return adapter, fetch_mock


# --- Pure math via the helpers ----------------------------------------------


class TestGreatCircleBearing:
    def test_due_north_is_zero(self):
        # Same longitude, north of point 1 -> bearing 0.
        bearing = _great_circle_bearing(0.0, 0.0, 0.0, 10.0)
        assert bearing == pytest.approx(0.0, abs=0.01)

    def test_due_east_is_ninety(self):
        bearing = _great_circle_bearing(0.0, 0.0, 10.0, 0.0)
        assert bearing == pytest.approx(90.0, abs=0.01)

    def test_due_south_is_one_eighty(self):
        bearing = _great_circle_bearing(0.0, 10.0, 0.0, 0.0)
        assert bearing == pytest.approx(180.0, abs=0.01)

    def test_wraps_into_canonical_range(self):
        # SW direction -> bearing in [180, 270).
        bearing = _great_circle_bearing(0.0, 0.0, -10.0, -10.0)
        assert 180.0 <= bearing < 270.0


class TestPropagatePosition:
    def test_iss_at_ref_time_returns_plausible_position(self):
        sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
        sample = _propagate_position(sat, _REF)
        assert sample is not None
        _, vel_eci, lon, lat, alt = sample
        # ISS inclination 51.6° caps |lat| around there
        assert -52.0 <= lat <= 52.0
        assert -180.0 <= lon <= 180.0
        assert 380.0 <= alt <= 460.0
        # |vel| roughly 7.66 km/s for ISS
        vmag = math.sqrt(sum(c * c for c in vel_eci))
        assert 7.5 <= vmag <= 8.0


# --- Adapter surface --------------------------------------------------------


class TestSettingsDefaults:
    def test_track_only_defaults_to_empty(self):
        s = SatPositionsSettings()
        assert s.track_only_norad_ids == []

    def test_max_tle_age_defaults_to_14_days(self):
        s = SatPositionsSettings()
        assert s.max_tle_age_days == 14


class TestSubjectFor:
    def test_subject_carries_norad_id(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path)
        ev = Event(
            id="25544:2026-06-09T07:00:00+00:00",
            adapter="sat_positions",
            category="position.sat_positions",
            time=_REF,
            severity=1,
            geo=Geo(centroid=(0.0, 0.0)),
            data={"norad_id": 25544},
        )
        assert adapter.subject_for(ev) == "central.sat.position.25544"

    def test_subject_for_distinct_norad_ids_distinct_subjects(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path)
        ev1 = Event(id="x", adapter="sat_positions",
                    category="position.sat_positions", time=_REF,
                    severity=1, geo=Geo(centroid=(0.0, 0.0)),
                    data={"norad_id": 25544})
        ev2 = Event(id="y", adapter="sat_positions",
                    category="position.sat_positions", time=_REF,
                    severity=1, geo=Geo(centroid=(0.0, 0.0)),
                    data={"norad_id": 43013})
        assert adapter.subject_for(ev1) != adapter.subject_for(ev2)


class TestBuildEvent:
    def test_event_record_shape(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path)
        ev = adapter._build_event(
            row=_row(),
            ref_time=_REF,
            lon_deg=-116.2,
            lat_deg=43.6,
            alt_km=408.5,
            velocity_kmps=7.66,
            heading_deg=87.3,
        )
        assert ev.severity == 1
        assert ev.category == "position.sat_positions"
        assert ev.adapter == "sat_positions"
        # Dedup id: <norad_id>:<position_iso (seconds-truncated)>
        assert ev.id == "25544:2026-06-09T07:00:00+00:00"
        # geo.centroid populated for live-map plotting
        assert ev.geo.centroid == (-116.2, 43.6)
        # geo.geometry intentionally None (no overlay in v1)
        assert ev.geo.geometry is None
        # All declared data fields present
        for k in ("norad_id", "satellite_name", "lon_deg", "lat_deg",
                  "alt_km", "velocity_kmps", "heading_deg", "tle_epoch"):
            assert k in ev.data

    def test_dedup_id_collapses_sub_second_ticks(self, tmp_path):
        """Two ref_times that differ only in microseconds yield the same dedup id."""
        adapter, _ = _make_adapter(tmp_path)
        t1 = datetime(2026, 6, 9, 7, 0, 0, 1, tzinfo=timezone.utc)
        t2 = datetime(2026, 6, 9, 7, 0, 0, 999999, tzinfo=timezone.utc)
        ev1 = adapter._build_event(_row(), t1, 0.0, 0.0, 400.0, 7.5, 90.0)
        ev2 = adapter._build_event(_row(), t2, 0.0, 0.0, 400.0, 7.5, 90.0)
        assert ev1.id == ev2.id


# --- Poll loop --------------------------------------------------------------


class TestPollEmptyTleTable:
    @pytest.mark.asyncio
    async def test_yields_zero_events_no_exception(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path, fetch_rows=[])
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        assert events == []
        await adapter.shutdown()


class TestPollTrackOnlyGate:
    @pytest.mark.asyncio
    async def test_empty_list_publishes_all_rows(self, tmp_path):
        rows = [_row(25544, "ISS (ZARYA)"), _row(43013, "NOAA 20")]
        adapter, _ = _make_adapter(tmp_path, settings={"track_only_norad_ids": []},
                                   fetch_rows=rows)
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert {e.data["norad_id"] for e in events} == {25544, 43013}

    @pytest.mark.asyncio
    async def test_non_empty_list_filters_to_pinned_ids(self, tmp_path):
        rows = [_row(25544, "ISS (ZARYA)"), _row(43013, "NOAA 20")]
        adapter, _ = _make_adapter(tmp_path,
                                   settings={"track_only_norad_ids": [25544]},
                                   fetch_rows=rows)
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert {e.data["norad_id"] for e in events} == {25544}


class TestPollStaleTleSkip:
    """The max_tle_age_days setting is honored by binding a timedelta interval
    to the SQL query. Verifying the parameter passed to conn.fetch is the
    right interpretation of the spec's 'stale TLE skip' requirement: SQL
    enforces the cutoff, and we assert it gets the configured value."""

    @pytest.mark.asyncio
    async def test_default_passes_14d_interval(self, tmp_path):
        adapter, fetch_mock = _make_adapter(tmp_path, fetch_rows=[])
        await adapter.startup()
        _ = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        # First positional arg after SQL is the interval timedelta.
        args, _kwargs = fetch_mock.call_args
        assert args[1] == timedelta(days=14)

    @pytest.mark.asyncio
    async def test_operator_tightened_window_propagates_to_sql(self, tmp_path):
        adapter, fetch_mock = _make_adapter(tmp_path,
                                            settings={"max_tle_age_days": 3},
                                            fetch_rows=[])
        await adapter.startup()
        _ = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        args, _kwargs = fetch_mock.call_args
        assert args[1] == timedelta(days=3)


class TestPollPropagatesIss:
    """End-to-end with a single real TLE: poll yields one event with all
    fields populated within plausible ranges for ISS."""

    @pytest.mark.asyncio
    async def test_iss_row_produces_one_telemetry_event(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path, fetch_rows=[_row()])
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert len(events) == 1
        ev = events[0]
        assert ev.data["norad_id"] == 25544
        assert ev.data["satellite_name"] == "ISS (ZARYA)"
        # ISS inclination 51.6° bounds latitude
        assert -52.0 <= ev.data["lat_deg"] <= 52.0
        assert -180.0 <= ev.data["lon_deg"] <= 180.0
        assert 380.0 <= ev.data["alt_km"] <= 460.0
        assert 7.5 <= ev.data["velocity_kmps"] <= 8.0
        assert 0.0 <= ev.data["heading_deg"] < 360.0
        assert ev.severity == 1
        # geo.centroid carries full SGP4 precision; data fields are rounded
        # for operator-readability. They agree to 4 decimal places by design.
        assert ev.geo.centroid[0] == pytest.approx(ev.data["lon_deg"], abs=1e-4)
        assert ev.geo.centroid[1] == pytest.approx(ev.data["lat_deg"], abs=1e-4)
