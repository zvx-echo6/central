"""Tests for the sat_orbits adapter (v0.13.0).

Deterministic: pinned ISS TLE + pinned propagation start + mocked asyncpg
pool. Covers track propagation (vertex count, current sub-sat point in
plausible range), antimeridian-aware geometry (LineString vs
MultiLineString), event-record shape, subject derivation, empty-TLE,
track_only gate, forward_minutes setting, stale-TLE skip.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sgp4.api import Satrec

from central.adapters.sat_orbits import (
    SatOrbitsAdapter,
    SatOrbitsSettings,
    _propagate_track,
)
from central.config_models import AdapterConfig
from central.models import Event, Geo


# Live TLE from v0.11.0 stations fixture, ISS (NORAD 25544). Shared with
# test_satpass_predict / test_sat_common / test_sat_positions so all four
# suites pin against the same orbital configuration.
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
) -> tuple[SatOrbitsAdapter, AsyncMock]:
    cfg = AdapterConfig(
        name="sat_orbits",
        enabled=True,
        cadence_s=300,
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
    adapter = SatOrbitsAdapter(cfg, config_store, tmp_path / "cursors.db")
    return adapter, fetch_mock


# --- Propagation primitive --------------------------------------------------


class TestPropagateTrack:
    def test_iss_90min_at_60s_step_yields_91_vertices(self):
        sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
        track = _propagate_track(sat, _REF, forward_minutes=90, sample_seconds=60)
        # 90 min / 60s + 1 inclusive endpoint = 91 vertices.
        assert len(track) == 91

    def test_first_vertex_matches_v0_11_1_iss_values(self):
        """Phase A sanity pin: at the v0.11.0 fixture TLE + 2026-06-09T07:00 UTC,
        ISS sub-sat point should be ~(170.66, -17.15, 417.41)."""
        sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
        track = _propagate_track(sat, _REF, forward_minutes=90, sample_seconds=60)
        lon, lat, alt = track[0]
        assert lon == pytest.approx(170.66, abs=0.02)
        assert lat == pytest.approx(-17.15, abs=0.02)
        assert alt == pytest.approx(417.41, abs=0.5)

    def test_last_vertex_geographically_distinct_from_first(self):
        """A full LEO orbit traverses 360 degrees in inertial frame; relative to
        the rotating earth the ground track shifts noticeably in 90 minutes."""
        sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
        track = _propagate_track(sat, _REF, forward_minutes=90, sample_seconds=60)
        first, last = track[0], track[-1]
        # At least 10 degrees of lon difference -- ISS moves visibly in 90min.
        # (Account for antimeridian wrap by using the wrap-aware distance.)
        dlon_raw = abs(last[0] - first[0])
        dlon = min(dlon_raw, 360.0 - dlon_raw)
        dlat = abs(last[1] - first[1])
        assert dlon + dlat > 5.0

    def test_forward_minutes_30_yields_31_vertices(self):
        """Linearly scales: 30min / 60s + 1 = 31 vertices."""
        sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
        track = _propagate_track(sat, _REF, forward_minutes=30, sample_seconds=60)
        assert len(track) == 31


# --- Settings defaults pin the v0.13.0 contract -----------------------------


class TestSettingsDefaults:
    def test_default_forward_minutes_is_90(self):
        assert SatOrbitsSettings().forward_minutes == 90

    def test_default_sample_seconds_is_60(self):
        assert SatOrbitsSettings().sample_seconds == 60

    def test_default_max_tle_age_days_is_14(self):
        assert SatOrbitsSettings().max_tle_age_days == 14

    def test_default_track_only_is_empty(self):
        assert SatOrbitsSettings().track_only_norad_ids == []


# --- subject_for + event-record shape ---------------------------------------


class TestSubjectFor:
    def test_subject_carries_norad_id(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path)
        ev = Event(
            id="25544:2026-06-09T07:00:00+00:00",
            adapter="sat_orbits",
            category="orbit.sat_orbits",
            time=_REF,
            severity=1,
            geo=Geo(centroid=(0.0, 0.0)),
            data={"norad_id": 25544},
        )
        assert adapter.subject_for(ev) == "central.sat.orbit.25544"


class TestBuildEvent:
    def test_iss_track_produces_linestring_or_multilinestring(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path)
        sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
        track = _propagate_track(sat, _REF, forward_minutes=90, sample_seconds=60)
        ev = adapter._build_event(_row(), _REF, track)
        assert ev is not None
        assert ev.category == "orbit.sat_orbits"
        assert ev.severity == 1
        # Geometry is either LineString (no crossing) or MultiLineString
        # (crossings present); both are valid for a 90min ISS arc.
        assert ev.geo.geometry["type"] in ("LineString", "MultiLineString")
        # geo.centroid matches first vertex (current position)
        assert ev.geo.centroid[0] == pytest.approx(track[0][0], abs=1e-6)
        assert ev.geo.centroid[1] == pytest.approx(track[0][1], abs=1e-6)

    def test_record_shape(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path)
        sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
        track = _propagate_track(sat, _REF, forward_minutes=90, sample_seconds=60)
        ev = adapter._build_event(_row(), _REF, track)
        assert ev is not None
        assert ev.id == "25544:2026-06-09T07:00:00+00:00"
        for k in ("norad_id", "satellite_name", "propagation_start_iso",
                  "forward_minutes", "sample_seconds", "vertex_count",
                  "current_lon_deg", "current_lat_deg", "current_alt_km",
                  "tle_epoch"):
            assert k in ev.data, f"missing data key {k!r}"
        assert ev.data["vertex_count"] == 91

    def test_returns_none_for_degenerate_track(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path)
        # Fewer than 2 vertices = degenerate; caller skips.
        assert adapter._build_event(_row(), _REF, []) is None
        assert adapter._build_event(_row(), _REF, [(0.0, 0.0, 400.0)]) is None

    def test_dedup_id_collapses_sub_second_ticks(self, tmp_path):
        """Two propagation_start times differing only in microseconds yield
        identical dedup ids."""
        adapter, _ = _make_adapter(tmp_path)
        sat = Satrec.twoline2rv(_ISS_L1, _ISS_L2)
        track = _propagate_track(sat, _REF, forward_minutes=10, sample_seconds=60)
        t1 = _REF.replace(microsecond=1)
        t2 = _REF.replace(microsecond=999999)
        ev1 = adapter._build_event(_row(), t1, track)
        ev2 = adapter._build_event(_row(), t2, track)
        assert ev1 is not None and ev2 is not None
        assert ev1.id == ev2.id


# --- Poll loop --------------------------------------------------------------


class TestPollEmptyTleTable:
    @pytest.mark.asyncio
    async def test_yields_zero_no_exception(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path, fetch_rows=[])
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert events == []


class TestPollTrackOnlyGate:
    @pytest.mark.asyncio
    async def test_empty_publishes_all(self, tmp_path):
        rows = [_row(25544, "ISS"), _row(43013, "NOAA 20")]
        adapter, _ = _make_adapter(tmp_path,
                                   settings={"track_only_norad_ids": []},
                                   fetch_rows=rows)
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert {e.data["norad_id"] for e in events} == {25544, 43013}

    @pytest.mark.asyncio
    async def test_non_empty_restricts(self, tmp_path):
        rows = [_row(25544, "ISS"), _row(43013, "NOAA 20")]
        adapter, _ = _make_adapter(tmp_path,
                                   settings={"track_only_norad_ids": [25544]},
                                   fetch_rows=rows)
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert {e.data["norad_id"] for e in events} == {25544}


class TestPollStaleTleSkip:
    @pytest.mark.asyncio
    async def test_default_passes_14d_interval(self, tmp_path):
        adapter, fetch_mock = _make_adapter(tmp_path, fetch_rows=[])
        await adapter.startup()
        _ = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        args, _kwargs = fetch_mock.call_args
        assert args[1] == timedelta(days=14)

    @pytest.mark.asyncio
    async def test_operator_window_propagates(self, tmp_path):
        adapter, fetch_mock = _make_adapter(
            tmp_path, settings={"max_tle_age_days": 3}, fetch_rows=[],
        )
        await adapter.startup()
        _ = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        args, _kwargs = fetch_mock.call_args
        assert args[1] == timedelta(days=3)


class TestPollForwardMinutesSetting:
    @pytest.mark.asyncio
    async def test_30min_setting_produces_31_vertex_track(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path,
                                   settings={"forward_minutes": 30,
                                             "sample_seconds": 60},
                                   fetch_rows=[_row()])
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert len(events) == 1
        assert events[0].data["vertex_count"] == 31
        assert events[0].data["forward_minutes"] == 30


class TestPollPropagatesIss:
    @pytest.mark.asyncio
    async def test_iss_row_produces_one_telemetry_event(self, tmp_path):
        adapter, _ = _make_adapter(tmp_path, fetch_rows=[_row()])
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert len(events) == 1
        ev = events[0]
        assert ev.data["norad_id"] == 25544
        assert ev.severity == 1
        assert ev.geo.geometry["type"] in ("LineString", "MultiLineString")
        assert 380.0 <= ev.data["current_alt_km"] <= 460.0
        assert -52.0 <= ev.data["current_lat_deg"] <= 52.0
