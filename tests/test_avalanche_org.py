"""Tests for the v0.10.10 avalanche_org adapter."""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.adapters.avalanche_org import (
    _DANGER_TO_SEVERITY,
    AvalancheOrgAdapter,
    AvalancheOrgSettings,
    _centroid,
    _parse_iso,
    _slug,
)
from central.config_models import AdapterConfig


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "avalanche_snfac.json"


@pytest.fixture
def snfac_response() -> dict:
    """Frozen 2026-06-08 SNFAC map-layer response. 4 features, all off-season."""
    return json.loads(FIXTURE_PATH.read_text())


@pytest.fixture
def adapter(tmp_path: Path) -> AvalancheOrgAdapter:
    cfg = AdapterConfig(
        name="avalanche_org",
        enabled=True,
        cadence_s=1800,
        settings={"center_ids": ["SNFAC"]},
        updated_at=datetime.now(timezone.utc),
    )
    return AvalancheOrgAdapter(cfg, MagicMock(), tmp_path / "cursors.db")


# --- Pure helper tests ------------------------------------------------------


@pytest.mark.parametrize("text, expected", [
    ("Banner Summit", "banner_summit"),
    ("Sawtooth & Western Smoky Mtns", "sawtooth_western_smoky_mtns"),
    ("Galena Summit & Eastern Mtns", "galena_summit_eastern_mtns"),
    ("Soldier & Wood River Valley Mtns", "soldier_wood_river_valley_mtns"),
    ("ALL CAPS", "all_caps"),
    ("  leading/trailing  ", "leading_trailing"),
    ("hyphens-and_underscores", "hyphens_and_underscores"),
    ("", ""),
])
def test_slug(text, expected):
    assert _slug(text) == expected


def test_parse_iso_naive_treated_as_utc():
    """avalanche.org returns naive ISO strings; we tag UTC and pass through."""
    dt = _parse_iso("2026-05-04T17:59:00")
    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert dt.year == 2026 and dt.month == 5 and dt.day == 4


def test_parse_iso_handles_z_suffix():
    dt = _parse_iso("2026-06-08T10:00:00Z")
    assert dt is not None
    assert dt.hour == 10


@pytest.mark.parametrize("bad", [None, "", "not-a-date", 12345, "2026-99-99"])
def test_parse_iso_returns_none_on_bad_input(bad):
    assert _parse_iso(bad) is None


def test_centroid_of_simple_polygon():
    geom = {
        "type": "Polygon",
        "coordinates": [[[-115, 44], [-114, 44], [-114, 45], [-115, 45], [-115, 44]]],
    }
    c = _centroid(geom)
    assert c is not None
    lon, lat = c
    assert abs(lon - (-114.5)) < 1e-6
    assert abs(lat - 44.5) < 1e-6


def test_centroid_handles_invalid_geom():
    assert _centroid(None) is None
    assert _centroid({"type": "Polygon", "coordinates": "garbage"}) is None


def test_danger_severity_map_matches_central_4_most_severe_convention():
    """Anti-regression: meshai's original spec inverted this (5→1). v0.10.10
    corrected to Central-wide convention: higher = more severe.
    """
    assert _DANGER_TO_SEVERITY == {3: 2, 4: 3, 5: 4}


# --- Build-event severity gate ---------------------------------------------


def _base_feature(**overrides) -> dict:
    """Minimal valid feature; tests override the properties under test."""
    props = {
        "name": "Banner Summit",
        "state": "ID",
        "off_season": False,
        "danger_level": 3,
        "danger": "Considerable",
        "travel_advice": "Watch for unstable snow.",
        "start_date": "2026-12-15T17:59:00",
        "end_date": "2026-12-16T19:00:00",
    }
    props.update(overrides)
    return {
        "type": "Feature",
        "id": 1,
        "properties": props,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-115.0, 44.0], [-114.0, 44.0],
                             [-114.0, 45.0], [-115.0, 45.0], [-115.0, 44.0]]],
        },
    }


@pytest.mark.parametrize("danger_level, expected_severity", [
    (3, 2),  # Considerable
    (4, 3),  # High
    (5, 4),  # Extreme
])
def test_publishable_danger_levels_yield_event_with_mapped_severity(
    adapter, danger_level, expected_severity
):
    ev = adapter._build_event_record(_base_feature(danger_level=danger_level), "SNFAC")
    assert ev is not None
    assert ev.severity == expected_severity
    assert ev.data["danger_level"] == danger_level
    assert ev.data["state"] == "ID"
    assert ev.data["center_id"] == "SNFAC"
    assert ev.data["zone_name"] == "Banner Summit"
    assert ev.data["off_season"] is False
    assert ev.id == "SNFAC_banner_summit"
    assert ev.category == "avy.advisory.snfac"
    assert ev.geo.primary_region == "US-ID"
    assert ev.geo.geometry is not None


@pytest.mark.parametrize("danger_level", [-1, 0, 1, 2])
def test_low_or_no_rating_danger_levels_are_omitted(adapter, danger_level):
    assert adapter._build_event_record(
        _base_feature(danger_level=danger_level), "SNFAC"
    ) is None


def test_off_season_true_omitted_regardless_of_danger_level(adapter):
    feat = _base_feature(off_season=True, danger_level=4)
    assert adapter._build_event_record(feat, "SNFAC") is None


def test_missing_state_omitted(adapter):
    feat = _base_feature(state="")
    assert adapter._build_event_record(feat, "SNFAC") is None


def test_unparseable_geometry_omitted(adapter):
    feat = _base_feature()
    feat["geometry"] = None
    assert adapter._build_event_record(feat, "SNFAC") is None


def test_travel_advice_truncated_to_200_chars(adapter):
    long = "x" * 500
    ev = adapter._build_event_record(_base_feature(travel_advice=long), "SNFAC")
    assert ev is not None
    assert len(ev.data["travel_advice"]) == 200


def test_subject_for_uses_state_lowercase(adapter):
    ev = adapter._build_event_record(_base_feature(state="ID"), "SNFAC")
    assert adapter.subject_for(ev) == "central.avy.advisory.us.id"


# --- Real-fixture behavior (the negative case) ------------------------------


def test_real_snfac_fixture_all_zones_omitted_during_off_season(adapter, snfac_response):
    """The frozen 2026-06-08 SNFAC fixture has 4 zones, all off-season + danger
    -1. The adapter must yield zero events from that response.
    """
    yielded = []
    for feat in snfac_response["features"]:
        ev = adapter._build_event_record(feat, "SNFAC")
        if ev is not None:
            yielded.append(ev)
    assert len(snfac_response["features"]) == 4
    assert yielded == []


def test_real_snfac_fixture_with_synthetic_winter_overrides_publishes_all(
    adapter, snfac_response
):
    """Same fixture, but mutate each feature to a winter Considerable state.
    Asserts the geometry/centroid path works against the real polygon shapes
    avalanche.org actually returns (not our hand-crafted square)."""
    winter = copy.deepcopy(snfac_response)
    for feat in winter["features"]:
        feat["properties"]["off_season"] = False
        feat["properties"]["danger_level"] = 3
        feat["properties"]["danger"] = "Considerable"
    yielded = []
    for feat in winter["features"]:
        ev = adapter._build_event_record(feat, "SNFAC")
        if ev is not None:
            yielded.append(ev)
    assert len(yielded) == 4
    # Each yielded event has a non-(0,0) centroid and unique id slug.
    ids = {ev.id for ev in yielded}
    assert len(ids) == 4
    for ev in yielded:
        lon, lat = ev.geo.centroid
        assert -120 < lon < -110, f"unexpected lon: {lon}"
        assert 40 < lat < 50, f"unexpected lat: {lat}"
        assert ev.geo.geometry["type"] in ("Polygon", "MultiPolygon")


# --- Settings + adapter scaffolding ----------------------------------------


def test_default_settings_cover_snfac_and_pac():
    s = AvalancheOrgSettings()
    assert "SNFAC" in s.center_ids
    assert "PAC" in s.center_ids


@pytest.mark.asyncio
async def test_apply_config_swaps_center_ids(adapter):
    new_cfg = AdapterConfig(
        name="avalanche_org",
        enabled=True,
        cadence_s=1800,
        settings={"center_ids": ["NWAC"]},
        updated_at=datetime.now(timezone.utc),
    )
    await adapter.apply_config(new_cfg)
    assert adapter._center_ids == ["NWAC"]


# --- Stream registry + family mapping (the v0.10.10 wiring) ----------------


def test_central_avy_registered_in_streams():
    from central.streams import STREAMS
    avy = [s for s in STREAMS if s.name == "CENTRAL_AVY"]
    assert len(avy) == 1
    assert avy[0].subject_filter == "central.avy.>"
    assert avy[0].event_bearing is True


def test_central_avy_in_supervisor_family_map():
    from central.supervisor import STREAM_CATEGORY_DOMAINS
    assert STREAM_CATEGORY_DOMAINS["CENTRAL_AVY"] == ("avy",)


@pytest.mark.asyncio
async def test_poll_with_no_centers_yields_nothing():
    """Defensive: empty center_ids must not crash, must yield zero events."""
    cfg = AdapterConfig(
        name="avalanche_org", enabled=True, cadence_s=1800,
        settings={"center_ids": []},
        updated_at=datetime.now(timezone.utc),
    )
    adapter = AvalancheOrgAdapter(cfg, MagicMock(), Path("/tmp/avy_empty.db"))
    await adapter.startup()
    try:
        events = [e async for e in adapter.poll()]
        assert events == []
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_poll_yields_only_publishable_features(adapter, snfac_response, monkeypatch):
    """End-to-end poll(): inject a mock _fetch returning a mixed winter response;
    assert only danger>=3 features are yielded."""
    await adapter.startup()
    try:
        mixed = copy.deepcopy(snfac_response)
        # Feature 0: off-season (omit)   Feature 1: danger 1 (omit)
        # Feature 2: danger 4 (publish)  Feature 3: danger -1 (omit)
        for f in mixed["features"]:
            f["properties"]["off_season"] = False
        mixed["features"][0]["properties"]["off_season"] = True
        mixed["features"][1]["properties"]["danger_level"] = 1
        mixed["features"][2]["properties"]["danger_level"] = 4
        mixed["features"][3]["properties"]["danger_level"] = -1

        monkeypatch.setattr(adapter, "_fetch", AsyncMock(return_value=mixed))
        events = [e async for e in adapter.poll()]
        assert len(events) == 1
        assert events[0].severity == 3  # danger 4 → severity 3
    finally:
        await adapter.shutdown()
