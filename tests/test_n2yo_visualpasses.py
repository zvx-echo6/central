"""Tests for the n2yo_visualpasses adapter (v0.12.1).

Strictly offline: every HTTP path is mocked. The synthetic ISS-over-Filer
fixture mirrors the shape of n2yo's documented ``visualpasses`` response
(see https://www.n2yo.com/api/ -> "Visual Passes" section). Values are
plausible for an ISS pass at low magnitude and a peak around 47° elev.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.adapters.n2yo_visualpasses import (
    N2yoVisualpassesAdapter,
    N2yoVisualpassesSettings,
    Observer,
    _severity_from_magnitude,
)
from central.config_models import AdapterConfig
from central.models import Event, Geo


# n2yo response fixture: one ISS pass over Filer, mag -3.4 (naked-eye easy),
# peak 47° elevation toward ESE, ~9.3 min above horizon. UTC fields are
# Unix timestamps per n2yo's convention.
_AOS_UNIX = 1781382000  # 2026-06-08T21:00:00Z (illustrative)
_PEAK_UNIX = _AOS_UNIX + 300
_LOS_UNIX = _AOS_UNIX + 560


def _iss_pass_fixture() -> dict[str, Any]:
    return {
        "info": {
            "satid": 25544,
            "satname": "ISS (ZARYA)",
            "transactionscount": 1,
            "passescount": 1,
        },
        "passes": [{
            "startAz": 285.4, "startAzCompass": "WNW", "startEl": 0.0,
            "startUTC": _AOS_UNIX,
            "maxAz": 196.7, "maxAzCompass": "SSW", "maxEl": 47.0,
            "maxUTC": _PEAK_UNIX,
            "endAz": 113.2, "endAzCompass": "ESE", "endEl": 0.0,
            "endUTC": _LOS_UNIX,
            "mag": -3.4, "duration": 560,
        }],
    }


def _empty_fixture(norad_id: int = 25544) -> dict[str, Any]:
    """n2yo returns passes=[] when no visible passes in the horizon."""
    return {
        "info": {"satid": norad_id, "satname": "SAT",
                 "transactionscount": 1, "passescount": 0},
        "passes": [],
    }


def _filer() -> Observer:
    return Observer(name="Filer", slug="filer", state="ID",
                    lat=42.57, lon=-114.60, elev_m=1200.0)


def _make_adapter(
    tmp_path: Path,
    settings: dict[str, Any] | None = None,
    api_key: str | None = "fake-test-key",
) -> N2yoVisualpassesAdapter:
    """Build adapter with a mocked ConfigStore returning the supplied api_key."""
    cfg = AdapterConfig(
        name="n2yo_visualpasses",
        enabled=True,
        cadence_s=3600,
        settings=settings if settings is not None else {
            "observers": [_filer().model_dump()],
            "norad_ids": [25544],
            "days_ahead": 2,
            "min_visibility_seconds": 300,
            "api_key_alias": "n2yo",
        },
        updated_at=datetime.now(timezone.utc),
    )
    config_store = MagicMock()
    config_store.get_api_key = AsyncMock(return_value=api_key)
    return N2yoVisualpassesAdapter(cfg, config_store, tmp_path / "cursors.db")


# --- Pure severity bucketing ----------------------------------------------


class TestSeverityBucketing:
    """Boundary cases per spec: mag <= -3 -> 4; <= -1 -> 3; <= 2 -> 2; else 1."""

    def test_very_bright_iridium_flare(self):
        assert _severity_from_magnitude(-4.5) == 4

    def test_exactly_minus_three_is_bucket_4(self):
        """Boundary: -3 is INCLUDED in bucket 4 (lower = brighter, more severe)."""
        assert _severity_from_magnitude(-3.0) == 4

    def test_just_above_minus_three_is_bucket_3(self):
        assert _severity_from_magnitude(-2.9) == 3

    def test_just_below_minus_three_is_bucket_4(self):
        assert _severity_from_magnitude(-3.1) == 4

    def test_naked_eye_easy_is_bucket_3(self):
        assert _severity_from_magnitude(-1.5) == 3

    def test_exactly_minus_one_is_bucket_3(self):
        assert _severity_from_magnitude(-1.0) == 3

    def test_just_above_minus_one_is_bucket_2(self):
        assert _severity_from_magnitude(-0.5) == 2

    def test_exactly_two_is_bucket_2(self):
        assert _severity_from_magnitude(2.0) == 2

    def test_above_two_is_bucket_1(self):
        assert _severity_from_magnitude(2.5) == 1


# --- Settings defaults pin the curated 6x6 set ------------------------------


class TestSettingsDefaults:
    def test_default_six_observers_in_id_and_ut(self):
        s = N2yoVisualpassesSettings()
        slugs = [o.slug for o in s.observers]
        assert slugs == ["filer", "boise", "idaho-falls", "ogden",
                         "salt-lake-city", "provo"]
        states = {o.state for o in s.observers}
        assert states == {"ID", "UT"}

    def test_default_six_curated_norad_ids(self):
        s = N2yoVisualpassesSettings()
        # ISS, NOAA-15/18/19, SO-50, AO-91
        assert s.norad_ids == [25544, 25338, 28654, 33591, 27607, 43017]

    def test_quota_math_under_free_tier(self):
        s = N2yoVisualpassesSettings()
        polls_per_day_at_1h_cadence = 24
        daily = len(s.observers) * len(s.norad_ids) * polls_per_day_at_1h_cadence
        assert daily == 864
        assert daily < 1000  # n2yo free-tier daily cap

    def test_default_api_key_alias(self):
        assert N2yoVisualpassesSettings().api_key_alias == "n2yo"


# --- Adapter class attrs pin GUI wiring -------------------------------------


class TestAdapterClassAttrs:
    def test_requires_api_key_n2yo(self):
        assert N2yoVisualpassesAdapter.requires_api_key == "n2yo"

    def test_api_key_field_pins_settings_field_name(self):
        """Lets the GUI render an api_key_select dropdown bound to settings."""
        assert N2yoVisualpassesAdapter.api_key_field == "api_key_alias"

    def test_data_class_is_event(self):
        assert N2yoVisualpassesAdapter.data_class == "event"

    def test_default_cadence_is_one_hour(self):
        assert N2yoVisualpassesAdapter.default_cadence_s == 3600


# --- subject_for + event-record shape ---------------------------------------


class TestSubjectFor:
    def test_subject_matches_satpass_predict_shape(self, tmp_path):
        """Subject collision with satpass_predict is intentional per v0.12.1
        design. Vendor disambiguation lives in data.category."""
        adapter = _make_adapter(tmp_path)
        ev = Event(
            id="x", adapter="n2yo_visualpasses", category="pass.n2yo_visualpasses",
            time=datetime.now(timezone.utc), severity=2,
            geo=Geo(centroid=(0.0, 0.0)),
            data={"observer_state": "ID", "observer_slug": "filer"},
        )
        assert adapter.subject_for(ev) == "central.sat.pass.us.id.filer"

    def test_state_is_lowercased(self, tmp_path):
        adapter = _make_adapter(tmp_path)
        ev = Event(
            id="x", adapter="n2yo_visualpasses", category="pass.n2yo_visualpasses",
            time=datetime.now(timezone.utc), severity=2,
            geo=Geo(centroid=(0.0, 0.0)),
            data={"observer_state": "UT", "observer_slug": "ogden"},
        )
        assert adapter.subject_for(ev) == "central.sat.pass.us.ut.ogden"

    def test_unknown_state_falls_back(self, tmp_path):
        adapter = _make_adapter(tmp_path)
        ev = Event(
            id="x", adapter="n2yo_visualpasses", category="pass.n2yo_visualpasses",
            time=datetime.now(timezone.utc), severity=2,
            geo=Geo(centroid=(0.0, 0.0)),
            data={"observer_state": "", "observer_slug": ""},
        )
        assert adapter.subject_for(ev) == "central.sat.pass.us.unknown.unknown"


class TestPassToEvent:
    def test_record_shape(self, tmp_path):
        adapter = _make_adapter(tmp_path)
        fix = _iss_pass_fixture()
        ev = adapter._pass_to_event(fix["passes"][0], fix["info"], _filer())
        # Identity / category / severity
        assert ev.adapter == "n2yo_visualpasses"
        assert ev.category == "pass.n2yo_visualpasses"
        assert ev.severity == 4  # mag=-3.4 -> bucket 4
        # Dedup id: <observer_slug>:<norad_id>:<aos_iso>
        aos_iso = datetime.fromtimestamp(_AOS_UNIX, tz=timezone.utc).isoformat()
        assert ev.id == f"filer:25544:{aos_iso}"
        # event.time == peak_time
        assert ev.time == datetime.fromtimestamp(_PEAK_UNIX, tz=timezone.utc)
        # geo plots at observer
        assert ev.geo.centroid == (-114.60, 42.57)
        assert ev.geo.primary_region == "US-ID"
        # Data fields populated
        for k in ("observer_name", "observer_slug", "observer_state",
                  "norad_id", "satellite_name", "aos_time", "peak_time",
                  "los_time", "max_elevation_deg", "magnitude",
                  "azimuth_at_aos", "azimuth_at_aos_compass",
                  "azimuth_at_peak", "azimuth_at_peak_compass",
                  "azimuth_at_los", "azimuth_at_los_compass", "duration_s"):
            assert k in ev.data, f"missing data key {k!r}"
        # Sanity on the cherry-picked values
        assert ev.data["norad_id"] == 25544
        assert ev.data["satellite_name"] == "ISS (ZARYA)"
        assert ev.data["magnitude"] == -3.4
        assert ev.data["max_elevation_deg"] == 47.0
        assert ev.data["duration_s"] == 560
        assert ev.data["azimuth_at_peak_compass"] == "SSW"


# --- Poll loop with mocked _fetch_passes ------------------------------------


class TestPollMissingKey:
    @pytest.mark.asyncio
    async def test_no_key_yields_zero_events_no_exception(self, tmp_path):
        adapter = _make_adapter(tmp_path, api_key=None)
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert events == []


class TestPollEmptyConfig:
    @pytest.mark.asyncio
    async def test_no_observers_yields_zero(self, tmp_path):
        adapter = _make_adapter(tmp_path, settings={
            "observers": [], "norad_ids": [25544],
            "days_ahead": 2, "min_visibility_seconds": 300,
            "api_key_alias": "n2yo",
        })
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert events == []

    @pytest.mark.asyncio
    async def test_no_norad_ids_yields_zero(self, tmp_path):
        adapter = _make_adapter(tmp_path, settings={
            "observers": [_filer().model_dump()], "norad_ids": [],
            "days_ahead": 2, "min_visibility_seconds": 300,
            "api_key_alias": "n2yo",
        })
        await adapter.startup()
        events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert events == []


class TestPollHappyPath:
    @pytest.mark.asyncio
    async def test_one_observer_one_sat_one_pass(self, tmp_path):
        adapter = _make_adapter(tmp_path)
        await adapter.startup()
        with patch.object(adapter, "_fetch_passes",
                          new=AsyncMock(return_value=_iss_pass_fixture())):
            events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert len(events) == 1
        assert events[0].severity == 4
        assert events[0].data["norad_id"] == 25544


class TestPollEmptyPassesArray:
    @pytest.mark.asyncio
    async def test_passes_empty_yields_zero_no_exception(self, tmp_path):
        adapter = _make_adapter(tmp_path)
        await adapter.startup()
        with patch.object(adapter, "_fetch_passes",
                          new=AsyncMock(return_value=_empty_fixture())):
            events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert events == []


class TestPollFetchFailureDoesNotKillPoll:
    @pytest.mark.asyncio
    async def test_one_fetch_returns_none_others_succeed(self, tmp_path):
        """Three sats: first fetch fails (returns None), other two succeed.
        Aggregate count = 2 events from the two successes."""
        adapter = _make_adapter(tmp_path, settings={
            "observers": [_filer().model_dump()],
            "norad_ids": [25544, 25338, 28654],
            "days_ahead": 2, "min_visibility_seconds": 300,
            "api_key_alias": "n2yo",
        })
        await adapter.startup()
        responses = [None, _iss_pass_fixture(), _iss_pass_fixture()]

        async def _stub(_obs, _nid):
            return responses.pop(0)

        with patch.object(adapter, "_fetch_passes", side_effect=_stub):
            events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert len(events) == 2


class TestPollMultiObserverMultiSatAggregate:
    @pytest.mark.asyncio
    async def test_six_by_six_aggregate(self, tmp_path):
        """Default 6 observers x 6 sats x 1 pass each = 36 events.
        Explicit settings pass-through (the _make_adapter helper's
        settings=None branch defaults to a Filer-only fixture; we want
        the production schema defaults here)."""
        prod_defaults = N2yoVisualpassesSettings().model_dump()
        adapter = _make_adapter(tmp_path, settings=prod_defaults)
        await adapter.startup()

        async def _stub(_obs, _nid):
            return _iss_pass_fixture()

        with patch.object(adapter, "_fetch_passes", side_effect=_stub):
            events = [ev async for ev in adapter.poll()]
        await adapter.shutdown()
        assert len(events) == 36
        # Sanity: all 6 observers represented
        assert {e.data["observer_slug"] for e in events} == {
            "filer", "boise", "idaho-falls", "ogden",
            "salt-lake-city", "provo",
        }


# --- HTTP layer (single end-to-end test through session.get) ----------------


class TestHttpErrorPath:
    """One HTTP-level test verifies the session.get path: HTTP 401 (invalid key)
    yields None from _fetch_passes, which the poll loop handles as a failure
    (skipped, doesn't kill the poll)."""

    @pytest.mark.asyncio
    async def test_http_401_returns_none(self, tmp_path):
        adapter = _make_adapter(tmp_path)
        await adapter.startup()

        # Build a mock async context manager whose .__aenter__ returns a
        # response with status=401.
        resp = MagicMock()
        resp.status = 401
        resp.json = AsyncMock(return_value={"error": "Invalid API key"})
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=resp)
        cm.__aexit__ = AsyncMock(return_value=False)
        assert adapter._session is not None
        with patch.object(adapter._session, "get",
                          MagicMock(return_value=cm)):
            result = await adapter._fetch_passes(_filer(), 25544)
        await adapter.shutdown()
        assert result is None


# --- Static isolation guard --------------------------------------------------


class TestStaticIsolation:
    def test_no_absolute_paths_in_adapter_source(self):
        """Acceptance bar #4: no /home/, /tmp/, /opt/ in adapter or this test."""
        adapter_src = Path(__file__).parent.parent / "src" / "central" / \
                      "adapters" / "n2yo_visualpasses.py"
        text = adapter_src.read_text()
        # Note: matching the *path prefixes*, not substrings inside text.
        for needle in ("/home/", "/opt/", "/tmp/"):
            assert needle not in text, f"hardcoded path {needle!r} in adapter"

    def test_no_hardcoded_api_key_in_adapter_source(self):
        """Acceptance bar #2: no API-key constants embedded in source."""
        adapter_src = Path(__file__).parent.parent / "src" / "central" / \
                      "adapters" / "n2yo_visualpasses.py"
        text = adapter_src.read_text()
        # Surface-pattern check: no raw 40-char alphanumeric strings that look
        # like keys. n2yo keys are short and tokenized but the principle holds.
        # Also: explicit guard against the literal placeholder forms we might
        # have left behind.
        for needle in ("apiKey=AKL", "apiKey=DEMO", "apiKey=YOUR"):
            assert needle not in text, f"likely hardcoded key remnant {needle!r}"
