"""Tests for the v0.11.0 celestrak_tle adapter."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from central.adapter import SourceAdapter
from central.adapters.celestrak_tle import (
    CelestrakTleAdapter,
    CelestrakTleSettings,
    _catnr_source_url,
    _decode_epoch,
    _decode_orbit,
    _group_source_url,
    _norad_id_from_line1,
    _parse_tle_groups,
)
from central.config_models import AdapterConfig


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "celestrak_stations.tle"


@pytest.fixture
def stations_tle_text() -> str:
    """Live CelesTrak stations TLE response captured at probe time."""
    return FIXTURE_PATH.read_text()


@pytest.fixture
def adapter(tmp_path: Path) -> CelestrakTleAdapter:
    cfg = AdapterConfig(
        name="celestrak_tle",
        enabled=True,
        cadence_s=14400,
        settings={"groups": ["stations"], "extra_norad_ids": []},
        updated_at=datetime.now(timezone.utc),
    )
    return CelestrakTleAdapter(cfg, MagicMock(), tmp_path / "cursors.db")


# --- 3-line group splitter --------------------------------------------------


def test_parse_tle_groups_real_stations_fixture(stations_tle_text):
    """The live stations fixture must split cleanly into 25 (name, l1, l2)."""
    groups = _parse_tle_groups(stations_tle_text)
    assert len(groups) == 25
    names = {n.strip() for n, _, _ in groups}
    assert "ISS (ZARYA)" in names
    # Every line1 starts '1 ', every line2 starts '2 '.
    for _, l1, l2 in groups:
        assert l1.startswith("1 ")
        assert l2.startswith("2 ")


def test_parse_tle_groups_tolerates_crlf_line_endings():
    text = "FOO\r\n1 00001U 00001A   26100.5  .0  0  0  0   1\r\n2 00001  0.0  0.0 0000000  0.0  0.0  1.00000000    0\r\n"
    groups = _parse_tle_groups(text)
    assert len(groups) == 1
    assert groups[0][0] == "FOO"


def test_parse_tle_groups_tolerates_trailing_blank_lines():
    text = "FOO\n1 00001U 00001A   26100.5  .0  0  0  0   1\n2 00001  0.0  0.0 0000000  0.0  0.0  1.00000000    0\n\n\n"
    groups = _parse_tle_groups(text)
    assert len(groups) == 1


def test_parse_tle_groups_skips_misaligned_segments():
    """A stray line that's not 1/2-prefixed shouldn't sink the rest."""
    text = (
        "FOO\n"
        "1 00001U 00001A   26100.5  .0  0  0  0   1\n"
        "2 00001  0.0  0.0 0000000  0.0  0.0  1.00000000    0\n"
        "garbage line that doesn't belong\n"
        "BAR\n"
        "1 00002U 00001A   26100.5  .0  0  0  0   1\n"
        "2 00002  0.0  0.0 0000000  0.0  0.0  1.00000000    0\n"
    )
    groups = _parse_tle_groups(text)
    assert {n for n, _, _ in groups} == {"FOO", "BAR"}


def test_parse_tle_groups_empty_input():
    assert _parse_tle_groups("") == []
    assert _parse_tle_groups("\n\n\n") == []


# --- Epoch decoding ---------------------------------------------------------


def test_decode_epoch_real_iss_tle():
    line1 = "1 25544U 98067A   26159.80410962  .00007129  00000+0  13425-3 0  9999"
    dt = _decode_epoch(line1)
    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert dt.year == 2026
    # day 159 of 2026 = June 8 (Jan 31 + Feb 28 + Mar 31 + Apr 30 + May 31 = 151, + 8 = 159)
    assert dt.month == 6 and dt.day == 8


def test_decode_epoch_y2k_pivot_2056_vs_1957():
    """NORAD Y2K rule: YY 00-56 → 2000-2056; YY 57-99 → 1957-1999."""
    # YY=56 → 2056
    line1_2056 = "1 00001U 00001A   56001.50000000  .0  0  0  0   1"
    dt = _decode_epoch(line1_2056)
    assert dt is not None and dt.year == 2056

    # YY=57 → 1957 (the dawn of orbital tracking)
    line1_1957 = "1 00001U 57001A   57001.50000000  .0  0  0  0   1"
    dt = _decode_epoch(line1_1957)
    assert dt is not None and dt.year == 1957

    # YY=99 → 1999
    line1_1999 = "1 00001U 99001A   99365.00000000  .0  0  0  0   1"
    dt = _decode_epoch(line1_1999)
    assert dt is not None and dt.year == 1999

    # YY=00 → 2000
    line1_2000 = "1 00001U 00001A   00001.00000000  .0  0  0  0   1"
    dt = _decode_epoch(line1_2000)
    assert dt is not None and dt.year == 2000


def test_decode_epoch_doy_arithmetic():
    """Day 1.0 = Jan 1 00:00; day 1.5 = Jan 1 12:00."""
    line1 = "1 00001U 00001A   26001.50000000  .0  0  0  0   1"
    dt = _decode_epoch(line1)
    assert dt is not None
    assert dt.year == 2026 and dt.month == 1 and dt.day == 1 and dt.hour == 12


def test_decode_epoch_returns_none_on_malformed():
    assert _decode_epoch("") is None
    assert _decode_epoch("too short") is None
    assert _decode_epoch("1 25544U 98067A   xxxxx.xxxxxxxx  .0  0  0  0   1") is None


# --- Orbit decoding ---------------------------------------------------------


def test_decode_orbit_real_iss_line2():
    line2 = "2 25544  51.6336 341.5878 0006923 148.5365 211.6039 15.49672912570453"
    orbit = _decode_orbit(line2)
    assert orbit is not None
    assert abs(orbit["inclination_deg"] - 51.6336) < 1e-6
    # Eccentricity carries an implicit leading "0." → 0.0006923
    assert abs(orbit["eccentricity"] - 0.0006923) < 1e-9
    assert abs(orbit["mean_motion_rev_per_day"] - 15.49672912) < 1e-9


def test_decode_orbit_implicit_leading_zero_on_eccentricity():
    """'0006923' must parse to 0.0006923, not 6923. The leading-0. is implicit."""
    line2 = "2 00001  45.0000 100.0000 1234567   0.0000   0.0000  1.00000000    0"
    orbit = _decode_orbit(line2)
    assert orbit is not None
    assert abs(orbit["eccentricity"] - 0.1234567) < 1e-9


def test_decode_orbit_zero_eccentricity():
    line2 = "2 00001  45.0000 100.0000 0000000   0.0000   0.0000  1.00000000    0"
    orbit = _decode_orbit(line2)
    assert orbit is not None
    assert orbit["eccentricity"] == 0.0


def test_decode_orbit_returns_none_on_malformed():
    assert _decode_orbit("") is None
    assert _decode_orbit("too short") is None
    # Garbage in inclination slot.
    assert _decode_orbit(
        "2 25544  XXXXXXX 341.5878 0006923 148.5365 211.6039 15.49672912570453"
    ) is None


# --- NORAD ID extraction ----------------------------------------------------


def test_norad_id_extraction_real_iss():
    line1 = "1 25544U 98067A   26159.80410962  .00007129  00000+0  13425-3 0  9999"
    assert _norad_id_from_line1(line1) == 25544


def test_norad_id_extraction_short_id_padded():
    line1 = "1 00007U 00007A   26100.5       .0  0  0  0   1"
    assert _norad_id_from_line1(line1) == 7


def test_norad_id_extraction_returns_none_on_malformed():
    assert _norad_id_from_line1("") is None
    assert _norad_id_from_line1("1 XXXXX") is None


# --- _build_event_record ----------------------------------------------------


def _real_iss_triple():
    return (
        "ISS (ZARYA)             ",
        "1 25544U 98067A   26159.80410962  .00007129  00000+0  13425-3 0  9999",
        "2 25544  51.6336 341.5878 0006923 148.5365 211.6039 15.49672912570453",
    )


def test_build_event_record_real_iss_shape(adapter):
    name, l1, l2 = _real_iss_triple()
    src = "https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=TLE"
    ev = adapter._build_event_record(name, l1, l2, src)
    assert ev is not None
    # Identity
    assert ev.adapter == "celestrak_tle"
    assert ev.category == "tle.celestrak_tle"
    assert ev.severity == 1
    # Dedup id = {norad_id}:{epoch_iso}
    assert ev.id.startswith("25544:")
    assert ":2026-06-08T" in ev.id  # day 159 of 2026 = June 8
    # Geo intentionally empty (TLEs are global state, no surface point)
    assert ev.geo.centroid is None
    assert ev.geo.bbox is None
    assert ev.geo.geometry is None
    # Data fields per spec
    assert ev.data["norad_id"] == 25544
    assert ev.data["satellite_name"] == "ISS (ZARYA)"
    assert ev.data["tle_line1"] == l1
    assert ev.data["tle_line2"] == l2
    assert ev.data["classification"] == "U"
    assert ev.data["intl_designator"] == "98067A"
    assert ev.data["source_url"] == src
    # _enriched.orbit bundle present
    orb = ev.data["_enriched"]["orbit"]
    assert abs(orb["inclination_deg"] - 51.6336) < 1e-6
    assert abs(orb["mean_motion_rev_per_day"] - 15.49672912) < 1e-9


def test_build_event_record_omits_orbit_bundle_on_parse_failure(adapter):
    """Malformed Line 2 → orbit bundle absent, event still publishes."""
    name = "BROKEN"
    l1 = "1 00001U 00001A   26100.50000000  .0  0  0  0   1"
    l2 = "2 00001  XXXXXX 341.5878 0006923 148.5365 211.6039 15.49672912570453"
    ev = adapter._build_event_record(name, l1, l2, "x")
    assert ev is not None
    assert "_enriched" not in ev.data  # parse failure → bundle absent


def test_build_event_record_returns_none_on_missing_norad_id(adapter):
    l1 = "1 XXXXXX 00001A   26100.50000000  .0  0  0  0   1"
    l2 = "2 00001  45.0 100.0 0000000  0.0  0.0  1.00000000    0"
    assert adapter._build_event_record("X", l1, l2, "x") is None


# --- subject_for ------------------------------------------------------------


def test_subject_for_uses_norad_id(adapter):
    ev = adapter._build_event_record(*_real_iss_triple(), "x")
    assert adapter.subject_for(ev) == "central.sat.tle.25544"


def test_subject_for_falls_back_when_norad_id_missing(adapter):
    """Defensive: if the data dict somehow lacks norad_id, route to .unknown."""
    from central.models import Event, Geo
    ev = Event(
        id="x", adapter="celestrak_tle", category="tle.celestrak_tle",
        time=datetime.now(timezone.utc), severity=1, geo=Geo(), data={},
    )
    assert adapter.subject_for(ev) == "central.sat.tle.unknown"


# --- Source-URL builders ----------------------------------------------------


def test_source_url_builders():
    assert _group_source_url("stations") == (
        "https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=TLE"
    )
    assert _catnr_source_url(25544) == (
        "https://celestrak.org/NORAD/elements/gp.php?CATNR=25544&FORMAT=TLE"
    )


# --- Settings + adapter scaffolding ----------------------------------------


def test_default_settings_match_spec():
    s = CelestrakTleSettings()
    assert s.groups == ["stations", "weather", "amateur"]
    assert s.extra_norad_ids == []


def test_inherits_dedup_mixin_from_source_adapter(tmp_path):
    """v0.9.1 regression guard: every adapter must inherit the dedup mixin
    so the supervisor's is_published() / mark_published() calls dispatch."""
    assert issubclass(CelestrakTleAdapter, SourceAdapter)
    a = CelestrakTleAdapter(
        AdapterConfig(
            name="celestrak_tle", enabled=False, cadence_s=14400,
            settings={}, updated_at=datetime.now(timezone.utc),
        ),
        MagicMock(),
        tmp_path / "cursors.db",  # tmp_path: pytest's per-test isolation
    )
    # Inherited methods exposed on the instance.
    assert callable(a.is_published)
    assert callable(a.mark_published)
    assert callable(a.sweep_old_ids)


@pytest.mark.asyncio
async def test_apply_config_updates_groups_and_extras(adapter):
    new_cfg = AdapterConfig(
        name="celestrak_tle", enabled=True, cadence_s=14400,
        settings={"groups": ["amateur"], "extra_norad_ids": [25544, 33591]},
        updated_at=datetime.now(timezone.utc),
    )
    await adapter.apply_config(new_cfg)
    assert adapter._groups == ["amateur"]
    assert adapter._extra_ids == {25544, 33591}


# --- Group / extras collapse + dedup logic ---------------------------------


@pytest.mark.asyncio
async def test_extra_norad_ids_collapse_with_group_membership(tmp_path, monkeypatch):
    """If an extra_norad_id is already in a configured group's roster, the
    extra-fetch is SKIPPED (no second CATNR request)."""
    cfg = AdapterConfig(
        name="celestrak_tle", enabled=True, cadence_s=14400,
        settings={"groups": ["stations"], "extra_norad_ids": [25544, 99999]},
        updated_at=datetime.now(timezone.utc),
    )
    adapter = CelestrakTleAdapter(cfg, MagicMock(), tmp_path / "cursors.db")
    await adapter.startup()
    try:
        # stations response contains ISS (NORAD 25544) -- so the 25544 extra
        # should collapse; only NORAD 99999 needs a CATNR fetch.
        stations_text = (
            "ISS (ZARYA)             \n"
            "1 25544U 98067A   26100.50000000  .0  0  0  0   1\n"
            "2 25544  51.6 0.0 0006923 0.0 0.0 15.5    0\n"
        )
        catnr_99999_text = (
            "MYSAT                   \n"
            "1 99999U 00001A   26100.50000000  .0  0  0  0   1\n"
            "2 99999  45.0 0.0 0006923 0.0 0.0 14.0    0\n"
        )

        fetch_urls: list[str] = []

        async def _fake_fetch(url):
            fetch_urls.append(url)
            if "GROUP=stations" in url:
                return stations_text
            if "CATNR=99999" in url:
                return catnr_99999_text
            return None

        monkeypatch.setattr(adapter, "_fetch_url", _fake_fetch)
        events = [e async for e in adapter.poll()]

        # ONE group fetch + ONE catnr fetch (for 99999), NOT two catnr fetches.
        assert any("GROUP=stations" in u for u in fetch_urls)
        assert any("CATNR=99999" in u for u in fetch_urls)
        assert not any("CATNR=25544" in u for u in fetch_urls), (
            "25544 was in the stations group -- CATNR fetch should have been "
            "skipped per the dedup-collapse rule"
        )
        norad_ids = {e.data["norad_id"] for e in events}
        assert norad_ids == {25544, 99999}
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_duplicate_across_groups_yields_once(tmp_path, monkeypatch):
    """If a satellite appears in two configured groups, the adapter yields
    only ONE Event for it (first-group-wins by configured order)."""
    cfg = AdapterConfig(
        name="celestrak_tle", enabled=True, cadence_s=14400,
        settings={"groups": ["stations", "amateur"], "extra_norad_ids": []},
        updated_at=datetime.now(timezone.utc),
    )
    adapter = CelestrakTleAdapter(cfg, MagicMock(), tmp_path / "cursors.db")
    await adapter.startup()
    try:
        # Both groups return the same NORAD 25544 record.
        same_sat = (
            "ISS (ZARYA)             \n"
            "1 25544U 98067A   26100.50000000  .0  0  0  0   1\n"
            "2 25544  51.6 0.0 0006923 0.0 0.0 15.5    0\n"
        )

        async def _fake_fetch(url):
            return same_sat

        monkeypatch.setattr(adapter, "_fetch_url", _fake_fetch)
        events = [e async for e in adapter.poll()]
        assert len(events) == 1
        assert events[0].data["norad_id"] == 25544
    finally:
        await adapter.shutdown()


@pytest.mark.asyncio
async def test_poll_yields_all_stations_from_real_fixture(tmp_path, monkeypatch, stations_tle_text):
    """End-to-end with the live fixture: 25 stations sats → 25 events."""
    cfg = AdapterConfig(
        name="celestrak_tle", enabled=True, cadence_s=14400,
        settings={"groups": ["stations"], "extra_norad_ids": []},
        updated_at=datetime.now(timezone.utc),
    )
    adapter = CelestrakTleAdapter(cfg, MagicMock(), tmp_path / "cursors.db")
    await adapter.startup()
    try:
        async def _fake_fetch(url):
            return stations_tle_text
        monkeypatch.setattr(adapter, "_fetch_url", _fake_fetch)
        events = [e async for e in adapter.poll()]
        assert len(events) == 25
        # ISS (NORAD 25544) must be among them, with severity 1 and empty geo.
        iss = [e for e in events if e.data["norad_id"] == 25544]
        assert len(iss) == 1
        assert iss[0].severity == 1
        assert iss[0].geo.centroid is None
        assert iss[0].category == "tle.celestrak_tle"
    finally:
        await adapter.shutdown()


# --- Stream registry + family mapping (the v0.11.0 wiring) -----------------


def test_central_sat_registered_in_streams():
    from central.streams import STREAMS
    sat = [s for s in STREAMS if s.name == "CENTRAL_SAT"]
    assert len(sat) == 1
    assert sat[0].subject_filter == "central.sat.>"
    assert sat[0].event_bearing is True


def test_central_sat_in_supervisor_family_map():
    """v0.11.0 set this to ('tle',); v0.11.1 extended to ('tle', 'pass') so
    satpass_predict events also route to CENTRAL_SAT."""
    from central.supervisor import STREAM_CATEGORY_DOMAINS
    assert "tle" in STREAM_CATEGORY_DOMAINS["CENTRAL_SAT"]


def test_celestrak_tle_in_space_adapter_group():
    """ADAPTER_GROUPS['Space'] must include 'celestrak_tle' alongside SWPC."""
    from central.gui.routes import ADAPTER_GROUPS
    assert "celestrak_tle" in ADAPTER_GROUPS["Space"]


# --- Partials render cleanly with fixture (v0.10.0 pattern) -----------------


def test_summary_partial_renders_cleanly_with_real_data(adapter, tmp_path):
    """Render the _event_summaries/celestrak_tle.html partial against a real
    ISS Event payload; verify the rendered string contains the satellite
    name, NORAD id, orbit period (min), and inclination -- per the spec.
    """
    from jinja2 import Environment, FileSystemLoader
    templates_dir = Path(__file__).parent.parent / "src" / "central" / "gui" / "templates"
    env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=True)
    tmpl = env.get_template("_event_summaries/celestrak_tle.html")
    ev = adapter._build_event_record(*_real_iss_triple(), "x")
    rendered = tmpl.render(event={
        "data": {"data": {"data": ev.model_dump(mode="json")["data"]}}
    }).strip()
    assert "ISS (ZARYA)" in rendered, f"got: {rendered!r}"
    assert "25544" in rendered
    assert "min orbit at" in rendered  # period rendering
    assert "51.6°" in rendered  # inclination


def test_row_partial_renders_cleanly_with_real_data(adapter):
    """Render the _event_rows/celestrak_tle.html partial; verify the four
    labeled rows the spec calls for: Satellite, NORAD ID, Epoch, Inclination,
    Orbital period."""
    from jinja2 import Environment, FileSystemLoader
    templates_dir = Path(__file__).parent.parent / "src" / "central" / "gui" / "templates"
    env = Environment(loader=FileSystemLoader(str(templates_dir)), autoescape=True)
    tmpl = env.get_template("_event_rows/celestrak_tle.html")
    ev = adapter._build_event_record(*_real_iss_triple(), "x")
    rendered = tmpl.render(event={
        "data": {"data": {"data": ev.model_dump(mode="json")["data"]}}
    })
    assert "<dt>Satellite</dt>" in rendered and "ISS (ZARYA)" in rendered
    assert "<dt>NORAD ID</dt>" in rendered and "25544" in rendered
    assert "<dt>Epoch</dt>" in rendered
    assert "<dt>Inclination</dt>" in rendered and "51.63°" in rendered
    assert "<dt>Orbital period</dt>" in rendered and "min" in rendered
