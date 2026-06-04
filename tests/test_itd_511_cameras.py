"""Tests for the itd_511_cameras adapter (v0.10.0).

Fixture covers 4 cameras: ITDNET, ACHD, UDOT (cross-border per v0.10.0
finding 4), and an RWIS multi-view (to exercise the additional_views capture):
  tests/fixtures/itd_511_cameras_sample.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.adapter import SourceAdapter
from central.adapters.itd_511 import _Transient
from central.adapters.itd_511_cameras import NATIVE_SOURCES, Itd511CamerasAdapter
from central.config_models import AdapterConfig

FIX = Path(__file__).parent / "fixtures"
CAMS = json.loads((FIX / "itd_511_cameras_sample.json").read_text())


def _cfg():
    return AdapterConfig(
        name="itd_511_cameras", enabled=True, cadence_s=600,
        settings={"api_key_alias": "idaho_511"},
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def adapter(tmp_path):
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value="testkey-32chars-deadbeefdeadbeef")
    return Itd511CamerasAdapter(_cfg(), cs, tmp_path / "cursors.db")


def test_class_attributes_match_spec():
    assert Itd511CamerasAdapter.name == "itd_511_cameras"
    assert Itd511CamerasAdapter.data_class == "telemetry"
    assert Itd511CamerasAdapter.requires_api_key == "idaho_511"
    assert Itd511CamerasAdapter.default_cadence_s == 600


def test_build_event_category_and_subject(adapter):
    e = adapter._build_event(CAMS[0])
    assert e.category == "camera.itd_511_cameras"
    assert e.adapter == "itd_511_cameras"
    assert adapter.subject_for(e) == f"central.traffic_cameras.us.id.{CAMS[0]['Id']}"


def test_dedup_id_per_utc_day(adapter):
    e = adapter._build_event(CAMS[0])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert e.id == f"idaho_511:cam:{CAMS[0]['Id']}:{today}"


def test_image_url_passthrough(adapter):
    e = adapter._build_event(CAMS[0])
    assert e.data["image_url"] == CAMS[0]["Views"][0]["Url"]
    assert e.data["image_url"].startswith("https://511.idaho.gov/map/Cctv/")


def test_source_jurisdiction_preserves_border_cameras(adapter):
    """Per v0.10.0 finding 4: ITD aggregates ~1.2% cross-DOT mirrors (UDOT,
    ODOT, WYDOT, ...). Region stays US-ID; source_jurisdiction preserves the
    raw upstream Source value for downstream re-bucketing."""
    udot = next((c for c in CAMS if c["Source"] == "UDOT"), None)
    assert udot is not None, "fixture must include a UDOT cross-border camera"
    e = adapter._build_event(udot)
    assert e.data["source_jurisdiction"] == "UDOT"
    assert e.data["source"] == "UDOT"
    assert e.geo.primary_region == "US-ID"  # uniform Idaho tagging per locked decision
    assert e.geo.regions == ["US-ID"]


def test_multiple_views_captured(adapter):
    multi = next((c for c in CAMS if len(c.get("Views") or []) > 1), None)
    assert multi is not None, "fixture must include a multi-view camera"
    e = adapter._build_event(multi)
    assert e.data["view_count"] == len(multi["Views"])
    assert e.data["additional_views"] == [v["Url"] for v in multi["Views"][1:]]


def test_single_view_has_empty_additional_views(adapter):
    single = next((c for c in CAMS if len(c.get("Views") or []) == 1), None)
    assert single is not None, "fixture must include a single-view camera"
    e = adapter._build_event(single)
    assert e.data["additional_views"] == []
    assert e.data["view_count"] == 1


def test_build_event_returns_none_without_id(adapter):
    assert adapter._build_event({"Source": "ITDNET"}) is None


def test_severity_is_1_for_telemetry(adapter):
    e = adapter._build_event(CAMS[0])
    assert e.severity == 1


@pytest.mark.asyncio
async def test_poll_yields_one_event_per_camera(adapter):
    await adapter.startup()
    adapter._fetch_cameras = AsyncMock(return_value=CAMS)
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert len(events) == len(CAMS)
    assert all(e.adapter == "itd_511_cameras" for e in events)
    assert all(e.category == "camera.itd_511_cameras" for e in events)


@pytest.mark.asyncio
async def test_poll_skips_cleanly_without_api_key(tmp_path):
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value=None)
    adapter = Itd511CamerasAdapter(_cfg(), cs, tmp_path / "cursors.db")
    await adapter.startup()
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert events == []


def test_summary_partial_renders():
    from central.gui.routes import _derive_subject
    inner = {"camera_id": 1, "location": "I-15 UT/ID State Line UT", "roadway": "I-15"}
    row = {"adapter": "itd_511_cameras", "data": {"data": {"data": inner}}}
    assert _derive_subject(row) == "Camera: I-15 UT/ID State Line UT"


def test_summary_partial_falls_back_to_id_when_location_missing():
    from central.gui.routes import _derive_subject
    inner = {"camera_id": 42}
    row = {"adapter": "itd_511_cameras", "data": {"data": {"data": inner}}}
    assert _derive_subject(row) == "Camera: #42"


def test_inherits_dedup_mixin_from_source_adapter():
    for m in ("is_published", "mark_published", "sweep_old_ids"):
        assert m not in Itd511CamerasAdapter.__dict__, f"redefines {m}"
        assert getattr(Itd511CamerasAdapter, m) is getattr(SourceAdapter, m)


# --- BUG E: poll() must catch _Transient (tenacity reraise after retries) ---

@pytest.mark.asyncio
async def test_poll_catches_transient_after_exhausted_retries(adapter):
    """BUG E regression: cameras.poll() except tuple must include _Transient
    so tenacity's reraise of a persistent 5xx or 429 (after exhausted
    retries) does NOT crash the whole poll cycle."""
    await adapter.startup()

    async def boom_transient():
        raise _Transient("503 persistent")

    adapter._fetch_cameras = boom_transient
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    assert events == []  # poll exited cleanly, didn't raise


# --- BUG D2: NATIVE_SOURCES allow-list lives at the adapter, not the partial -

def test_native_sources_module_constant():
    assert NATIVE_SOURCES == frozenset({"ITDNET", "Idaho511", "ACHD", "RWIS"})


def test_is_native_source_flag_set_per_camera(adapter):
    """Border-region UDOT camera is non-native; ITDNET is native."""
    udot = next(c for c in CAMS if c["Source"] == "UDOT")
    itdnet = next(c for c in CAMS if c["Source"] == "ITDNET")
    assert adapter._build_event(udot).data["is_native_source"] is False
    assert adapter._build_event(itdnet).data["is_native_source"] is True


def test_row_partial_does_not_hardcode_source_list(adapter):
    """D2 regression: the cross-DOT-mirror annotation is driven by
    data.is_native_source — the partial must NOT carry the source allow-list
    itself ([[feedback_no_hardcoding]])."""
    from central.gui.routes import _get_templates
    tmpl = _get_templates().env.get_template("_event_rows/itd_511_cameras.html")
    udot_evt = adapter._build_event(next(c for c in CAMS if c["Source"] == "UDOT"))
    itdnet_evt = adapter._build_event(next(c for c in CAMS if c["Source"] == "ITDNET"))
    udot_html = tmpl.render(event={"data": {"data": {"data": udot_evt.data}}})
    itdnet_html = tmpl.render(event={"data": {"data": {"data": itdnet_evt.data}}})
    assert "(cross-DOT mirror)" in udot_html
    assert "(cross-DOT mirror)" not in itdnet_html
    # Audit: the partial source on disk must not contain the allow-list.
    partial = Path(__file__).resolve().parents[1] / (
        "src/central/gui/templates/_event_rows/itd_511_cameras.html"
    )
    text = partial.read_text()
    for name in NATIVE_SOURCES:
        assert name not in text, f"partial hardcodes {name}"


# --- BUG D3: assert→if-raise on the cameras sibling --------------------------

@pytest.mark.asyncio
async def test_fetch_session_unset_raises_runtime(adapter):
    """D3 regression: asserts strip under python -O; the session-not-started
    precondition must hold even with optimizations."""
    assert adapter._session is None
    with pytest.raises(RuntimeError, match="session not started"):
        await adapter._fetch_cameras()
