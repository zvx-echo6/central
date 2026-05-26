"""Tests for the state_511_atis_cameras adapter (v0.9.6).

Fixture is a real /List/GetData/Cameras capture (2 cameras: one single-image
UDOT border camera, one multi-image RWIS camera):
  tests/fixtures/state_511_atis_cameras_sample.json

No conftest entry: dedup uses the supervisor-injected cursors.db (inherited
mixin); polling is stateless.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from central.adapter import SourceAdapter
from central.adapters.state_511_atis_cameras import (
    State511ATISCamerasAdapter,
    StateConfig,
    _parse_wkt,
)
from central.config_models import AdapterConfig

FIX = json.loads((Path(__file__).parent / "fixtures" / "state_511_atis_cameras_sample.json").read_text())
CAMS = FIX["data"]
ID = StateConfig(code="ID", base_url="https://511.idaho.gov")
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _cfg():
    return AdapterConfig(
        name="state_511_atis_cameras", enabled=True, cadence_s=600,
        settings={"states": [ID.model_dump()]}, updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def adapter(tmp_path):
    return State511ATISCamerasAdapter(_cfg(), MagicMock(), tmp_path / "cursors.db")


def test_wkt_parse():
    assert _parse_wkt("POINT (-112.198 42.0011)") == (42.0011, -112.198)  # (lat, lon)
    assert _parse_wkt(None) == (None, None)
    assert _parse_wkt("nonsense") == (None, None)


def test_dedup_key_shape(adapter):
    e = adapter._build_event(CAMS[0], ID)
    assert e.id == f"ID:cam:{CAMS[0]['id']}:{TODAY}"  # per-UTC-day bucketing


def test_build_event_with_image_url(adapter):
    e = adapter._build_event(CAMS[0], ID)
    assert e.category == "camera.state_511_atis_cameras"
    assert e.severity == 1
    assert e.data["image_url"] == "https://511.idaho.gov" + CAMS[0]["images"][0]["imageUrl"]
    assert e.data["source"] == CAMS[0]["source"]            # provenance surfaced
    assert e.data["roadway_name"] == CAMS[0]["roadway"]
    assert e.data["latitude"] is not None and e.data["longitude"] is not None


def test_build_event_multi_image(adapter):
    e = adapter._build_event(CAMS[1], ID)
    assert e.data["image_count"] == len(CAMS[1]["images"])
    assert e.data["image_count"] >= 2


def test_no_image_url_handled_gracefully(adapter):
    cam = {"id": 999, "roadway": "US-95", "location": "US-95 Somewhere", "source": "ITDNET",
           "direction": "Unknown", "images": [],
           "latLng": {"geography": {"wellKnownText": "POINT (-116.5 46.4)"}}}
    e = adapter._build_event(cam, ID)
    assert e is not None
    assert e.data["image_url"] is None and e.data["image_count"] == 0
    assert e.data["location_description"] == "US-95 Somewhere"


def test_subject_for_state_id(adapter):
    e = adapter._build_event(CAMS[0], ID)
    assert adapter.subject_for(e) == f"central.traffic_cameras.id.{CAMS[0]['id']}"


def test_subject_for_unknown_state(adapter):
    e = adapter._build_event(CAMS[0], StateConfig(code="", base_url="https://x"))
    assert adapter.subject_for(e) == f"central.traffic_cameras.unknown.{CAMS[0]['id']}"


def test_summary_partial_renders():
    from central.gui.routes import _derive_subject
    inner = {"location_description": "I-84 Mountain Home", "camera_id": 42}
    row = {"adapter": "state_511_atis_cameras", "data": {"data": {"data": inner}}}
    assert _derive_subject(row) == "Camera: I-84 Mountain Home"


def test_rows_partial_includes_img_tag():
    from central.gui.routes import _get_templates
    inner = {"image_url": "https://511.idaho.gov/map/Cctv/1", "roadway_name": "I-84",
             "location_description": "I-84 Mountain Home", "source": "ITDNET", "image_count": 1}
    row = {"data": {"data": {"data": inner}}}
    html = _get_templates().env.get_template("_event_rows/state_511_atis_cameras.html").render(event=row)
    assert "<img" in html
    assert "https://511.idaho.gov/map/Cctv/1" in html
    assert "ITDNET" in html  # source provenance rendered


@pytest.mark.asyncio
async def test_poll_paginates(tmp_path):
    a = State511ATISCamerasAdapter(_cfg(), MagicMock(), tmp_path / "cursors.db")
    await a.startup()

    def _cam(i):
        return {"id": i, "roadway": "I-84", "location": f"loc {i}", "direction": "N",
                "source": "ITDNET", "images": [{"imageUrl": f"/map/Cctv/{i}"}],
                "latLng": {"geography": {"wellKnownText": "POINT (-116.2 43.6)"}}}

    pages = {0: {"recordsTotal": 150, "data": [_cam(i) for i in range(100)]},
             100: {"recordsTotal": 150, "data": [_cam(i) for i in range(100, 150)]}}

    async def fake_page(base_url, start):
        return pages[start]

    a._fetch_page = fake_page
    events = [e async for e in a.poll()]
    await a.shutdown()
    assert len(events) == 150  # all 150 fetched across 2 pages, NOT truncated at the 100-row cap


def test_inherits_dedup_mixin():
    for m in ("is_published", "mark_published", "sweep_old_ids"):
        assert m not in State511ATISCamerasAdapter.__dict__, f"redefines {m}"
        assert getattr(State511ATISCamerasAdapter, m) is getattr(SourceAdapter, m)
