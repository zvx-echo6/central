"""Tests for the WZDx adapter.

Fixtures are real captures trimmed to representative features:
  wzdx_utah_sample.json -- curl https://udottraffic.utah.gov/wzdx/udot/v40/data
      | jq '{road_event_feed_info, type, features: .features[0:1]}'
      (LineString, vehicle_impact "unknown", has event_status, no lanes)
  wzdx_iowa_sample.json -- curl https://iowa-atms.cloud-q-free.com/api/rest/dataprism/wzdx/wzdxfeed
      | jq '{... , features: [<all-lanes-open feature>, <some-lanes-closed feature>]}'
      (MultiPoint, lanes + types_of_work, no event_status)

No tests/conftest isolation entry is added: WZDx dedup uses the supervisor-
injected cursors.db and registry discovery is stateless, so there is no
adapter-owned cache to redirect (unlike nwis's NWIS_CACHE_DB_PATH).
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pydantic import ValidationError

from central.adapters.wzdx import (
    _DEFAULT_SEVERITY,
    _DEFAULT_STATES,
    _VEHICLE_IMPACT_SEVERITY,
    WZDxAdapter,
    WZDxSettings,
    _eligible,
    _flatten_geometry,
)
from central.config_models import AdapterConfig

FIX = Path(__file__).parent / "fixtures"
UTAH = json.loads((FIX / "wzdx_utah_sample.json").read_text())
IOWA = json.loads((FIX / "wzdx_iowa_sample.json").read_text())


def _cfg(settings=None):
    return AdapterConfig(
        name="wzdx", enabled=True, cadence_s=600,
        settings=settings or {}, updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def adapter(tmp_path):
    return WZDxAdapter(_cfg(), MagicMock(), tmp_path / "cursors.db")


@pytest.mark.parametrize("row,keep", [
    ({"format": "geojson", "active": True, "needapikey": False, "version": "4.1"}, True),
    ({"format": "geojson", "active": True, "needapikey": False, "version": "4"}, True),
    ({"format": "json", "active": True, "needapikey": False, "version": "4.1"}, False),
    ({"format": "geojson", "active": False, "needapikey": False, "version": "4.1"}, False),
    ({"format": "geojson", "active": True, "needapikey": True, "version": "4.1"}, False),
    ({"format": "geojson", "active": True, "needapikey": False, "version": "3.1"}, False),
    ({"format": "geojson", "active": True, "needapikey": False, "version": "CWZ 1.0"}, False),
])
def test_eligible_filter(row, keep):
    assert _eligible(row) is keep


def test_dedup_key(adapter):
    eu = adapter._build_event(UTAH["features"][0], {"feedname": "udot", "state": "utah"})
    ei = adapter._build_event(IOWA["features"][0], {"feedname": "idot", "state": "iowa"})
    assert eu.id == "UDOT-Construction:2365_eastbound"
    assert ei.id == "IowaDOT-WZDx:OpenTMS-Event22920571864-1"


@pytest.mark.parametrize("vi,sev", [
    ("all-lanes-closed", 3), ("some-lanes-closed", 2), ("all-lanes-open", 1),
    ("unknown", 1), (None, 1),
])
def test_severity(vi, sev):
    assert _VEHICLE_IMPACT_SEVERITY.get(vi, _DEFAULT_SEVERITY) == sev


@pytest.mark.parametrize("geom,expect", [
    ({"type": "LineString", "coordinates": [[-111.8, 40.7], [-111.6, 40.6]]}, (40.7, -111.8)),
    ({"type": "MultiPoint", "coordinates": [[-93.5, 40.7]]}, (40.7, -93.5)),
    ({"type": "Point", "coordinates": [-93.5, 40.7]}, (40.7, -93.5)),
    (None, (None, None)),
    ({"type": "Polygon", "coordinates": []}, (None, None)),
])
def test_flatten_geometry(geom, expect):
    assert _flatten_geometry(geom) == expect


def test_build_utah_shape(adapter):
    e = adapter._build_event(UTAH["features"][0], {"feedname": "udot", "state": "utah"})
    assert e.category == "work_zone.wzdx"
    assert e.severity == 1  # vehicle_impact "unknown"
    assert e.data["latitude"] is not None
    assert e.data["event_status"] == "active"  # Utah carries it


def test_build_iowa_shape(adapter):
    e0 = adapter._build_event(IOWA["features"][0], {"feedname": "idot", "state": "iowa"})
    e1 = adapter._build_event(IOWA["features"][1], {"feedname": "idot", "state": "iowa"})
    assert e0.severity == 1  # all-lanes-open
    assert e1.severity == 2  # some-lanes-closed
    assert e0.data["event_status"] is None  # Iowa lacks it -> no raise


def test_subject_from_registry(adapter):
    e = adapter._build_event(UTAH["features"][0], {"feedname": "udot", "state": "utah"})
    assert adapter.subject_for(e) == "central.traffic.work_zone.ut"


def test_subject_unknown(adapter):
    e = adapter._build_event(UTAH["features"][0], {"feedname": "x", "state": "n/a"})
    assert adapter.subject_for(e) == "central.traffic.work_zone.unknown"


def test_subject_geocoder_fallback(adapter):
    e = adapter._build_event(UTAH["features"][0], {"feedname": "x", "state": "n/a"})
    e.data["_enriched"] = {"geocoder": {"state": "Idaho"}}
    assert adapter.subject_for(e) == "central.traffic.work_zone.id"


def test_event_type_split(adapter):
    # Mirrors routes.py split_part(category, '.', 1) -> GUI event_type.
    e = adapter._build_event(UTAH["features"][0], {"feedname": "udot", "state": "utah"})
    assert e.category.split(".")[0] == "work_zone"


@pytest.mark.asyncio
async def test_poll_yields_events(tmp_path):
    # Explicit allowlist (UT+IA): Iowa is outside the v0.9.17 default set, so this
    # test sets states directly to exercise multi-feed fan-out + the json-skip.
    adapter = WZDxAdapter(_cfg({"states": ["UT", "IA"]}), MagicMock(), tmp_path / "cursors.db")
    await adapter.startup()
    registry = [
        {"format": "geojson", "active": True, "needapikey": False, "version": "4", "feedname": "udot", "state": "utah", "url": {"url": "u"}},
        {"format": "geojson", "active": True, "needapikey": False, "version": "4", "feedname": "idot", "state": "iowa", "url": {"url": "i"}},
        {"format": "json", "active": True, "needapikey": False, "version": "4", "feedname": "skip", "state": "ohio", "url": {"url": "s"}},
    ]
    adapter._fetch_registry = AsyncMock(return_value=registry)

    async def fake_feed(row):
        return {"udot": UTAH, "idot": IOWA}.get(row["feedname"], {"features": []})["features"]

    adapter._fetch_feed = fake_feed
    events = [e async for e in adapter.poll()]
    await adapter.shutdown()
    # Utah 1 + Iowa 2 = 3; the json feed is dropped by _discover.
    assert len(events) == 3
    assert {e.adapter for e in events} == {"wzdx"}


def test_summary_partial_renders_subject():
    # End-to-end through the real _event_summaries/wzdx.html partial selection.
    from central.gui.routes import _derive_subject
    flat = {"road_names": ["I-80"], "direction": "eastbound"}
    row = {"adapter": "wzdx", "data": {"data": {"data": flat}}}
    assert _derive_subject(row) == "Work zone on I-80 eastbound"


def test_summary_omits_unknown_direction():
    # direction "unknown" (common in e.g. AZ mcdot) must not leak into the subject.
    from central.gui.routes import _derive_subject
    flat = {"road_names": ["I-80"], "direction": "unknown"}
    row = {"adapter": "wzdx", "data": {"data": {"data": flat}}}
    assert _derive_subject(row) == "Work zone on I-80"


# --- v0.9.17: poll-time state allowlist ---------------------------------------

def _registry_row(state):
    return {"format": "geojson", "active": True, "needapikey": False,
            "version": "4", "feedname": state[:3], "state": state, "url": {"url": state}}


def test_default_states_when_unset():
    # Settings default + adapter both fall back to the Idaho-region 7-set, NOT "all".
    assert WZDxSettings().states == _DEFAULT_STATES
    assert set(_DEFAULT_STATES) == {"ID", "WA", "OR", "NV", "UT", "WY", "MT"}
    a = WZDxAdapter(_cfg({}), MagicMock(), Path("/tmp/unused.db"))
    assert a._states == set(_DEFAULT_STATES)


def test_null_states_uses_default():
    # The live DB row stores {"states": null} pre-deploy-DML; must not mean "all".
    a = WZDxAdapter(_cfg({"states": None}), MagicMock(), Path("/tmp/unused.db"))
    assert a._states == set(_DEFAULT_STATES)


def test_discover_filters_to_default_states(adapter):
    # adapter fixture has empty settings -> default 7-set. ID/UT in; IA/OH out.
    registry = [_registry_row(s) for s in ("idaho", "utah", "iowa", "ohio")]
    kept = {r["state"] for r in adapter._discover(registry)}
    assert kept == {"idaho", "utah"}


def test_discover_honours_explicit_allowlist(tmp_path):
    a = WZDxAdapter(_cfg({"states": ["id"]}), MagicMock(), tmp_path / "c.db")
    assert a._states == {"ID"}  # lower-case input is normalized
    registry = [_registry_row(s) for s in ("idaho", "utah", "iowa")]
    kept = {r["state"] for r in a._discover(registry)}
    assert kept == {"idaho"}


def test_malformed_state_code_rejected():
    with pytest.raises(ValidationError):
        WZDxSettings(states=["ID", "ZZ"])


def test_quota_estimate_informational():
    q = WZDxAdapter.quota_estimate(WZDxSettings(), 600)
    # 7 states -> 1 registry + 7 feeds = 8 GET/poll; 2_592_000/600 = 4320 polls.
    assert q["calls_per_month"] == 8 * (2_592_000 // 600)
    assert q["cap"] is None
    assert q["warn"] is False and q["blocked"] is False
    # Narrowing states lowers the estimate (the 82% delta the panel surfaces).
    assert WZDxAdapter.quota_estimate(
        WZDxSettings(states=["ID", "WA", "UT"]), 600
    )["calls_per_month"] < q["calls_per_month"]


def test_edit_page_renders_standalone_quota_panel():
    # v0.9.17: WZDx is a flat-config (checkboxes) adapter, so its quota panel
    # renders from the standalone block in adapters_edit.html (not the model_list
    # partial). Informational only -> "flash" with no warn/error class, cap=None.
    from starlette.requests import Request

    from central.gui import templates as gui_templates
    from central.gui.form_descriptors import describe_fields

    s = {"states": _DEFAULT_STATES}
    fields = describe_fields(WZDxAdapter.settings_schema, s)
    quota = WZDxAdapter.quota_estimate(WZDxSettings(**s), 600)
    ctx = {
        "operator": SimpleNamespace(username="admin"), "csrf_token": "x",
        "adapter": {"name": "wzdx", "display_name": "WZDx", "description": "",
                    "enabled": True, "cadence_s": 600, "settings": s, "paused_at": None,
                    "updated_at": None, "last_error": None},
        "fields": fields, "api_keys": [], "errors": None, "form_data": None,
        "tile_url": "https://t/{z}/{x}/{y}.png", "tile_attribution": "OSM",
        "api_key_missing": False, "requires_api_key_alias": None,
        "preview_rows": None, "preview_error": None, "quota": quota,
    }
    req = Request({"type": "http", "method": "GET", "path": "/", "headers": [], "query_string": b""})
    out = gui_templates.TemplateResponse(request=req, name="adapters_edit.html", context=ctx).body.decode()
    assert 'type="checkbox" name="states" value="ID"' in out  # checkboxes, default checked
    assert "model-list" not in out
    assert 'class="quota-panel flash "' in out  # no flash-warn/flash-error
    assert "upstream GET" in out and "free/uncapped" in out
    assert 'data-quota-cap="None"' in out
    assert "flash-warn" not in out and "flash-error" not in out
