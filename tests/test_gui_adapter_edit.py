"""v0.9.9 — adapter edit form: generic model_list editor (fixes the
describe_fields list[BaseModel] 500 across 4 adapters), TomTom bbox validation,
and quota gating."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.datastructures import FormData
from starlette.requests import Request

from central.gui import templates as gui_templates
from central.gui.form_descriptors import describe_fields
from central.gui.routes import adapters_edit_form, adapters_edit_submit
from central.adapters.tomtom_incidents import (
    TomTomIncidentsAdapter, TomTomIncidentsSettings, TOMTOM_FREE_TIER_CALLS_PER_MONTH,
)

_BBOXES = [
    {"name": "treasure_valley_ext", "min_lon": -117.05, "min_lat": 43.30,
     "max_lon": -116.00, "max_lat": 44.30, "state_code": "ID", "cadence_s": 3600},
    {"name": "mountain_home_corridor", "min_lon": -116.00, "min_lat": 42.65,
     "max_lon": -114.85, "max_lat": 43.40, "state_code": "ID", "cadence_s": 5400},
    {"name": "magic_valley_burley", "min_lon": -114.85, "min_lat": 42.30,
     "max_lon": -113.40, "max_lat": 42.95, "state_code": "ID", "cadence_s": 3600},
]
_SETTINGS = {"bboxes": _BBOXES, "api_key_alias": "tomtom"}
# A small (~2.3k km^2) valid box used as the base for POST tests.
_SMALL = {"min_lon": "-117.0", "min_lat": "43.0", "max_lon": "-116.5", "max_lat": "43.5"}


def _render(name, ctx):
    req = Request({"type": "http", "method": "GET", "path": "/", "headers": [], "query_string": b""})
    return gui_templates.TemplateResponse(request=req, name=name, context=ctx).body.decode()


def _ctx(settings, fields, quota, name="tomtom_incidents", display="TomTom"):
    return {
        "operator": SimpleNamespace(username="admin"), "csrf_token": "x",
        "adapter": {"name": name, "display_name": display, "description": "", "enabled": True,
                    "cadence_s": 1800, "settings": settings, "paused_at": None,
                    "updated_at": None, "last_error": None},
        "fields": fields, "api_keys": [{"alias": "tomtom"}], "errors": None, "form_data": None,
        "tile_url": "https://t/{z}/{x}/{y}.png", "tile_attribution": "OSM",
        "api_key_missing": False, "requires_api_key_alias": "tomtom",
        "preview_rows": None, "preview_error": None, "quota": quota,
    }


class TestModelListRender:
    def test_tomtom_renders_rows_and_quota(self):
        fields = describe_fields(TomTomIncidentsAdapter.settings_schema, _SETTINGS)
        quota = TomTomIncidentsAdapter.quota_estimate(TomTomIncidentsSettings(**_SETTINGS), 1800)
        out = _render("adapters_edit.html", _ctx(_SETTINGS, fields, quota))
        assert "treasure_valley_ext" in out and "magic_valley_burley" in out
        assert "model-list" in out and 'name="bboxes-0-name"' in out
        assert "free tier" in out  # quota panel rendered

    def test_quota_panel_exposes_client_recompute_constants(self):
        # v0.9.15: data-quota-* attrs let the client JS mirror the server
        # formula with no hardcoded magic numbers.
        fields = describe_fields(TomTomIncidentsAdapter.settings_schema, _SETTINGS)
        quota = TomTomIncidentsAdapter.quota_estimate(TomTomIncidentsSettings(**_SETTINGS), 1800)
        out = _render("adapters_edit.html", _ctx(_SETTINGS, fields, quota))
        assert 'data-quota-cap="2500"' in out
        assert 'data-quota-spm="2592000"' in out
        assert 'data-quota-default="1800"' in out

    def test_quota_panel_wraps_detail_and_msg_for_live_update(self):
        # v0.9.15: detail/msg split into spans so the IIFE can rewrite the text
        # and warn/block state live. JS exec needs a browser, so this is
        # structural only -- live behaviour (cadence edit + Add/Delete row)
        # was eyeballed manually against /adapters/tomtom_incidents.
        fields = describe_fields(TomTomIncidentsAdapter.settings_schema, _SETTINGS)
        quota = TomTomIncidentsAdapter.quota_estimate(TomTomIncidentsSettings(**_SETTINGS), 1800)
        out = _render("adapters_edit.html", _ctx(_SETTINGS, fields, quota))
        assert '<span class="quota-detail">' in out
        assert '<span class="quota-msg">' in out

    def test_nws_region_intact_no_model_list(self):
        from central.adapters.nws import NWSSettings
        s = {"contact_email": "a@b.com",
             "region": {"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}}
        out = _render("adapters_edit.html",
                      _ctx(s, describe_fields(NWSSettings, s), None, name="nws", display="NWS"))
        assert "region-map" in out                 # region picker intact
        assert "model-list" not in out and "free tier" not in out

    def test_tomtom_flow_renders_tiles_editor(self):
        from central.adapters.tomtom_flow import TomTomFlowAdapter
        s = {"tiles": [{"z": 10, "x": 181, "y": 373}], "api_key_alias": "tomtom"}
        out = _render("adapters_edit.html",
                      _ctx(s, describe_fields(TomTomFlowAdapter.settings_schema, s), None,
                           name="tomtom_flow", display="Flow"))
        assert "model-list" in out and 'name="tiles-0-z"' in out


class TestDescribeFieldsModelList:
    def test_bbox_is_model_list_with_subfields(self):
        bf = next(f for f in describe_fields(TomTomIncidentsSettings, _SETTINGS) if f.name == "bboxes")
        assert bf.widget == "model_list"
        assert [s.name for s in bf.sub_fields] == \
            ["name", "min_lon", "min_lat", "max_lon", "max_lat", "state_code", "cadence_s"]
        assert bf.current_value[0]["name"] == "treasure_valley_ext"


def _post_req(pairs, cadence_s="1800"):
    req = MagicMock()
    req.state.operator = SimpleNamespace(id=1, username="admin")
    req.state.csrf_token = "x"
    req.form = AsyncMock(return_value=FormData(
        [("csrf_token", "x"), ("enabled", "on"), ("cadence_s", cadence_s)] + pairs))
    return req


def _pool(settings=_SETTINGS):
    conn = AsyncMock()
    conn.fetchrow.side_effect = [
        {"name": "tomtom_incidents", "enabled": True, "cadence_s": 1800, "settings": settings,
         "paused_at": None, "updated_at": None, "last_error": None},
        {"map_tile_url": None, "map_attribution": None},
    ]
    conn.fetch.return_value = [{"alias": "tomtom"}]
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    return pool, conn


def _bbox_pairs(i, name, state="ID", cadence=None, **coords):
    c = {**_SMALL, **coords}
    p = [(f"bboxes-{i}-name", name), (f"bboxes-{i}-state_code", state),
         (f"bboxes-{i}-min_lon", c["min_lon"]), (f"bboxes-{i}-min_lat", c["min_lat"]),
         (f"bboxes-{i}-max_lon", c["max_lon"]), (f"bboxes-{i}-max_lat", c["max_lat"])]
    if cadence is not None:
        p.append((f"bboxes-{i}-cadence_s", cadence))
    return p


async def _submit_expect_422(pairs, cadence_s="1800"):
    pool, _ = _pool()
    tmpl = MagicMock(); tmpl.TemplateResponse.return_value = MagicMock()
    with patch("central.gui.routes._get_templates", return_value=tmpl), \
         patch("central.gui.routes.get_pool", return_value=pool), \
         patch("central.gui.routes.adapter_has_resolved_api_key",
               new=AsyncMock(return_value=(True, "tomtom"))):
        await adapters_edit_submit(_post_req(pairs + [("api_key_alias", "tomtom")], cadence_s),
                                   "tomtom_incidents")
    ca = tmpl.TemplateResponse.call_args
    assert ca.kwargs["status_code"] == 422
    return ca.kwargs["context"]["errors"]


class TestBboxValidation:
    @pytest.mark.asyncio
    async def test_valid_persists(self):
        pool, conn = _pool()
        captured = {}
        async def cap(q, *a):
            if "UPDATE config.adapters" in q:
                captured["s"] = a[2]
        conn.execute.side_effect = cap
        pairs = _bbox_pairs(0, "metro", cadence="3600") + [("api_key_alias", "tomtom")]
        with patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.write_audit", new=AsyncMock()):
            res = await adapters_edit_submit(_post_req(pairs), "tomtom_incidents")
        assert res.status_code == 302
        assert captured["s"]["bboxes"][0]["name"] == "metro"

    @pytest.mark.asyncio
    async def test_oversized_422(self):
        errs = await _submit_expect_422(
            _bbox_pairs(0, "big", min_lon="-120", min_lat="30", max_lon="-100", max_lat="45"))
        assert "bboxes" in errs

    @pytest.mark.asyncio
    async def test_degenerate_422(self):
        errs = await _submit_expect_422(_bbox_pairs(0, "bad", min_lon="-100", max_lon="-110"))
        assert "bboxes" in errs

    @pytest.mark.asyncio
    async def test_duplicate_names_422(self):
        errs = await _submit_expect_422(_bbox_pairs(0, "dup") + _bbox_pairs(1, "dup"))
        assert "bboxes" in errs

    @pytest.mark.asyncio
    async def test_cadence_floor_422(self):
        errs = await _submit_expect_422(_bbox_pairs(0, "metro", cadence="30"))
        assert "bboxes" in errs

    @pytest.mark.asyncio
    async def test_quota_block_422(self):
        # top cadence 300, one bbox cadence 60 -> 8,640/mo > 2,500 cap
        errs = await _submit_expect_422(_bbox_pairs(0, "metro", cadence="60"), cadence_s="300")
        assert "bboxes" in errs and "free-tier" in errs["bboxes"]


class TestGetRoute:
    @pytest.mark.asyncio
    async def test_get_200_with_quota_in_context(self):
        pool, _ = _pool()
        tmpl = MagicMock(); tmpl.TemplateResponse.return_value = MagicMock(status_code=200)
        req = MagicMock()
        req.state.operator = SimpleNamespace(id=1, username="admin")
        req.state.csrf_token = "x"
        with patch("central.gui.routes._get_templates", return_value=tmpl), \
             patch("central.gui.routes.get_pool", return_value=pool), \
             patch("central.gui.routes.adapter_has_resolved_api_key",
                   new=AsyncMock(return_value=(True, "tomtom"))):
            await adapters_edit_form(req, "tomtom_incidents")
        ctx = tmpl.TemplateResponse.call_args.kwargs["context"]
        assert ctx["quota"]["calls_per_month"] == 1920  # 720+480+720
        assert ctx["quota"]["blocked"] is False


class TestBboxMapPreview:
    """v0.9.10 — read-only Leaflet preview of bbox rows in the model_list editor."""

    def test_tomtom_renders_bbox_map(self):
        """bbox row model (min/max lon+lat) → map div + Leaflet init present."""
        fields = describe_fields(TomTomIncidentsAdapter.settings_schema, _SETTINGS)
        quota = TomTomIncidentsAdapter.quota_estimate(TomTomIncidentsSettings(**_SETTINGS), 1800)
        out = _render("adapters_edit.html", _ctx(_SETTINGS, fields, quota))
        assert "bbox-map" in out                            # map container present
        assert "L.map(" in out and "L.rectangle(" in out    # Leaflet init present

    def test_state_511_atis_no_bbox_map(self):
        """Non-bbox model_list (StateConfig) → generic editor, no map (no regression)."""
        from central.adapters.state_511_atis import State511ATISAdapter
        s = {"states": [{"code": "ID", "base_url": "https://511.idaho.gov"}]}
        out = _render("adapters_edit.html",
                      _ctx(s, describe_fields(State511ATISAdapter.settings_schema, s), None,
                           name="state_511_atis", display="511 ATIS"))
        assert "model-list" in out      # generic editor still renders
        assert "bbox-map" not in out    # but no map div
