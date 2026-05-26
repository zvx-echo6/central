"""Tests for the v0.9.4 tomtom_flow Navi passthrough endpoint.

Exercises _flow_passthrough directly with mocked upstream + NATS (mirrors how
test_events_feed_frontend calls route handlers). Uses the real Orbis fixture
from v0.9.3 for the decode-and-publish path.
"""

import json
import re
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.gui import routes

PBF = (Path(__file__).parent / "fixtures" / "tomtom_flow_orbis.pbf").read_bytes()
PNG = b"\x89PNG\r\n\x1a\n" + b"fake-png-bytes"


@contextmanager
def patches(key="testkey", tile=PNG, js=None, fetch_exc=None):
    cs = MagicMock()
    cs.get_api_key = AsyncMock(return_value=key)
    fetch = AsyncMock(side_effect=fetch_exc) if fetch_exc else AsyncMock(return_value=tile)
    with patch("central.gui.routes.get_pool", return_value=MagicMock()), \
         patch("central.gui.routes.ConfigStore", return_value=cs), \
         patch("central.gui.routes._fetch_tomtom_tile", fetch), \
         patch("central.gui.routes.get_js", return_value=js):
        yield


@pytest.mark.asyncio
async def test_png_passthrough_no_publish():
    js = MagicMock()
    js.publish = AsyncMock()
    with patches(tile=PNG, js=js):
        resp = await routes._flow_passthrough(10, 181, 374, "png")
    assert resp.status_code == 200
    assert resp.body == PNG
    assert resp.media_type == "image/png"
    assert resp.headers["Cache-Control"] == "public, max-age=60"
    js.publish.assert_not_called()  # raster is unparseable -> no storage


@pytest.mark.asyncio
async def test_pbf_passthrough_publishes_segments():
    js = MagicMock()
    js.publish = AsyncMock()
    with patches(tile=PBF, js=js):
        resp = await routes._flow_passthrough(10, 181, 374, "pbf")
    assert resp.status_code == 200
    assert resp.body == PBF
    assert resp.media_type == "application/x-protobuf"
    assert js.publish.await_count > 50
    subject = js.publish.await_args_list[0].args[0]
    payload = json.loads(js.publish.await_args_list[0].args[1])
    assert subject == "central.traffic_flow.10.181.374"
    # dedup id collides with the polling adapter's minute-bucketed pattern
    assert re.match(r"^10/181/374:\d+:\d{4}-\d\d-\d\dT\d\d:\d\d$", payload["data"]["id"])


@pytest.mark.asyncio
async def test_missing_key_503():
    with patches(key=None):
        resp = await routes._flow_passthrough(10, 181, 374, "pbf")
    assert resp.status_code == 503
    assert b"not configured" in resp.body


@pytest.mark.asyncio
async def test_upstream_failure_502_no_publish():
    js = MagicMock()
    js.publish = AsyncMock()
    with patches(fetch_exc=RuntimeError("boom"), js=js):
        resp = await routes._flow_passthrough(10, 181, 374, "pbf")
    assert resp.status_code == 502
    assert b"upstream tile fetch failed" in resp.body
    js.publish.assert_not_called()


@pytest.mark.asyncio
async def test_key_never_leaks_on_error(caplog):
    secret = "SECRETKEY1234567890ABCDEF12345678"
    with caplog.at_level("WARNING"):
        with patches(key=secret, fetch_exc=RuntimeError(f"401 url=...key={secret}")):
            resp = await routes._flow_passthrough(10, 181, 374, "png")
    assert resp.status_code == 502
    assert secret not in resp.body.decode()
    assert secret not in caplog.text
    # the redacted error string rode in the log record's `error` extra field
    errs = " ".join(str(getattr(r, "error", "")) for r in caplog.records)
    assert secret not in errs
    assert "<KEY>" in errs


@pytest.mark.asyncio
async def test_publish_failure_still_returns_tile():
    js = MagicMock()
    js.publish = AsyncMock(side_effect=RuntimeError("nats down"))
    with patches(tile=PBF, js=js):
        resp = await routes._flow_passthrough(10, 181, 374, "pbf")
    assert resp.status_code == 200  # storage is best-effort; tile still served
    assert resp.body == PBF


@pytest.mark.asyncio
async def test_no_js_still_returns_tile():
    with patches(tile=PBF, js=None):  # GUI NATS not connected
        resp = await routes._flow_passthrough(10, 181, 374, "pbf")
    assert resp.status_code == 200
    assert resp.body == PBF
