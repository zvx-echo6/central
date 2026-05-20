"""Tests for NaviBackend (composed Navi /api/reverse endpoint).

HTTP is exercised via patching the backend's `_fetch` (the codebase has no
aioresponses/respx dep); URL construction is asserted on the pure `_url`
helper. An env-gated integration smoke against the live Navi endpoint is
skipped by default.
"""

import os
from unittest.mock import AsyncMock

import pytest

from central.enrichment.backends.navi import NaviBackend
from central.enrichment.geocoder import GEOCODER_FIELDS, all_null_bundle

# Full Navi response — already canonical shape.
_NAVI_OK = {
    "name": "Where you are",
    "city": "Boise",
    "county": "Ada",
    "state": "Idaho",
    "country": "United States",
    "postal_code": "83702",
    "timezone": "America/Boise",
    "landclass": "Public — National Forest",
    "elevation_m": 824,
}


def _backend() -> NaviBackend:
    # warmup=False so construction issues no background task in tests.
    return NaviBackend(base_url="http://navi.test:8440", warmup=False)


def test_url_construction():
    b = _backend()
    assert b._url(43.615, -116.2023) == "http://navi.test:8440/api/reverse/43.615/-116.2023"


def test_base_url_trailing_slash_stripped():
    b = NaviBackend(base_url="http://navi.test:8440/", warmup=False)
    assert b._url(1.0, 2.0) == "http://navi.test:8440/api/reverse/1.0/2.0"


@pytest.mark.asyncio
async def test_happy_path_passthrough():
    b = _backend()
    b._fetch = AsyncMock(return_value=dict(_NAVI_OK))
    result = await b.reverse(43.615, -116.2023)
    assert result == _NAVI_OK
    assert set(result.keys()) == set(GEOCODER_FIELDS)


@pytest.mark.asyncio
async def test_partial_nulls_preserved():
    """Navi 200-with-nulls (non-US: timezone + elevation, rest null)."""
    partial = {**all_null_bundle(), "timezone": "Europe/Paris", "elevation_m": 35}
    b = _backend()
    b._fetch = AsyncMock(return_value=partial)
    result = await b.reverse(48.85, 2.35)
    assert result["timezone"] == "Europe/Paris"
    assert result["elevation_m"] == 35
    assert result["city"] is None
    assert set(result.keys()) == set(GEOCODER_FIELDS)


@pytest.mark.asyncio
async def test_extra_keys_dropped():
    b = _backend()
    b._fetch = AsyncMock(return_value={**_NAVI_OK, "debug_internal": "leak"})
    result = await b.reverse(1.0, 2.0)
    assert "debug_internal" not in result
    assert set(result.keys()) == set(GEOCODER_FIELDS)


@pytest.mark.asyncio
async def test_network_error_returns_all_null_never_raises():
    b = _backend()
    b._fetch = AsyncMock(side_effect=ConnectionError("boom"))
    result = await b.reverse(1.0, 2.0)
    assert result == all_null_bundle()


@pytest.mark.asyncio
async def test_timeout_returns_all_null():
    import asyncio

    b = _backend()
    b._fetch = AsyncMock(side_effect=asyncio.TimeoutError())
    assert await b.reverse(1.0, 2.0) == all_null_bundle()


@pytest.mark.asyncio
async def test_malformed_response_returns_all_null():
    b = _backend()
    b._fetch = AsyncMock(side_effect=ValueError("not json"))
    assert await b.reverse(1.0, 2.0) == all_null_bundle()


@pytest.mark.asyncio
async def test_headers_passed_through_config():
    b = NaviBackend(base_url="http://navi.test", headers={"Authorization": "Bearer x"}, warmup=False)
    assert b._headers == {"Authorization": "Bearer x"}


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("NAVI_INTEGRATION_TEST") != "1",
    reason="set NAVI_INTEGRATION_TEST=1 to hit the live Navi endpoint",
)
async def test_live_navi_boise():
    """Integration smoke against the real endpoint (default skipped)."""
    b = NaviBackend(warmup=False)  # default base_url
    result = await b.reverse(43.6150, -116.2023)
    assert result["name"] == "Where you are"
    assert result["city"] == "Boise"
    assert result["state"] == "Idaho"
    assert result["elevation_m"] is not None
    assert abs(float(result["elevation_m"]) - 824) < 50
