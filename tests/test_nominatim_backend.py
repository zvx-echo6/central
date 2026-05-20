"""Tests for NominatimBackend (OSM Nominatim /reverse jsonv2)."""

from unittest.mock import AsyncMock, patch

import pytest

from central.enrichment.backends.nominatim import (
    DEFAULT_USER_AGENT,
    NominatimBackend,
)
from central.enrichment.geocoder import GEOCODER_FIELDS, all_null_bundle

_NOMINATIM_OK = {
    "name": "Boise",
    "display_name": "Boise, Ada County, Idaho, United States",
    "address": {
        "city": "Boise",
        "county": "Ada County",
        "state": "Idaho",
        "country": "United States",
        "postcode": "83702",
    },
}


def _backend(**kw) -> NominatimBackend:
    kw.setdefault("base_url", "http://nominatim.test")
    kw.setdefault("rate_limit_per_sec", 0)  # disabled by default in tests
    return NominatimBackend(**kw)


def test_url_and_format():
    b = _backend()
    url = b._url(43.6, -116.2)
    assert url.startswith("http://nominatim.test/reverse?")
    assert "format=jsonv2" in url and "lat=43.6" in url and "lon=-116.2" in url


def test_user_agent_header_present():
    b = _backend()
    assert b._request_headers()["User-Agent"] == DEFAULT_USER_AGENT


def test_custom_user_agent():
    b = _backend(user_agent="myapp/1.0 (me@example.com)")
    assert b._request_headers()["User-Agent"] == "myapp/1.0 (me@example.com)"


@pytest.mark.asyncio
async def test_happy_path_maps_address_block():
    b = _backend()
    b._fetch = AsyncMock(return_value=dict(_NOMINATIM_OK))
    result = await b.reverse(43.6, -116.2)
    assert set(result.keys()) == set(GEOCODER_FIELDS)
    assert result["name"] == "Boise"
    assert result["city"] == "Boise"
    assert result["county"] == "Ada County"
    assert result["state"] == "Idaho"
    assert result["country"] == "United States"
    assert result["postal_code"] == "83702"


@pytest.mark.asyncio
async def test_navi_only_fields_null():
    b = _backend()
    b._fetch = AsyncMock(return_value=dict(_NOMINATIM_OK))
    result = await b.reverse(43.6, -116.2)
    assert result["timezone"] is None
    assert result["landclass"] is None
    assert result["elevation_m"] is None


@pytest.mark.asyncio
async def test_city_falls_back_to_town_then_village():
    b = _backend()
    b._fetch = AsyncMock(return_value={"address": {"village": "Tinytown"}})
    result = await b.reverse(1.0, 2.0)
    assert result["city"] == "Tinytown"


@pytest.mark.asyncio
async def test_network_error_returns_all_null():
    b = _backend()
    b._fetch = AsyncMock(side_effect=ConnectionError("down"))
    assert await b.reverse(1.0, 2.0) == all_null_bundle()


@pytest.mark.asyncio
async def test_malformed_json_returns_all_null():
    b = _backend()
    b._fetch = AsyncMock(side_effect=ValueError("bad"))
    assert await b.reverse(1.0, 2.0) == all_null_bundle()


@pytest.mark.asyncio
async def test_rate_limit_disabled_does_not_sleep():
    b = _backend(rate_limit_per_sec=0)
    with patch("central.enrichment.backends.nominatim.asyncio.sleep") as slp:
        await b._throttle()
        await b._throttle()
        slp.assert_not_called()


@pytest.mark.asyncio
async def test_rate_limit_spaces_consecutive_requests():
    """With a 1/sec limit, the second back-to-back call must sleep a positive
    interval. Mock monotonic to a fixed instant so the throttle computes a wait."""
    b = _backend(rate_limit_per_sec=1.0)
    sleeps: list[float] = []

    async def _fake_sleep(d):
        sleeps.append(d)

    with patch("central.enrichment.backends.nominatim.time.monotonic", return_value=100.0), \
         patch("central.enrichment.backends.nominatim.asyncio.sleep", side_effect=_fake_sleep):
        await b._throttle()  # first: last_request_at was 0 -> no wait
        await b._throttle()  # second: now==100, last==100 -> wait ~1.0s
    assert any(d > 0 for d in sleeps), f"expected a positive throttle sleep, got {sleeps}"
