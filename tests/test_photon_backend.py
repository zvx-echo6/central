"""Tests for PhotonBackend (raw Photon /reverse)."""

from unittest.mock import AsyncMock

import pytest

from central.enrichment.backends.photon import PhotonBackend
from central.enrichment.geocoder import GEOCODER_FIELDS, all_null_bundle

# Photon reverse response shape.
_PHOTON_OK = {
    "features": [
        {
            "properties": {
                "name": "Boise",
                "city": "Boise",
                "county": "Ada County",
                "state": "Idaho",
                "country": "United States",
                "postcode": "83702",
                "osm_key": "place",
            },
            "geometry": {"type": "Point", "coordinates": [-116.2, 43.6]},
        }
    ]
}


def _backend() -> PhotonBackend:
    return PhotonBackend(base_url="http://photon.test:2322")


def test_url_construction():
    b = _backend()
    url = b._url(43.6, -116.2)
    assert url.startswith("http://photon.test:2322/reverse?")
    assert "lat=43.6" in url and "lon=-116.2" in url and "limit=1" in url


@pytest.mark.asyncio
async def test_happy_path_maps_to_canonical():
    b = _backend()
    b._fetch = AsyncMock(return_value=dict(_PHOTON_OK))
    result = await b.reverse(43.6, -116.2)
    assert set(result.keys()) == set(GEOCODER_FIELDS)
    assert result["city"] == "Boise"
    assert result["county"] == "Ada County"
    assert result["state"] == "Idaho"
    assert result["country"] == "United States"
    assert result["postal_code"] == "83702"  # mapped from Photon 'postcode'


@pytest.mark.asyncio
async def test_navi_only_fields_are_null():
    b = _backend()
    b._fetch = AsyncMock(return_value=dict(_PHOTON_OK))
    result = await b.reverse(43.6, -116.2)
    assert result["timezone"] is None
    assert result["landclass"] is None
    assert result["elevation_m"] is None


@pytest.mark.asyncio
async def test_empty_features_returns_all_null():
    b = _backend()
    b._fetch = AsyncMock(return_value={"features": []})
    assert await b.reverse(0.0, 0.0) == all_null_bundle()


@pytest.mark.asyncio
async def test_missing_properties_keys_become_null():
    b = _backend()
    b._fetch = AsyncMock(return_value={"features": [{"properties": {"name": "X"}}]})
    result = await b.reverse(1.0, 2.0)
    assert result["name"] == "X"
    assert result["city"] is None
    assert result["postal_code"] is None
    assert set(result.keys()) == set(GEOCODER_FIELDS)


@pytest.mark.asyncio
async def test_network_error_returns_all_null():
    b = _backend()
    b._fetch = AsyncMock(side_effect=ConnectionError("down"))
    assert await b.reverse(1.0, 2.0) == all_null_bundle()


@pytest.mark.asyncio
async def test_malformed_json_returns_all_null():
    b = _backend()
    b._fetch = AsyncMock(side_effect=ValueError("bad json"))
    assert await b.reverse(1.0, 2.0) == all_null_bundle()
