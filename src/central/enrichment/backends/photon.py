"""Raw Photon reverse-geocoding backend.

For deployers who run a Photon instance directly, without the composed
Navi-style endpoint. Photon resolves name + address only — timezone,
landclass, and elevation_m are Navi-composed-endpoint extras and are nulled
here.

Photon reverse response shape:
    {"features": [{"properties": {name, city, county, state, country,
                                  postcode, ...}, "geometry": {...}}]}
"""

import logging
from typing import Any
from urllib.parse import urlencode

import aiohttp
from pydantic import BaseModel, ConfigDict, Field

from central.enrichment.geocoder import all_null_bundle

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "http://localhost:2322"


class PhotonBackendSettings(BaseModel):
    """Settings for PhotonBackend. Mirrors __init__ defaults exactly."""

    model_config = ConfigDict(extra="forbid")

    base_url: str = Field(default=DEFAULT_BASE_URL, description="Photon /reverse base URL")
    timeout_s: float = Field(default=10.0, description="Per-request timeout in seconds")
    headers: dict[str, str] | None = Field(default=None, description="Extra request headers")


class PhotonBackend:
    """GeocoderBackend backed by a raw Photon /reverse endpoint."""

    settings_schema = PhotonBackendSettings

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout_s: float = 10.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._headers = dict(headers or {})

    def _url(self, lat: float, lon: float) -> str:
        qs = urlencode({"lat": lat, "lon": lon, "limit": 1})
        return f"{self._base_url}/reverse?{qs}"

    async def _fetch(self, lat: float, lon: float) -> dict[str, Any]:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._timeout_s),
        ) as session:
            async with session.get(self._url(lat, lon), headers=self._headers) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
        try:
            data = await self._fetch(lat, lon)
            features = data.get("features") or []
            props = features[0].get("properties", {}) if features else {}
        except Exception:
            logger.debug("PhotonBackend reverse failed; returning all-null bundle")
            return all_null_bundle()
        return {
            "name": props.get("name"),
            "city": props.get("city"),
            "county": props.get("county"),
            "state": props.get("state"),
            "country": props.get("country"),
            "postal_code": props.get("postcode"),  # Photon names it 'postcode'
            "timezone": None,   # not provided by raw Photon
            "landclass": None,  # Navi-composed-endpoint only
            "elevation_m": None,  # Navi-composed-endpoint only
        }
