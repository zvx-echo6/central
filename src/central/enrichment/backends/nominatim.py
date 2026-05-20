"""OSM Nominatim reverse-geocoding backend.

Works against public OSM Nominatim (1 req/sec + User-Agent required) or a
self-hosted instance (no limit). Resolves name + address only; timezone,
landclass, and elevation_m are nulled (not in the Nominatim reverse response).

Nominatim jsonv2 reverse response shape:
    {"display_name": "...", "name": "...",
     "address": {city|town|village, county, state, country, postcode, ...}}
"""

import asyncio
import logging
import time
from typing import Any
from urllib.parse import urlencode

import aiohttp

from central.enrichment.geocoder import all_null_bundle

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://nominatim.openstreetmap.org"
DEFAULT_USER_AGENT = "central-enrichment/0.5 (https://github.com/zvx-echo6/central)"


class NominatimBackend:
    """GeocoderBackend backed by an OSM Nominatim /reverse endpoint.

    rate_limit_per_sec throttles outbound requests (public OSM requires <= 1/s);
    set it to 0 to disable for self-hosted instances.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        rate_limit_per_sec: float = 1.0,
        timeout_s: float = 10.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._user_agent = user_agent
        self._min_interval = (1.0 / rate_limit_per_sec) if rate_limit_per_sec > 0 else 0.0
        self._timeout_s = timeout_s
        self._rl_lock = asyncio.Lock()
        self._last_request_at = 0.0

    def _url(self, lat: float, lon: float) -> str:
        qs = urlencode({"lat": lat, "lon": lon, "format": "jsonv2"})
        return f"{self._base_url}/reverse?{qs}"

    def _request_headers(self) -> dict[str, str]:
        # Public Nominatim rejects requests without an identifying User-Agent.
        return {"User-Agent": self._user_agent}

    async def _throttle(self) -> None:
        if self._min_interval <= 0:
            return
        async with self._rl_lock:
            now = time.monotonic()
            wait = self._last_request_at + self._min_interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = time.monotonic()

    async def _fetch(self, lat: float, lon: float) -> dict[str, Any]:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._timeout_s),
        ) as session:
            async with session.get(
                self._url(lat, lon), headers=self._request_headers()
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
        try:
            await self._throttle()
            data = await self._fetch(lat, lon)
            addr = data.get("address", {}) or {}
        except Exception:
            logger.debug("NominatimBackend reverse failed; returning all-null bundle")
            return all_null_bundle()
        return {
            "name": data.get("name") or data.get("display_name"),
            "city": addr.get("city") or addr.get("town") or addr.get("village"),
            "county": addr.get("county"),
            "state": addr.get("state"),
            "country": addr.get("country"),
            "postal_code": addr.get("postcode"),
            "timezone": None,   # not in Nominatim reverse response
            "landclass": None,  # Navi-composed-endpoint only
            "elevation_m": None,  # Navi-composed-endpoint only
        }
