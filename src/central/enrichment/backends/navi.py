"""Navi reverse-geocoding backend.

Hits the composed Navi endpoint `<base_url>/api/reverse/<lat>/<lon>`, which
already returns the canonical 9-field bundle (name, city, county, state,
country, postal_code, timezone, landclass, elevation_m). Navi composes Photon
(name/address) + tz_world (timezone) + PAD-US (landclass) + planet-DEM
(elevation_m), so this backend is a near-passthrough mapping.

Coverage today: US events get a rich bundle; non-US events get timezone +
elevation_m populated (both planet-scale) and the rest null until Navi's
Photon planet expansion lands (no Central change needed when it does).
"""

import asyncio
import logging
from typing import Any

import aiohttp

from central.enrichment.geocoder import GEOCODER_FIELDS, all_null_bundle

logger = logging.getLogger(__name__)

# Generic default — operators point this at their Navi instance via the
# /enrichment config page (backend_settings.base_url). No deployment-specific
# host belongs in source.
DEFAULT_BASE_URL = "http://localhost:8440"
# Boise — warmup coordinate, amortizes Photon/DEM cold-connection cost at startup.
_WARMUP_LAT = 43.6150
_WARMUP_LON = -116.2023


class NaviBackend:
    """GeocoderBackend backed by the composed Navi /api/reverse endpoint."""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout_s: float = 10.0,
        headers: dict[str, str] | None = None,
        warmup: bool = True,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        # Future-proof: drop an Authorization: Bearer … here config-only, no code change.
        self._headers = dict(headers or {})
        if warmup:
            # Fire-and-forget warmup ping; only if a loop is running (it is under
            # the supervisor's asyncio.run, not under sync test construction).
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._warmup())
            except RuntimeError:
                pass

    def _url(self, lat: float, lon: float) -> str:
        return f"{self._base_url}/api/reverse/{lat}/{lon}"

    async def _warmup(self) -> None:
        try:
            await self._fetch(_WARMUP_LAT, _WARMUP_LON)
        except Exception:
            # Warmup is best-effort; a failure here must not break startup.
            logger.debug("NaviBackend warmup ping failed (non-fatal)")

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
        except Exception:
            # Non-200, network error, timeout, malformed JSON — never raise.
            logger.debug("NaviBackend reverse failed; returning all-null bundle")
            return all_null_bundle()
        # Navi's response already matches the canonical shape; map defensively.
        return {field: data.get(field) for field in GEOCODER_FIELDS}
