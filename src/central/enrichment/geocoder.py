"""Reverse-geocoding enricher + the pluggable backend contract.

GeocoderEnricher is the only enricher in PR J. It owns the cache + the
all-null normalization; the actual lookup is delegated to a GeocoderBackend.
PR J ships NoOpBackend only (all-null); real backends (Navi/Photon/Nominatim)
land in PR K.
"""

import logging
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel

from central.enrichment.cache import EnrichmentCache

logger = logging.getLogger(__name__)

# Locked canonical geocoder field set. The single source of truth for what a
# geocoder enrichment bundle looks like — backends fill what they can and
# return None for the rest; NoOpBackend returns all None.
GEOCODER_FIELDS: tuple[str, ...] = (
    "name",
    "city",
    "county",
    "state",
    "country",
    "postal_code",
    "timezone",
    "landclass",
    "elevation_m",
)


def all_null_bundle() -> dict[str, Any]:
    """A geocoder bundle with every locked field present and None."""
    return {field: None for field in GEOCODER_FIELDS}


@runtime_checkable
class GeocoderBackend(Protocol):
    """The pluggable reverse-geocoding layer beneath GeocoderEnricher."""

    # Pydantic model (extra='forbid') describing this backend's accepted
    # settings. The supervisor validates config.enrichment.backend_settings
    # against it before instantiating, turning a config/settings mismatch into
    # a clean ValidationError instead of a constructor TypeError.
    settings_schema: type[BaseModel]

    async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
        """Return canonical geocoder fields (see GEOCODER_FIELDS).

        Fields the backend can't resolve return None. Must never raise.
        """
        ...


class GeocoderEnricher:
    """Reverse-geocode a location into the canonical geocoder field set.

    Resolution: cache hit -> return cached; cache miss -> call backend, cache
    the (normalized) result even when all-null, return it. Backend failure
    (any exception escaping the backend's "never raise" contract) -> return
    all-null and DO NOT cache, so the next call retries.
    """

    name = "geocoder"

    def __init__(
        self,
        backend: GeocoderBackend,
        cache: EnrichmentCache | None = None,
    ) -> None:
        self._backend = backend
        self._cache = cache

    async def enrich(self, location: dict[str, float]) -> dict[str, Any]:
        lat = location.get("lat")
        lon = location.get("lon")
        if lat is None or lon is None:
            return all_null_bundle()

        if self._cache is not None:
            cached = await self._cache.get(self.name, lat, lon)
            if cached is not None:
                return cached

        try:
            raw = await self._backend.reverse(lat, lon)
        except Exception:
            # Backend broke its "never raise" contract. Return all-null and do
            # NOT cache, so a transient failure doesn't get pinned for the TTL.
            logger.exception("geocoder backend raised; returning all-null bundle")
            return all_null_bundle()

        # Normalize to the locked field set: every field present, extras dropped.
        normalized = {field: raw.get(field) for field in GEOCODER_FIELDS}

        if self._cache is not None:
            await self._cache.set(self.name, lat, lon, normalized)
        return normalized
