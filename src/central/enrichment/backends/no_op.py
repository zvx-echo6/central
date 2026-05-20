"""No-op geocoder backend — returns an all-null bundle for every input.

The default backend in PR J. Real backends (Navi, Photon, Nominatim) land in
PR K; until then the framework is exercisable end-to-end with NoOpBackend,
which satisfies the GeocoderBackend contract while resolving nothing.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict

from central.enrichment.geocoder import all_null_bundle


class NoOpBackendSettings(BaseModel):
    """No-op backend takes no settings. extra='forbid' makes switching to
    NoOpBackend while stale backend_settings (e.g. a base_url) remain a clean
    ValidationError instead of a TypeError at construction."""

    model_config = ConfigDict(extra="forbid")


class NoOpBackend:
    """GeocoderBackend that resolves no fields."""

    settings_schema = NoOpBackendSettings

    async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
        return all_null_bundle()
