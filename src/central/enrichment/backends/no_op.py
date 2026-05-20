"""No-op geocoder backend — returns an all-null bundle for every input.

The default backend in PR J. Real backends (Navi, Photon, Nominatim) land in
PR K; until then the framework is exercisable end-to-end with NoOpBackend,
which satisfies the GeocoderBackend contract while resolving nothing.
"""

from typing import Any

from central.enrichment.geocoder import all_null_bundle


class NoOpBackend:
    """GeocoderBackend that resolves no fields."""

    async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
        return all_null_bundle()
