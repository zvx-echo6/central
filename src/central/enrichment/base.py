"""Enricher protocol — the framework-level contract.

An Enricher takes a single location and returns a flat dict of enrichment
fields. The supervisor attaches each enricher's result under
`event.data["_enriched"][enricher.name]` before publishing. Everything under
`_enriched` is Central-provenance; everything else in `data` is upstream
verbatim.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Enricher(Protocol):
    """Pluggable enrichment unit.

    name: short identifier, used as the key under event.data["_enriched"].
    """

    name: str

    async def enrich(self, location: dict[str, float]) -> dict[str, Any]:
        """Given a location ({"lat": float, "lon": float}), return enrichment fields.

        Fields the enricher can't resolve are present with value None (NOT
        omitted) so consumers see a stable field set. Implementations must
        NEVER raise — they handle their own failures and return an all-null
        bundle on total failure.
        """
        ...
