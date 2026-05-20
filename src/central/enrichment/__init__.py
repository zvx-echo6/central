"""Central enrichment framework.

The supervisor runs registered enrichers over each event whose adapter
declares `enrichment_locations`, attaching results under
`event.data["_enriched"][<enricher.name>]`. Provenance is explicit: anything
under `_enriched` is Central-derived; everything else in `data` is upstream
verbatim.
"""

from central.enrichment.backends.no_op import NoOpBackend
from central.enrichment.base import Enricher
from central.enrichment.cache import EnrichmentCache
from central.enrichment.geocoder import (
    GEOCODER_FIELDS,
    GeocoderBackend,
    GeocoderEnricher,
    all_null_bundle,
)

__all__ = [
    "Enricher",
    "EnrichmentCache",
    "GeocoderEnricher",
    "GeocoderBackend",
    "GEOCODER_FIELDS",
    "all_null_bundle",
    "NoOpBackend",
]
