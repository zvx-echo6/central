"""Regression tests for apply_enrichment's coordless path.

Design principle: every event from an adapter that declares enrichment_locations
must carry data["_enriched"] — populated when coordinates resolve, an all-null
bundle when they don't (e.g. removal tombstones with no lat/lon). Adapters that
declare no enrichment_locations are still skipped entirely.
"""

from datetime import datetime, timezone
from typing import Any

import pytest

from central.config_models import EnrichmentConfig
from central.enrichment.cache import EnrichmentCache
from central.enrichment.geocoder import GeocoderEnricher, all_null_bundle
from central.models import Event, Geo
from central.supervisor import apply_enrichment, build_enrichers


def _make_event(data: dict[str, Any]) -> Event:
    return Event(
        id="evt-1",
        adapter="usgs_quake",
        category="quake.event.test",
        time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        geo=Geo(),
        data=data,
    )


class _PopulatingBackend:
    """Deterministic backend that resolves any real coords to a fixed place."""

    async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
        return {**all_null_bundle(), "city": "Boise", "state": "ID"}


@pytest.mark.asyncio
async def test_coordless_event_with_declared_locations_gets_null_bundle(tmp_path):
    """An event whose declared coord paths are all None still gets _enriched."""
    cache = EnrichmentCache(tmp_path / "enrichment_cache.db")
    enrichers = build_enrichers(EnrichmentConfig(), cache)
    event = _make_event(
        {"latitude": None, "longitude": None, "reason": "fallen_off_current_service"}
    )
    assert "_enriched" not in event.data

    await apply_enrichment(event, [("latitude", "longitude")], enrichers)

    assert event.data["_enriched"]["geocoder"] == all_null_bundle()


@pytest.mark.asyncio
async def test_event_with_coords_still_enriches_normally(tmp_path):
    """The coord-bearing path is unchanged: the backend is consulted and its
    resolved fields land in the bundle."""
    cache = EnrichmentCache(tmp_path / "enrichment_cache.db")
    enricher = GeocoderEnricher(_PopulatingBackend(), cache=cache)
    event = _make_event({"latitude": 43.0, "longitude": -116.0})

    await apply_enrichment(event, [("latitude", "longitude")], [enricher])

    bundle = event.data["_enriched"]["geocoder"]
    assert bundle["state"] == "ID"
    assert bundle["city"] == "Boise"


@pytest.mark.asyncio
async def test_adapter_with_no_enrichment_locations_still_skipped(tmp_path):
    """Adapters declaring no enrichment_locations are skipped — no _enriched."""
    cache = EnrichmentCache(tmp_path / "enrichment_cache.db")
    enrichers = build_enrichers(EnrichmentConfig(), cache)
    event = _make_event({"latitude": 43.0, "longitude": -116.0})

    await apply_enrichment(event, [], enrichers)

    assert "_enriched" not in event.data
