"""Tests for GeocoderEnricher with the default NoOpBackend."""

from typing import Any

import pytest

from central.enrichment.backends.no_op import NoOpBackend
from central.enrichment.cache import EnrichmentCache
from central.enrichment.geocoder import (
    GEOCODER_FIELDS,
    GeocoderEnricher,
    all_null_bundle,
)


@pytest.mark.asyncio
async def test_noop_backend_returns_all_null_bundle():
    enricher = GeocoderEnricher(NoOpBackend())
    result = await enricher.enrich({"lat": 45.0, "lon": -116.0})
    assert result == all_null_bundle()
    assert all(v is None for v in result.values())


@pytest.mark.asyncio
async def test_field_set_matches_locked_protocol():
    """Every field in the locked GEOCODER_FIELDS set is present (all None for
    NoOpBackend), and no extra keys leak through — bidirectional equality."""
    enricher = GeocoderEnricher(NoOpBackend())
    result = await enricher.enrich({"lat": 1.0, "lon": 2.0})
    assert set(result.keys()) == set(GEOCODER_FIELDS)


@pytest.mark.asyncio
async def test_missing_coords_returns_all_null_without_backend_call():
    class _Tripwire:
        async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
            raise AssertionError("backend must not be called for null coords")

    enricher = GeocoderEnricher(_Tripwire())
    assert await enricher.enrich({"lat": None, "lon": None}) == all_null_bundle()  # type: ignore[dict-item]
    assert await enricher.enrich({}) == all_null_bundle()


@pytest.mark.asyncio
async def test_enricher_name_is_geocoder():
    """The name keys the result under event.data['_enriched'][name]."""
    assert GeocoderEnricher(NoOpBackend()).name == "geocoder"


@pytest.mark.asyncio
async def test_sequential_calls_same_coords_hit_cache(tmp_path):
    class _CountingNoOp:
        def __init__(self) -> None:
            self.calls = 0

        async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
            self.calls += 1
            return all_null_bundle()

    cache = EnrichmentCache(tmp_path / "c.db", ttl_s=3600)
    backend = _CountingNoOp()
    enricher = GeocoderEnricher(backend, cache=cache)
    for _ in range(5):
        await enricher.enrich({"lat": 33.5, "lon": -111.9})
    assert backend.calls == 1, "repeated identical coords must collapse to one backend call"
