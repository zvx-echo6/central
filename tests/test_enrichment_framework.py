"""Tests for the enrichment cache + framework wiring.

Covers cache hit/miss/TTL/rounding, idempotent concurrent writes, and the
"backend failure -> all-null, not cached" contract via GeocoderEnricher.
"""

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from central.enrichment.cache import EnrichmentCache, round_coord
from central.enrichment.geocoder import GEOCODER_FIELDS, GeocoderEnricher, all_null_bundle


@pytest.fixture
def cache_path(tmp_path: Path) -> Path:
    return tmp_path / "nested" / "enrichment_cache.db"


def test_init_creates_parent_dir_and_table(cache_path: Path):
    assert not cache_path.parent.exists()
    cache = EnrichmentCache(cache_path, ttl_s=60)
    assert cache_path.parent.is_dir()
    # Table exists and is queryable.
    conn = sqlite3.connect(cache_path)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='enrichment_cache'"
        )
        assert cur.fetchone() is not None
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_cache_miss_then_hit(cache_path: Path):
    cache = EnrichmentCache(cache_path, ttl_s=3600)
    assert await cache.get("geocoder", 45.0, -116.0) is None  # miss
    payload = {"name": "Somewhere", "state": "ID"}
    await cache.set("geocoder", 45.0, -116.0, payload)
    hit = await cache.get("geocoder", 45.0, -116.0)
    assert hit == payload


@pytest.mark.asyncio
async def test_ttl_expiry_returns_miss(cache_path: Path):
    cache = EnrichmentCache(cache_path, ttl_s=0)  # everything immediately stale
    await cache.set("geocoder", 1.0, 2.0, {"name": "x"})
    # ttl_s=0 -> age (>0) always exceeds ttl -> treated as expired.
    assert await cache.get("geocoder", 1.0, 2.0) is None


def test_round_coord_4dp():
    assert round_coord(45.123456789) == 45.1235
    assert round_coord(-116.000049) == -116.0
    assert round_coord(12.99995) == 13.0


@pytest.mark.asyncio
async def test_rounding_collapses_nearby_coords_to_same_key(cache_path: Path):
    cache = EnrichmentCache(cache_path, ttl_s=3600)
    await cache.set("geocoder", 45.12341, -116.45678, {"name": "rounded"})
    # 45.123413 / -116.456784 round to the same 4dp key -> same row.
    hit = await cache.get("geocoder", 45.123413, -116.456784)
    assert hit == {"name": "rounded"}


@pytest.mark.asyncio
async def test_concurrent_sets_do_not_double_write(cache_path: Path):
    cache = EnrichmentCache(cache_path, ttl_s=3600)
    await asyncio.gather(
        *[cache.set("geocoder", 10.0, 20.0, {"n": i}) for i in range(20)]
    )
    conn = sqlite3.connect(cache_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM enrichment_cache WHERE enricher_name='geocoder' "
            "AND lat_rounded=? AND lon_rounded=?",
            (10.0, 20.0),
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 1, "PRIMARY KEY must collapse concurrent writes to one row"


class _CountingBackend:
    """Backend that counts reverse() calls; lets tests prove cache hits."""

    def __init__(self) -> None:
        self.calls = 0

    async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
        self.calls += 1
        return {**all_null_bundle(), "name": "Counted", "state": "ID"}


class _ExplodingBackend:
    """Backend that violates the never-raise contract."""

    def __init__(self) -> None:
        self.calls = 0

    async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
        self.calls += 1
        raise RuntimeError("upstream geocoder down")


@pytest.mark.asyncio
async def test_backend_failure_returns_all_null_and_does_not_cache(cache_path: Path):
    cache = EnrichmentCache(cache_path, ttl_s=3600)
    backend = _ExplodingBackend()
    enricher = GeocoderEnricher(backend, cache=cache)

    result = await enricher.enrich({"lat": 5.0, "lon": 6.0})
    assert result == all_null_bundle()

    # Nothing cached -> a second call retries the backend (calls increments).
    assert await cache.get("geocoder", 5.0, 6.0) is None
    await enricher.enrich({"lat": 5.0, "lon": 6.0})
    assert backend.calls == 2, "failed lookups must not be cached (must retry)"


@pytest.mark.asyncio
async def test_successful_result_is_cached_and_avoids_second_backend_call(cache_path: Path):
    cache = EnrichmentCache(cache_path, ttl_s=3600)
    backend = _CountingBackend()
    enricher = GeocoderEnricher(backend, cache=cache)

    first = await enricher.enrich({"lat": 7.5, "lon": 8.5})
    second = await enricher.enrich({"lat": 7.5, "lon": 8.5})
    assert first == second
    assert backend.calls == 1, "second call with same coords must hit cache"


@pytest.mark.asyncio
async def test_all_null_result_is_cached(cache_path: Path):
    """A backend that resolves nothing still gets cached — the contract says
    cache even all-null so we don't re-hammer the backend for known-empty
    coordinates."""

    class _NullCounting:
        def __init__(self) -> None:
            self.calls = 0

        async def reverse(self, lat: float, lon: float) -> dict[str, Any]:
            self.calls += 1
            return all_null_bundle()

    cache = EnrichmentCache(cache_path, ttl_s=3600)
    backend = _NullCounting()
    enricher = GeocoderEnricher(backend, cache=cache)
    await enricher.enrich({"lat": 1.0, "lon": 1.0})
    await enricher.enrich({"lat": 1.0, "lon": 1.0})
    assert backend.calls == 1
    cached = await cache.get("geocoder", 1.0, 1.0)
    assert cached == all_null_bundle()
