"""Coverage tests for adapter enrichment_locations declarations (PR L-a).

Every adapter must make a conscious enrichment_locations declaration — a
non-empty [(lat_field, lon_field)] for point adapters, or an explicit [] for
adapters with no point coordinate. Registry-derived (iterates
discover_adapters()), no hardcoded adapter lists.

Plus synthetic-event tests for the two adapters with isolated event builders
(usgs_quake._feature_to_event, nwis._build_event) proving the declared
latitude/longitude paths actually resolve on event.data. The four inline-build
point adapters (eonet, gdacs, wfigs_incidents, inciweb) construct events only
inside poll() and are covered by the live end-to-end smoke instead.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from central.adapter_discovery import discover_adapters
from central.config_models import AdapterConfig


def test_every_adapter_explicitly_declares_enrichment_locations():
    """Each adapter declares enrichment_locations in its OWN class body (not
    just inheriting the SourceAdapter default) — a conscious choice per adapter."""
    missing = [
        name for name, cls in discover_adapters().items()
        if "enrichment_locations" not in cls.__dict__
    ]
    assert not missing, f"adapters missing an explicit enrichment_locations: {missing}"


def test_enrichment_locations_shape_is_valid():
    """Each declaration is a list of (str, str) tuples (possibly empty)."""
    for name, cls in discover_adapters().items():
        locs = cls.enrichment_locations
        assert isinstance(locs, list), f"{name}: not a list"
        for tup in locs:
            assert isinstance(tup, tuple) and len(tup) == 2, f"{name}: bad tuple {tup!r}"
            assert all(isinstance(p, str) for p in tup), f"{name}: non-str path {tup!r}"


def test_point_adapters_use_canonical_lat_lon_paths():
    """Every point adapter (non-empty declaration) uses the same protocol keys
    ('latitude', 'longitude') — the convention FIRMS established."""
    for name, cls in discover_adapters().items():
        if cls.enrichment_locations:
            assert cls.enrichment_locations == [("latitude", "longitude")], (
                f"{name} uses non-canonical paths: {cls.enrichment_locations}"
            )


def test_at_least_the_known_point_adapters_are_non_empty():
    """Registry-derived sanity: the adapters that carry a point coordinate have
    a non-empty declaration. Derived by probing enrichment_locations, not a
    hardcoded list — guards against a regression that blanks them all."""
    non_empty = {
        name for name, cls in discover_adapters().items() if cls.enrichment_locations
    }
    # firms is the original; there must be several point adapters now.
    assert "firms" in non_empty
    assert len(non_empty) >= 5, f"expected several point adapters, got {sorted(non_empty)}"


# --- synthetic-event tests for the two isolated builders --------------------

def test_usgs_quake_event_exposes_top_level_latlon():
    from central.adapters.usgs_quake import USGSQuakeAdapter

    config = AdapterConfig(
        name="usgs_quake", enabled=True, cadence_s=60,
        settings={}, updated_at=datetime.now(timezone.utc),
    )
    adapter = USGSQuakeAdapter(config, MagicMock(), Path("/tmp/never_used.db"))
    feature = {
        "type": "Feature", "id": "test_q1",
        "properties": {"mag": 2.5, "place": "X", "time": 1715000000000,
                       "updated": 1715000000000},
        "geometry": {"type": "Point", "coordinates": [-116.2, 43.7, 10.5]},
    }
    event = adapter._feature_to_event(feature)
    assert event is not None
    assert event.data["latitude"] == 43.7
    assert event.data["longitude"] == -116.2


def test_nwis_event_mirrors_centroid_into_data():
    from central.adapters.nwis import NWISAdapter

    config = AdapterConfig(
        name="nwis", enabled=True, cadence_s=900,
        settings={}, updated_at=datetime.now(timezone.utc),
    )
    adapter = NWISAdapter(config, MagicMock(), Path("/tmp/never_used.db"))
    feature = {
        "geometry": {"type": "Point", "coordinates": [-90.25, 41.78]},  # (lon, lat)
        "properties": {
            "monitoring_location_id": "USGS-05420500",
            "time": "2026-05-20T00:00:00Z",
            "value": "123.0",
            "unit_of_measure": "ft",
        },
    }
    event = adapter._build_event(feature, "00060")
    assert event is not None
    # latitude = centroid[1], longitude = centroid[0]; no axis swap.
    assert event.data["latitude"] == 41.78
    assert event.data["longitude"] == -90.25
    # Geo.centroid retained for existing rendering uses.
    assert event.geo.centroid == (-90.25, 41.78)


# --- bump_last_seen contract (v0.7.0 Bug 1) ---------------------------------

def test_every_adapter_resolves_bump_last_seen():
    """The supervisor calls adapter.bump_last_seen on every dedup hit. Every
    registered adapter must resolve a callable (inherited from SourceAdapter or
    overridden) -- otherwise the AttributeError leaks to /dashboard (the eonet
    bug). Registry-derived, no hardcoded list."""
    missing = [
        name for name, cls in discover_adapters().items()
        if not callable(getattr(cls, "bump_last_seen", None))
    ]
    assert not missing, f"adapters missing bump_last_seen: {missing}"


def test_base_bump_last_seen_updates_and_is_noop_without_db():
    """SourceAdapter.bump_last_seen updates published_ids.last_seen when a _db
    handle is present, and is a safe no-op when it is not."""
    import sqlite3

    from central.adapter import SourceAdapter

    class _StubAdapter(SourceAdapter):
        name = "stub"

        async def poll(self): ...           # never called in this test
        async def apply_config(self, new_config): ...
        def subject_for(self, event): return ""

    adapter = _StubAdapter()
    adapter.bump_last_seen("e1")  # no _db attribute -> must not raise

    adapter._db = sqlite3.connect(":memory:")
    adapter._db.execute(
        "CREATE TABLE published_ids (adapter TEXT, event_id TEXT, "
        "first_seen TIMESTAMP, last_seen TIMESTAMP)"
    )
    adapter._db.execute(
        "INSERT INTO published_ids VALUES ('stub', 'e1', "
        "'2020-01-01 00:00:00', '2020-01-01 00:00:00')"
    )
    adapter._db.commit()

    adapter.bump_last_seen("e1")
    row = adapter._db.execute(
        "SELECT last_seen FROM published_ids WHERE event_id = 'e1'"
    ).fetchone()
    assert row[0] != "2020-01-01 00:00:00"  # bumped to CURRENT_TIMESTAMP
