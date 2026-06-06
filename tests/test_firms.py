"""Tests for FIRMS adapter."""

import json
import math
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import tempfile

from central.adapters.firms import (
    FIRMSAdapter,
    CONFIDENCE_MAP,
    SATELLITE_SHORT,
    _pixel_polygon,
)
from central.monitoring_area import build_geom_json
from central.config_models import AdapterConfig
from central.models import Event, Geo


# Sample FIRMS CSV response
SAMPLE_CSV = """latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_ti5,frp,daynight
45.123,-116.456,320.5,0.39,0.36,2026-05-16,1430,N,VIIRS,h,2.0NRT,290.2,15.3,D
46.789,-117.012,305.2,0.41,0.38,2026-05-16,1430,N,VIIRS,n,2.0NRT,285.1,8.7,D
45.123,-116.456,318.9,0.40,0.37,2026-05-16,1430,N,VIIRS,l,2.0NRT,288.5,12.1,D
"""

# Sample CSV with duplicate (same location, date, time)
SAMPLE_CSV_WITH_DUPE = """latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,confidence,version,bright_ti5,frp,daynight
45.123,-116.456,320.5,0.39,0.36,2026-05-16,1430,N,VIIRS,h,2.0NRT,290.2,15.3,D
45.123,-116.456,320.5,0.39,0.36,2026-05-16,1430,N,VIIRS,h,2.0NRT,290.2,15.3,D
"""


def make_adapter_config(
    region: dict | None = None,
    satellites: list[str] | None = None,
) -> AdapterConfig:
    """Create an AdapterConfig for testing."""
    settings = {
        "api_key_alias": "firms",
        "satellites": satellites or ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"],
    }
    if region:
        settings["region"] = region
    else:
        settings["region"] = {
            "north": 49.5,
            "south": 31.0,
            "east": -102.0,
            "west": -124.5,
        }

    return AdapterConfig(
        name="firms",
        enabled=True,
        cadence_s=300,
        settings=settings,
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def temp_db_path():
    """Create a temporary database path for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        yield Path(f.name)


@pytest.fixture
def mock_config_store():
    """Create a mock ConfigStore."""
    store = MagicMock()
    store.get_api_key = AsyncMock(return_value="test_api_key")
    return store


class TestConfidenceMapping:
    """Test confidence value mapping."""

    def test_low_confidence(self):
        assert CONFIDENCE_MAP["l"] == "low"

    def test_nominal_confidence(self):
        assert CONFIDENCE_MAP["n"] == "nominal"

    def test_high_confidence(self):
        assert CONFIDENCE_MAP["h"] == "high"


class TestSatelliteShortNames:
    """Test satellite short name mapping."""

    def test_snpp_short_name(self):
        assert SATELLITE_SHORT["VIIRS_SNPP_NRT"] == "viirs_snpp"

    def test_noaa20_short_name(self):
        assert SATELLITE_SHORT["VIIRS_NOAA20_NRT"] == "viirs_noaa20"

    def test_noaa21_short_name(self):
        assert SATELLITE_SHORT["VIIRS_NOAA21_NRT"] == "viirs_noaa21"


class TestStableIdGeneration:
    """Test stable ID generation for deduplication."""

    @pytest.mark.asyncio
    async def test_stable_id_format(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )

        stable_id = adapter._build_stable_id(
            satellite="VIIRS_SNPP_NRT",
            acq_date="2026-05-16",
            acq_time="1430",
            lat=45.1234567,
            lon=-116.4567890,
        )

        # Should be rounded to 3 decimal places
        assert stable_id == "VIIRS_SNPP_NRT:2026-05-16:1430:45.123:-116.457"

    @pytest.mark.asyncio
    async def test_stable_id_rounding(self, temp_db_path, mock_config_store):
        """Test that small lat/lon differences within 0.001 round to same ID."""
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )

        # Values that differ by less than 0.0005 should round to same value
        id1 = adapter._build_stable_id("SAT", "2026-05-16", "1430", 45.1234, -116.4564)
        id2 = adapter._build_stable_id("SAT", "2026-05-16", "1430", 45.1232, -116.4562)

        # Both should round to 45.124, -116.457
        assert id1 == id2


class TestCsvParsing:
    """Test CSV parsing."""

    @pytest.mark.asyncio
    async def test_parse_csv_rows(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )

        rows = adapter._parse_csv(SAMPLE_CSV, "VIIRS_SNPP_NRT")

        assert len(rows) == 3
        assert rows[0]["latitude"] == 45.123
        assert rows[0]["longitude"] == -116.456
        assert rows[0]["confidence"] == "high"
        assert rows[1]["confidence"] == "nominal"
        assert rows[2]["confidence"] == "low"

    @pytest.mark.asyncio
    async def test_parse_csv_brightness(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )

        rows = adapter._parse_csv(SAMPLE_CSV, "VIIRS_SNPP_NRT")

        assert rows[0]["bright_ti4"] == 320.5
        assert rows[0]["bright_ti5"] == 290.2
        assert rows[0]["frp"] == 15.3


class TestEventGeneration:
    """Test Event generation from CSV rows."""

    @pytest.mark.asyncio
    async def test_event_category(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )

        rows = adapter._parse_csv(SAMPLE_CSV, "VIIRS_SNPP_NRT")
        event = adapter._row_to_event(rows[0], "VIIRS_SNPP_NRT")

        assert event.category == "fire.hotspot.viirs_snpp.high"

    @pytest.mark.asyncio
    async def test_event_severity(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )

        rows = adapter._parse_csv(SAMPLE_CSV, "VIIRS_SNPP_NRT")

        high_event = adapter._row_to_event(rows[0], "VIIRS_SNPP_NRT")
        nominal_event = adapter._row_to_event(rows[1], "VIIRS_SNPP_NRT")
        low_event = adapter._row_to_event(rows[2], "VIIRS_SNPP_NRT")

        assert high_event.severity == 3
        assert nominal_event.severity == 2
        assert low_event.severity == 1

    @pytest.mark.asyncio
    async def test_event_geo(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )

        rows = adapter._parse_csv(SAMPLE_CSV, "VIIRS_SNPP_NRT")
        event = adapter._row_to_event(rows[0], "VIIRS_SNPP_NRT")

        # GeoJSON order: lon, lat. v0.9.21: degenerate point-bbox was dropped;
        # the pixel footprint now ships as geo.geometry (Polygon).
        assert event.geo.centroid == (-116.456, 45.123)
        assert event.geo.bbox is None
        assert event.geo.geometry is not None
        assert event.geo.geometry["type"] == "Polygon"


class TestDeduplication:
    """Test deduplication logic."""

    @pytest.mark.asyncio
    async def test_dedup_marks_published(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        stable_id = "VIIRS_SNPP_NRT:2026-05-16:1430:45.123:-116.456"

        # Not published initially
        assert not adapter.is_published(stable_id)

        # Mark as published
        adapter.mark_published(stable_id)

        # Now should be published
        assert adapter.is_published(stable_id)

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_dedup_prevents_duplicates(self, temp_db_path, mock_config_store):
        """Test that duplicate rows don't produce duplicate events."""
        # Use only one satellite to simplify the test
        config = make_adapter_config(satellites=["VIIRS_SNPP_NRT"])
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        # Mock the fetch to return CSV with duplicates
        with patch.object(adapter, "_fetch_csv", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_CSV_WITH_DUPE

            events = []
            async for event in adapter.poll():
                events.append(event)

            # Should only get one event despite two identical rows
            assert len(events) == 1

        await adapter.shutdown()


class TestSubjectGeneration:
    """Test subject generation for fire hotspots."""

    @pytest.mark.asyncio
    async def test_subject_format(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        event = Event(
            id="test",
            adapter="firms",
            category="fire.hotspot.viirs_snpp.high",
            time=datetime.now(timezone.utc),
            severity=3,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )

        subject = adapter.subject_for(event)
        assert subject == "central.fire.hotspot.viirs_snpp.high.unknown"

    @pytest.mark.asyncio
    async def test_subject_nominal_confidence(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        event = Event(
            id="test",
            adapter="firms",
            category="fire.hotspot.viirs_noaa20.nominal",
            time=datetime.now(timezone.utc),
            severity=2,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )

        subject = adapter.subject_for(event)
        assert subject == "central.fire.hotspot.viirs_noaa20.nominal.unknown"


class TestUrlBuilding:
    """Test FIRMS API URL building."""

    @pytest.mark.asyncio
    async def test_url_format(self, temp_db_path, mock_config_store):
        config = make_adapter_config(
            region={"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}
        )
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        url = adapter._build_url("VIIRS_SNPP_NRT")

        assert url is not None
        assert "test_api_key" in url
        assert "VIIRS_SNPP_NRT" in url
        assert "-124.5,31.0,-102.0,49.5" in url  # west,south,east,north
        assert "/1" in url  # dayRange

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_url_none_without_key(self, temp_db_path):
        mock_store = MagicMock()
        mock_store.get_api_key = AsyncMock(return_value=None)

        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        url = adapter._build_url("VIIRS_SNPP_NRT")

        assert url is None

        await adapter.shutdown()


class TestApplyConfig:
    """Test hot-reload configuration application."""

    @pytest.mark.asyncio
    async def test_apply_config_updates_region(self, temp_db_path, mock_config_store):
        config = make_adapter_config(
            region={"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}
        )
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        # Original region
        assert adapter.region.north == 49.5

        # Apply new config with different region
        new_config = make_adapter_config(
            region={"north": 48.0, "south": 45.0, "east": -115.0, "west": -125.0}
        )
        await adapter.apply_config(new_config)

        assert adapter.region.north == 48.0
        assert adapter.region.south == 45.0

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_apply_config_updates_satellites(self, temp_db_path, mock_config_store):
        config = make_adapter_config(satellites=["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"])
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        # Original satellites
        assert len(adapter._satellites) == 2

        # Apply config with single satellite
        new_config = make_adapter_config(satellites=["VIIRS_NOAA20_NRT"])
        await adapter.apply_config(new_config)

        assert adapter._satellites == ["VIIRS_NOAA20_NRT"]

        await adapter.shutdown()


class TestEnrichmentIntegration:
    """FIRMS is the PR J enrichment pilot."""

    def test_enrichment_locations_declared_and_resolvable(self, temp_db_path, mock_config_store):
        """FIRMS declares enrichment_locations and the declared paths actually
        resolve to coordinates in a real event's data — verified structurally,
        not by hardcoding the literal tuple."""
        locations = getattr(FIRMSAdapter, "enrichment_locations")
        assert isinstance(locations, list) and len(locations) >= 1
        for tup in locations:
            assert isinstance(tup, tuple) and len(tup) == 2
            assert all(isinstance(p, str) for p in tup)

        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        rows = adapter._parse_csv(SAMPLE_CSV, "VIIRS_SNPP_NRT")
        event = adapter._row_to_event(rows[0], "VIIRS_SNPP_NRT")
        # Every declared (lat_path, lon_path) must resolve to a float in data.
        for lat_path, lon_path in locations:
            assert isinstance(event.data.get(lat_path), float)
            assert isinstance(event.data.get(lon_path), float)

    @pytest.mark.asyncio
    async def test_event_passes_through_supervisor_enrichment(
        self, tmp_path, temp_db_path, mock_config_store
    ):
        """A FIRMS event run through the supervisor's enrichment stage emerges
        with data._enriched.geocoder populated (all-null under NoOpBackend)."""
        from central.config_models import EnrichmentConfig
        from central.enrichment.cache import EnrichmentCache
        from central.enrichment.geocoder import all_null_bundle
        from central.supervisor import apply_enrichment, build_enrichers

        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        rows = adapter._parse_csv(SAMPLE_CSV, "VIIRS_SNPP_NRT")
        event = adapter._row_to_event(rows[0], "VIIRS_SNPP_NRT")
        assert "_enriched" not in event.data

        cache = EnrichmentCache(tmp_path / "enrichment_cache.db")
        enrichers = build_enrichers(EnrichmentConfig(), cache)
        await apply_enrichment(event, adapter.enrichment_locations, enrichers)

        assert "_enriched" in event.data
        assert event.data["_enriched"]["geocoder"] == all_null_bundle()


class TestPixelPolygon:
    """v0.9.21: rectangular GeoJSON Polygon from FIRMS scan/track."""

    def test_polygon_geometry_constructed(self):
        """Happy path: realistic VIIRS scan/track -> CCW Polygon with closing vertex."""
        # VIIRS at nadir: ~0.375km square; centroid should lie inside the ring.
        geom = _pixel_polygon(lat=42.0, lon=-114.0, scan=0.375, track=0.375)
        assert geom is not None
        assert geom["type"] == "Polygon"
        ring = geom["coordinates"][0]
        # 4 corners + duplicated closing vertex
        assert len(ring) == 5
        assert ring[0] == ring[-1], "ring must close on its first vertex"
        # CCW: signed area of the ring should be positive (lon=x, lat=y).
        signed_area = sum(
            (ring[i][0] * ring[i + 1][1]) - (ring[i + 1][0] * ring[i][1])
            for i in range(len(ring) - 1)
        )
        assert signed_area > 0, "ring must wind counter-clockwise"
        # Centroid is inside (axis-aligned rectangle, so min/max bounds check it).
        lons = [pt[0] for pt in ring]
        lats = [pt[1] for pt in ring]
        assert min(lons) < -114.0 < max(lons)
        assert min(lats) < 42.0 < max(lats)

    def test_polygon_at_high_latitude(self):
        """At lat=70°, longitude degrees stretch by 1/cos(70°) ≈ 2.92x."""
        low = _pixel_polygon(lat=0.0, lon=0.0, scan=0.5, track=0.5)
        high = _pixel_polygon(lat=70.0, lon=0.0, scan=0.5, track=0.5)
        assert low is not None and high is not None

        # half_track_deg at lat=70 should be ≈ 1/cos(70°) larger than at lat=0.
        half_track_low = max(pt[0] for pt in low["coordinates"][0])
        half_track_high = max(pt[0] for pt in high["coordinates"][0])
        ratio = half_track_high / half_track_low
        expected = 1.0 / math.cos(math.radians(70.0))
        assert ratio == pytest.approx(expected, rel=1e-6)

        # Latitude (scan) extent is independent of lat — same at both.
        half_scan_low = max(pt[1] for pt in low["coordinates"][0])
        half_scan_high = max(pt[1] for pt in high["coordinates"][0]) - 70.0
        assert half_scan_low == pytest.approx(half_scan_high, rel=1e-9)

    def test_pole_clamp(self):
        """lat=89.99 must clamp to 89.0 instead of divide-by-near-zero."""
        # Without clamping, cos(89.99°) ≈ 1.7e-4, blowing half_track_deg up
        # to ~3000 deg. With the clamp at 89.0, cos(89°) ≈ 0.01745 keeps the
        # polygon finite.
        geom = _pixel_polygon(lat=89.99, lon=10.0, scan=0.5, track=0.5)
        assert geom is not None
        ring = geom["coordinates"][0]
        half_track = max(pt[0] for pt in ring) - 10.0
        expected = (0.5 / 2.0) / (111.0 * math.cos(math.radians(89.0)))
        assert half_track == pytest.approx(expected, rel=1e-9)

    def test_missing_scan_falls_back_to_point(self):
        """scan=None must return None geometry (caller falls back to centroid)."""
        assert _pixel_polygon(42.0, -114.0, None, 0.5) is None
        assert _pixel_polygon(42.0, -114.0, 0.5, None) is None

    def test_invalid_dims_fall_back(self):
        """Non-numeric / non-positive scan or track must return None."""
        assert _pixel_polygon(42.0, -114.0, "not-a-number", 0.5) is None
        assert _pixel_polygon(42.0, -114.0, 0.0, 0.5) is None
        assert _pixel_polygon(42.0, -114.0, -0.5, 0.5) is None


class TestGeoGeometryRoundTripsThroughArchive:
    """Regression guard: FIRMS geo.geometry must reach build_geom_json as Polygon."""

    @pytest.mark.asyncio
    async def test_geo_geometry_round_trips_through_archive_path(
        self, temp_db_path, mock_config_store
    ):
        """Event with scan/track produces a Polygon SQL clause (mirrors tomtom_flow)."""
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        rows = adapter._parse_csv(SAMPLE_CSV, "VIIRS_SNPP_NRT")
        event = adapter._row_to_event(rows[0], "VIIRS_SNPP_NRT")

        # Simulate what archive does: serialize geo to dict and run it through
        # the same helper that produces the PostGIS geom clause.
        geo_dict = event.geo.model_dump()
        sql_clause = build_geom_json(geo_dict)
        assert sql_clause is not None
        decoded = json.loads(sql_clause)
        assert decoded["type"] == "Polygon"
        # Real footprint, not a degenerate point — first and third corners
        # bracket the centroid in both axes.
        ring = decoded["coordinates"][0]
        assert ring[0] != ring[2]

    @pytest.mark.asyncio
    async def test_missing_dims_round_trips_as_point(
        self, temp_db_path, mock_config_store
    ):
        """Without scan/track, archive still gets a usable Point geometry."""
        config = make_adapter_config()
        adapter = FIRMSAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        rows = adapter._parse_csv(SAMPLE_CSV, "VIIRS_SNPP_NRT")
        # Strip scan/track to force fallback path.
        rows[0]["scan"] = None
        rows[0]["track"] = None
        event = adapter._row_to_event(rows[0], "VIIRS_SNPP_NRT")
        assert event.geo.geometry is None

        geo_dict = event.geo.model_dump()
        sql_clause = build_geom_json(geo_dict)
        assert sql_clause is not None
        assert json.loads(sql_clause)["type"] == "Point"
