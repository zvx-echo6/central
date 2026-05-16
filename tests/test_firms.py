"""Tests for FIRMS adapter."""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import tempfile

from central.adapters.firms import (
    FIRMSAdapter,
    CONFIDENCE_MAP,
    SATELLITE_SHORT,
    subject_for_fire_hotspot,
)
from central.config_models import AdapterConfig, RegionConfig
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

        # GeoJSON order: lon, lat
        assert event.geo.centroid == (-116.456, 45.123)
        assert event.geo.bbox == (-116.456, 45.123, -116.456, 45.123)


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

    def test_subject_format(self):
        event = Event(
            id="test",
            source="central/adapters/firms",
            category="fire.hotspot.viirs_snpp.high",
            time=datetime.now(timezone.utc),
            severity=3,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )

        subject = subject_for_fire_hotspot(event)
        assert subject == "central.fire.hotspot.viirs_snpp.high"

    def test_subject_nominal_confidence(self):
        event = Event(
            id="test",
            source="central/adapters/firms",
            category="fire.hotspot.viirs_noaa20.nominal",
            time=datetime.now(timezone.utc),
            severity=2,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )

        subject = subject_for_fire_hotspot(event)
        assert subject == "central.fire.hotspot.viirs_noaa20.nominal"


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
