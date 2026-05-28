"""Tests for USGS earthquake adapter."""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import tempfile

from central.adapters.usgs_quake import (
    USGSQuakeAdapter,
    magnitude_tier,
    magnitude_to_severity,
)
from central.config_models import AdapterConfig, RegionConfig
from central.models import Event, Geo


# Sample USGS GeoJSON response
SAMPLE_GEOJSON = {
    "type": "FeatureCollection",
    "metadata": {
        "generated": 1715878800000,
        "url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
        "title": "USGS All Earthquakes, Past Hour",
        "status": 200,
        "api": "1.10.3",
        "count": 3
    },
    "features": [
        {
            "type": "Feature",
            "properties": {
                "mag": 2.5,
                "place": "10km N of Boise, Idaho",
                "time": 1715878500000,
                "updated": 1715878600000,
                "tz": None,
                "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us1234",
                "detail": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/us1234.geojson",
                "felt": None,
                "cdi": None,
                "mmi": None,
                "alert": None,
                "status": "automatic",
                "tsunami": 0,
                "sig": 100,
                "net": "us",
                "code": "1234",
                "ids": ",us1234,",
                "sources": ",us,",
                "types": ",origin,",
                "nst": 10,
                "dmin": 0.5,
                "rms": 0.3,
                "gap": 100,
                "magType": "ml",
                "type": "earthquake",
                "title": "M 2.5 - 10km N of Boise, Idaho"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-116.2, 43.7, 10.5]
            },
            "id": "us1234"
        },
        {
            "type": "Feature",
            "properties": {
                "mag": 4.5,
                "place": "20km S of Portland, Oregon",
                "time": 1715878400000,
                "updated": 1715878500000,
                "tz": None,
                "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us5678",
                "detail": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/us5678.geojson",
                "felt": 50,
                "cdi": 4.0,
                "mmi": 3.5,
                "alert": "green",
                "status": "reviewed",
                "tsunami": 0,
                "sig": 300,
                "net": "us",
                "code": "5678",
                "ids": ",us5678,",
                "sources": ",us,",
                "types": ",origin,shakemap,",
                "nst": 25,
                "dmin": 0.2,
                "rms": 0.2,
                "gap": 50,
                "magType": "mw",
                "type": "earthquake",
                "title": "M 4.5 - 20km S of Portland, Oregon"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-122.6, 45.3, 15.0]
            },
            "id": "us5678"
        },
        {
            "type": "Feature",
            "properties": {
                "mag": 3.0,
                "place": "50km E of San Francisco, California",
                "time": 1715878300000,
                "updated": 1715878400000,
                "tz": None,
                "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us9999",
                "detail": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/us9999.geojson",
                "felt": None,
                "cdi": None,
                "mmi": None,
                "alert": None,
                "status": "automatic",
                "tsunami": 0,
                "sig": 150,
                "net": "us",
                "code": "9999",
                "ids": ",us9999,",
                "sources": ",us,",
                "types": ",origin,",
                "nst": 15,
                "dmin": 0.3,
                "rms": 0.25,
                "gap": 80,
                "magType": "ml",
                "type": "earthquake",
                "title": "M 3.0 - 50km E of San Francisco, California"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-121.5, 37.8, 8.0]
            },
            "id": "us9999"
        }
    ]
}

# Sample with null magnitude
SAMPLE_NULL_MAG = {
    "type": "FeatureCollection",
    "metadata": {"count": 1},
    "features": [
        {
            "type": "Feature",
            "properties": {
                "mag": None,
                "place": "Quarry blast",
                "time": 1715878500000,
                "type": "quarry blast"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-116.0, 44.0, 0.0]
            },
            "id": "usquarry1"
        }
    ]
}


def make_adapter_config(
    region: dict | None = None,
    feed: str = "all_hour",
) -> AdapterConfig:
    """Create an AdapterConfig for testing."""
    settings = {"feed": feed}
    if region:
        settings["region"] = region
    else:
        settings["region"] = {
            "north": 49.5,
            "south": 40.0,
            "east": -110.0,
            "west": -125.0,
        }

    return AdapterConfig(
        name="usgs_quake",
        enabled=True,
        cadence_s=60,
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
    return MagicMock()


class TestMagnitudeTier:
    """Test magnitude tier classification."""

    def test_minor(self):
        assert magnitude_tier(0.5) == "minor"
        assert magnitude_tier(2.9) == "minor"

    def test_light(self):
        assert magnitude_tier(3.0) == "light"
        assert magnitude_tier(3.9) == "light"

    def test_moderate(self):
        assert magnitude_tier(4.0) == "moderate"
        assert magnitude_tier(4.9) == "moderate"

    def test_strong(self):
        assert magnitude_tier(5.0) == "strong"
        assert magnitude_tier(5.9) == "strong"

    def test_major(self):
        assert magnitude_tier(6.0) == "major"
        assert magnitude_tier(6.9) == "major"

    def test_great(self):
        assert magnitude_tier(7.0) == "great"
        assert magnitude_tier(9.5) == "great"


class TestMagnitudeToSeverity:
    """Test magnitude to severity mapping."""

    def test_severity_levels(self):
        assert magnitude_to_severity(2.0) == 0
        assert magnitude_to_severity(3.5) == 1
        assert magnitude_to_severity(4.5) == 2
        assert magnitude_to_severity(5.5) == 3
        assert magnitude_to_severity(6.5) == 4
        assert magnitude_to_severity(7.5) == 5


class TestRegionFiltering:
    """Test region/bbox filtering."""

    @pytest.mark.asyncio
    async def test_filters_out_of_bbox(self, temp_db_path, mock_config_store):
        """Test that quakes outside bbox are filtered."""
        # Region covers PNW only (north of 40, west of -110)
        config = make_adapter_config(
            region={"north": 49.5, "south": 40.0, "east": -110.0, "west": -125.0}
        )
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        with patch.object(adapter, "_fetch_geojson", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_GEOJSON

            events = []
            async for event in adapter.poll():
                events.append(event)

            # us1234 (Boise) and us5678 (Portland) are in bbox
            # us9999 (SF, lat 37.8) is outside bbox (south < 40)
            assert len(events) == 2
            event_ids = {e.id for e in events}
            assert "us1234" in event_ids
            assert "us5678" in event_ids
            assert "us9999" not in event_ids

        await adapter.shutdown()


class TestDeduplication:
    """Test deduplication logic."""

    @pytest.mark.asyncio
    async def test_dedup_marks_published(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        event_id = "us1234"

        assert not adapter.is_published(event_id)
        adapter.mark_published(event_id)
        assert adapter.is_published(event_id)

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_second_poll_no_duplicates(self, temp_db_path, mock_config_store):
        """Test that second poll with same events yields nothing."""
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        with patch.object(adapter, "_fetch_geojson", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_GEOJSON

            # First poll
            events1 = []
            async for event in adapter.poll():
                events1.append(event)

            # Second poll - same data
            events2 = []
            async for event in adapter.poll():
                events2.append(event)

            # First poll should have events (2 in bbox)
            assert len(events1) == 2
            # Second poll should have 0 (all deduped)
            assert len(events2) == 0

        await adapter.shutdown()


class TestNullMagnitude:
    """Test handling of null magnitude events."""

    @pytest.mark.asyncio
    async def test_skips_null_magnitude(self, temp_db_path, mock_config_store):
        """Test that events with null magnitude are skipped."""
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        with patch.object(adapter, "_fetch_geojson", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_NULL_MAG

            events = []
            async for event in adapter.poll():
                events.append(event)

            # Should skip the null-magnitude event
            assert len(events) == 0

        await adapter.shutdown()


class TestEventGeneration:
    """Test Event generation from features."""

    @pytest.mark.asyncio
    async def test_event_category(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        with patch.object(adapter, "_fetch_geojson", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_GEOJSON

            events = []
            async for event in adapter.poll():
                events.append(event)

            # Check categories
            categories = {e.category for e in events}
            # us1234 is M2.5 -> minor, us5678 is M4.5 -> moderate
            assert "quake.event.minor" in categories
            assert "quake.event.moderate" in categories

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_event_severity(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        with patch.object(adapter, "_fetch_geojson", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_GEOJSON

            events = []
            async for event in adapter.poll():
                events.append(event)

            # Find events by ID
            events_by_id = {e.id: e for e in events}

            # M2.5 -> severity 0
            assert events_by_id["us1234"].severity == 0
            # M4.5 -> severity 2
            assert events_by_id["us5678"].severity == 2

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_event_geo(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        with patch.object(adapter, "_fetch_geojson", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = SAMPLE_GEOJSON

            events = []
            async for event in adapter.poll():
                events.append(event)

            events_by_id = {e.id: e for e in events}

            # Check Boise quake coordinates
            boise = events_by_id["us1234"]
            assert boise.geo.centroid == (-116.2, 43.7)

        await adapter.shutdown()


class TestApplyConfig:
    """Test hot-reload configuration application."""

    @pytest.mark.asyncio
    async def test_apply_config_updates_region(self, temp_db_path, mock_config_store):
        config = make_adapter_config(
            region={"north": 49.5, "south": 40.0, "east": -110.0, "west": -125.0}
        )
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        assert adapter.region.north == 49.5

        new_config = make_adapter_config(
            region={"north": 48.0, "south": 45.0, "east": -115.0, "west": -125.0}
        )
        await adapter.apply_config(new_config)

        assert adapter.region.north == 48.0
        assert adapter.region.south == 45.0

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_apply_config_updates_feed(self, temp_db_path, mock_config_store):
        config = make_adapter_config(feed="all_hour")
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        await adapter.startup()

        assert adapter._feed == "all_hour"

        new_config = make_adapter_config(feed="all_day")
        await adapter.apply_config(new_config)

        assert adapter._feed == "all_day"

        await adapter.shutdown()



class TestSubjectFor:
    """Test subject_for method for all magnitude tiers."""

    @pytest.mark.asyncio
    async def test_subject_minor(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        event = Event(
            id="test-minor",
            adapter="usgs_quake",
            category="quake.event.minor",
            time=datetime.now(timezone.utc),
            severity=0,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )
        assert adapter.subject_for(event) == "central.quake.event.minor.unknown"

    @pytest.mark.asyncio
    async def test_subject_light(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        event = Event(
            id="test-light",
            adapter="usgs_quake",
            category="quake.event.light",
            time=datetime.now(timezone.utc),
            severity=1,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )
        assert adapter.subject_for(event) == "central.quake.event.light.unknown"

    @pytest.mark.asyncio
    async def test_subject_moderate(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        event = Event(
            id="test-moderate",
            adapter="usgs_quake",
            category="quake.event.moderate",
            time=datetime.now(timezone.utc),
            severity=2,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )
        assert adapter.subject_for(event) == "central.quake.event.moderate.unknown"

    @pytest.mark.asyncio
    async def test_subject_strong(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        event = Event(
            id="test-strong",
            adapter="usgs_quake",
            category="quake.event.strong",
            time=datetime.now(timezone.utc),
            severity=3,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )
        assert adapter.subject_for(event) == "central.quake.event.strong.unknown"

    @pytest.mark.asyncio
    async def test_subject_major(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        event = Event(
            id="test-major",
            adapter="usgs_quake",
            category="quake.event.major",
            time=datetime.now(timezone.utc),
            severity=4,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )
        assert adapter.subject_for(event) == "central.quake.event.major.unknown"

    @pytest.mark.asyncio
    async def test_subject_great(self, temp_db_path, mock_config_store):
        config = make_adapter_config()
        adapter = USGSQuakeAdapter(
            config=config,
            config_store=mock_config_store,
            cursor_db_path=temp_db_path,
        )
        event = Event(
            id="test-great",
            adapter="usgs_quake",
            category="quake.event.great",
            time=datetime.now(timezone.utc),
            severity=5,
            geo=Geo(centroid=(-116.0, 45.0)),
            data={},
        )
        assert adapter.subject_for(event) == "central.quake.event.great.unknown"
