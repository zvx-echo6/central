"""Tests for WFIGS adapters."""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.config_models import AdapterConfig, RegionConfig
from central.models import Event, Geo


# Sample GeoJSON response with incidents
SAMPLE_INCIDENTS_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-116.5, 43.5]},
            "properties": {
                "IrwinID": "GUID-001-BOISE",
                "IncidentName": "Test Fire 1",
                "IncidentTypeCategory": "Wildfire",
                "DailyAcres": 150,
                "PercentContained": 25,
                "FireDiscoveryDateTime": 1716000000000,
                "ModifiedOnDateTime": 1716100000000,
                "POOState": "ID",
                "POOCounty": "Ada",
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-117.0, 44.0]},
            "properties": {
                "IrwinID": "GUID-002-CANYON",
                "IncidentName": "Test Fire 2",
                "IncidentTypeCategory": "PrescribedFire",
                "DailyAcres": 5,
                "PercentContained": 100,
                "FireDiscoveryDateTime": 1716200000000,
                "ModifiedOnDateTime": 1716300000000,
                "POOState": "ID",
                "POOCounty": "Canyon",
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-80.0, 26.0]},
            "properties": {
                "IrwinID": "GUID-003-FLORIDA",
                "IncidentName": "Florida Fire",
                "IncidentTypeCategory": "Wildfire",
                "DailyAcres": 50,
                "PercentContained": 0,
                "FireDiscoveryDateTime": 1716400000000,
                "ModifiedOnDateTime": 1716500000000,
                "POOState": "FL",
                "POOCounty": "Miami-Dade",
            },
        },
    ],
}

# Perimeters API uses prefixed field names (attr_*, poly_*)
SAMPLE_PERIMETERS_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-116.6, 43.4],
                    [-116.4, 43.4],
                    [-116.4, 43.6],
                    [-116.6, 43.6],
                    [-116.6, 43.4],
                ]],
            },
            "properties": {
                "attr_IrwinID": "GUID-001-BOISE",
                "attr_IncidentName": "Test Fire 1",
                "attr_IncidentTypeCategory": "Wildfire",
                "attr_IncidentSize": 150,
                "poly_GISAcres": 148.5,
                "attr_PercentContained": 25,
                "attr_FireDiscoveryDateTime": 1716000000000,
                "attr_ModifiedOnDateTime_dt": 1716100000000,
                "attr_POOState": "ID",
                "attr_POOCounty": "Ada",
            },
        },
    ],
}


class TestWFIGSCommon:
    """Tests for WFIGS common utilities."""

    def test_severity_from_acres_none(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(None) == 0
        assert severity_from_acres(0) == 0

    def test_severity_from_acres_small(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(5) == 1
        assert severity_from_acres(9.9) == 1

    def test_severity_from_acres_medium(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(10) == 2
        assert severity_from_acres(99) == 2

    def test_severity_from_acres_large(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(100) == 3
        assert severity_from_acres(999) == 3

    def test_severity_from_acres_very_large(self):
        from central.adapters.wfigs_common import severity_from_acres
        assert severity_from_acres(1000) == 4
        assert severity_from_acres(100000) == 4

    def test_parse_wfigs_timestamp(self):
        from central.adapters.wfigs_common import parse_wfigs_timestamp
        ts = parse_wfigs_timestamp(1716000000000)
        assert ts is not None
        assert ts.tzinfo == timezone.utc
        assert ts.year == 2024

    def test_parse_wfigs_timestamp_none(self):
        from central.adapters.wfigs_common import parse_wfigs_timestamp
        assert parse_wfigs_timestamp(None) is None

    def test_build_regions_full(self):
        from central.adapters.wfigs_common import build_regions
        regions, primary = build_regions("ID", "Ada")
        assert regions == ["US-ID-ADA"]
        assert primary == "US-ID-ADA"

    def test_build_regions_state_only(self):
        from central.adapters.wfigs_common import build_regions
        regions, primary = build_regions("ID", None)
        assert regions == ["US-ID"]
        assert primary == "US-ID"

    def test_build_regions_none(self):
        from central.adapters.wfigs_common import build_regions
        regions, primary = build_regions(None, None)
        assert regions == []
        assert primary is None

    def test_subject_suffix(self):
        from central.adapters.wfigs_common import subject_suffix
        assert subject_suffix("ID", "Ada") == "id.ada"
        assert subject_suffix("ID", "Ada County") == "id.ada_county"
        assert subject_suffix("ID", None) == "id"
        assert subject_suffix(None, None) == "unknown"

    def test_point_in_bbox(self):
        from central.adapters.wfigs_common import point_in_bbox
        assert point_in_bbox(-116.5, 43.5, -124, 31, -102, 49) is True
        assert point_in_bbox(-80.0, 26.0, -124, 31, -102, 49) is False


class TestWFIGSIncidentsAdapter:
    """Tests for WFIGS Incidents adapter."""

    @pytest.fixture
    def mock_config(self) -> AdapterConfig:
        return AdapterConfig(
            name="wfigs_incidents",
            enabled=True,
            cadence_s=300,
            settings={
                "region": {"north": 49.0, "south": 31.0, "east": -102.0, "west": -124.0}
            },
            updated_at=datetime.now(timezone.utc),
        )

    @pytest.fixture
    def mock_config_store(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def cursor_db_path(self, tmp_path: Path) -> Path:
        return tmp_path / "cursors.db"

    @pytest.mark.asyncio
    async def test_normalization_incidents(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Incidents are correctly normalized to Events."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value=SAMPLE_INCIDENTS_RESPONSE)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # Should have 2 events (Florida filtered out by bbox)
        assert len(events) == 2

        event = events[0]
        assert event.id == "GUID-001-BOISE"
        assert event.adapter == "wfigs_incidents"
        assert event.category == "fire.incident.wildfire"
        assert event.severity == 3  # 150 acres = severity 3 (100-999 range)
        assert event.geo.primary_region == "US-ID-ADA"
        assert event.data["IrwinID"] == "GUID-001-BOISE"

    @pytest.mark.asyncio
    async def test_is_published_dedup(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """is_published/mark_published provides dedup functionality."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        # Initially not published
        assert adapter.is_published("test-id") is False

        # Mark as published
        adapter.mark_published("test-id")

        # Now it should be published
        assert adapter.is_published("test-id") is True

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_fall_off_emits_removal(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Fall-off detection emits removal events."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        # First poll with 2 incidents
        mock_response1 = AsyncMock()
        mock_response1.raise_for_status = MagicMock()
        mock_response1.json = AsyncMock(return_value=SAMPLE_INCIDENTS_RESPONSE)

        # Second poll with only 1 incident (GUID-002 fell off)
        reduced_response = {
            "type": "FeatureCollection",
            "features": [SAMPLE_INCIDENTS_RESPONSE["features"][0]],
        }
        mock_response2 = AsyncMock()
        mock_response2.raise_for_status = MagicMock()
        mock_response2.json = AsyncMock(return_value=reduced_response)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response1), __aexit__=AsyncMock())):
            events1 = [e async for e in adapter.poll()]

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response2), __aexit__=AsyncMock())):
            events2 = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # First poll: 2 incident events
        assert len(events1) == 2

        # Second poll: 1 incident (seen again) + 1 removal for GUID-002
        # The incident event is yielded (supervisor does dedup via is_published)
        # The removal is yielded for GUID-002
        removal_events = [e for e in events2 if e.category == "fire.incident.removed"]
        assert len(removal_events) == 1
        assert removal_events[0].data["irwin_id"] == "GUID-002-CANYON"

    def test_subject_for_incidents(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)

        event = Event(
            id="test-id",
            adapter="wfigs_incidents",
            category="fire.incident.wildfire",
            time=datetime.now(timezone.utc),
            severity=2,
            geo=Geo(primary_region="US-ID-ADA"),
            data={"POOState": "ID", "POOCounty": "Ada"},
        )

        subject = adapter.subject_for(event)
        assert subject == "central.fire.incident.id.ada"

    def test_subject_for_removal(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)

        event = Event(
            id="test-id:removed:2024-01-01",
            adapter="wfigs_incidents",
            category="fire.incident.removed",
            time=datetime.now(timezone.utc),
            severity=0,
            geo=Geo(),
            data={"irwin_id": "test-id", "state": "ID"},
        )

        subject = adapter.subject_for(event)
        assert subject == "central.fire.incident.removed.id"

    @pytest.mark.asyncio
    async def test_bbox_post_filter(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Features outside bbox are filtered out."""
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value=SAMPLE_INCIDENTS_RESPONSE)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # Florida incident should be filtered out
        assert len(events) == 2
        irwin_ids = {e.id for e in events}
        assert "GUID-003-FLORIDA" not in irwin_ids

    @pytest.mark.asyncio
    async def test_apply_config_region_change(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        from central.adapters.wfigs_incidents import WFIGSIncidentsAdapter

        adapter = WFIGSIncidentsAdapter(mock_config, mock_config_store, cursor_db_path)

        assert adapter.region.north == 49.0

        new_config = AdapterConfig(
            name="wfigs_incidents",
            enabled=True,
            cadence_s=300,
            settings={
                "region": {"north": 50.0, "south": 35.0, "east": -100.0, "west": -120.0}
            },
            updated_at=datetime.now(timezone.utc),
        )
        await adapter.apply_config(new_config)

        assert adapter.region.north == 50.0
        assert adapter.region.south == 35.0


class TestWFIGSPerimetersAdapter:
    """Tests for WFIGS Perimeters adapter."""

    @pytest.fixture
    def mock_config(self) -> AdapterConfig:
        return AdapterConfig(
            name="wfigs_perimeters",
            enabled=True,
            cadence_s=300,
            settings={
                "region": {"north": 49.0, "south": 31.0, "east": -102.0, "west": -124.0}
            },
            updated_at=datetime.now(timezone.utc),
        )

    @pytest.fixture
    def mock_config_store(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def cursor_db_path(self, tmp_path: Path) -> Path:
        return tmp_path / "cursors.db"

    @pytest.mark.asyncio
    async def test_normalization_perimeters(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Perimeters are correctly normalized to Events with geometry."""
        from central.adapters.wfigs_perimeters import WFIGSPerimetersAdapter

        adapter = WFIGSPerimetersAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = AsyncMock(return_value=SAMPLE_PERIMETERS_RESPONSE)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        assert len(events) == 1

        event = events[0]
        assert event.id == "GUID-001-BOISE"
        assert event.adapter == "wfigs_perimeters"
        assert event.category == "fire.perimeter.wildfire"
        assert event.geo.primary_region == "US-ID-ADA"
        assert "geometry" in event.data
        assert event.data["geometry"]["type"] == "Polygon"

    def test_subject_for_perimeters(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        from central.adapters.wfigs_perimeters import WFIGSPerimetersAdapter

        adapter = WFIGSPerimetersAdapter(mock_config, mock_config_store, cursor_db_path)

        event = Event(
            id="test-id",
            adapter="wfigs_perimeters",
            category="fire.perimeter.wildfire",
            time=datetime.now(timezone.utc),
            severity=2,
            geo=Geo(primary_region="US-ID-ADA"),
            data={"POOState": "ID", "POOCounty": "Ada", "geometry": {}},
        )

        subject = adapter.subject_for(event)
        assert subject == "central.fire.perimeter.id.ada"
