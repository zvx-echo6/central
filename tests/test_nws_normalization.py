"""Tests for NWS adapter normalization."""

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from unittest.mock import MagicMock

import pytest

from central.adapters.nws import (
    NWSAdapter,
    _snake_case,
    _parse_datetime,
    _extract_states_from_codes,
    _build_regions,
    _compute_centroid,
    _compute_bbox,
    SEVERITY_MAP,
)
from central.config_models import AdapterConfig


# Sample NWS GeoJSON features for testing
# SAME codes: 6 digits, first 2 are state FIPS (ID=16, OR=41, CA=06, WA=53)
SAMPLE_FEATURE_ID = {
    "id": "urn:oid:2.49.0.1.840.0.a1b2c3d4e5f6",
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[
            [-116.5, 43.5],
            [-116.0, 43.5],
            [-116.0, 44.0],
            [-116.5, 44.0],
            [-116.5, 43.5],
        ]]
    },
    "properties": {
        "id": "urn:oid:2.49.0.1.840.0.a1b2c3d4e5f6",
        "event": "Severe Thunderstorm Warning",
        "sent": "2026-05-15T12:00:00-06:00",
        "expires": "2026-05-15T14:00:00-06:00",
        "severity": "Severe",
        "geocode": {
            "SAME": ["160001"],  # Idaho state FIPS 16
            "UGC": ["IDC001", "IDZ033"],
        },
    },
}

SAMPLE_FEATURE_OR = {
    "id": "urn:oid:2.49.0.1.840.0.x1y2z3w4",
    "type": "Feature",
    "geometry": {"type": "Point", "coordinates": [-122.7, 45.5]},  # Portland, OR
    "properties": {
        "id": "urn:oid:2.49.0.1.840.0.x1y2z3w4",
        "event": "Winter Storm Warning",
        "sent": "2026-05-15T08:00:00Z",
        "expires": "2026-05-16T08:00:00Z",
        "severity": "Moderate",
        "geocode": {
            "SAME": ["410051"],  # Oregon state FIPS 41
            "UGC": ["ORC051"],
        },
    },
}

SAMPLE_FEATURE_CA = {
    "id": "urn:oid:2.49.0.1.840.0.ca1234",
    "type": "Feature",
    "geometry": {
        "type": "Point",
        "coordinates": [-118.25, 34.05],
    },
    "properties": {
        "id": "urn:oid:2.49.0.1.840.0.ca1234",
        "event": "Fire Weather Watch",
        "sent": "2026-05-15T10:00:00-07:00",
        "expires": "2026-05-16T18:00:00-07:00",
        "severity": "Minor",
        "geocode": {
            "SAME": ["060037"],  # California state FIPS 06
            "UGC": ["CAZ568"],
        },
    },
}

SAMPLE_FEATURE_UNKNOWN_SEVERITY = {
    "id": "urn:oid:2.49.0.1.840.0.unk123",
    "type": "Feature",
    "geometry": None,
    "properties": {
        "id": "urn:oid:2.49.0.1.840.0.unk123",
        "event": "Test Alert",
        "sent": "2026-05-15T12:00:00Z",
        "expires": None,
        "severity": "Unknown",
        "geocode": {
            "SAME": ["530033"],  # Washington state FIPS 53
            "UGC": ["WAC033"],
        },
    },
}


class TestSnakeCase:
    """Tests for snake_case conversion."""

    def test_spaces_to_underscores(self) -> None:
        assert _snake_case("Severe Thunderstorm Warning") == "severe_thunderstorm_warning"

    def test_removes_special_chars(self) -> None:
        assert _snake_case("Fire Weather (Red Flag)") == "fire_weather_red_flag"

    def test_lowercase(self) -> None:
        assert _snake_case("TORNADO WARNING") == "tornado_warning"


class TestParseDatetime:
    """Tests for datetime parsing."""

    def test_iso_with_offset(self) -> None:
        result = _parse_datetime("2026-05-15T12:00:00-06:00")
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.hour == 18  # 12:00 MDT = 18:00 UTC

    def test_iso_with_z(self) -> None:
        result = _parse_datetime("2026-05-15T12:00:00Z")
        assert result is not None
        assert result.hour == 12

    def test_none_input(self) -> None:
        assert _parse_datetime(None) is None

    def test_invalid_input(self) -> None:
        assert _parse_datetime("not a date") is None


class TestExtractStates:
    """Tests for state extraction from geocodes."""

    def test_same_codes(self) -> None:
        # Idaho FIPS is 16
        states = _extract_states_from_codes(["160001", "160003"], [])
        assert states == {"ID"}

    def test_ugc_codes(self) -> None:
        states = _extract_states_from_codes([], ["IDC001", "ORC051"])
        assert states == {"ID", "OR"}

    def test_combined(self) -> None:
        # Idaho FIPS is 16
        states = _extract_states_from_codes(["160001"], ["WAC033"])
        assert states == {"ID", "WA"}

    def test_empty(self) -> None:
        states = _extract_states_from_codes([], [])
        assert states == set()


class TestBuildRegions:
    """Tests for region string building."""

    def test_same_to_fips_region(self) -> None:
        # Idaho FIPS is 16
        regions = _build_regions(["160001"], [])
        assert "US-ID-FIPS160001" in regions

    def test_ugc_county(self) -> None:
        regions = _build_regions([], ["IDC001"])
        assert "US-ID-C001" in regions

    def test_ugc_zone(self) -> None:
        regions = _build_regions([], ["IDZ033"])
        assert "US-ID-Z033" in regions

    def test_sorted_alphabetically(self) -> None:
        regions = _build_regions(["160001"], ["IDC001", "IDZ033"])
        assert regions == sorted(regions)


class TestStateFilter:
    """Tests for region-based filtering."""

    @pytest.fixture
    def adapter(self, tmp_path: Path) -> NWSAdapter:
        """Create adapter with Pacific Northwest region (excludes CA)."""
        config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=60,
            settings={
                "contact_email": "test@example.com",
                # Pacific NW region: WA/OR/ID - excludes CA (LA at 34N, region starts at 42N)
                "region": {"north": 49.0, "south": 42.0, "east": -104.0, "west": -125.0},
            },
            updated_at=datetime.now(timezone.utc),
        )
        mock_config_store = MagicMock()
        return NWSAdapter(config, mock_config_store, tmp_path / "test.db")

    def test_accepts_id_feature(self, adapter: NWSAdapter) -> None:
        event = adapter._normalize_feature(SAMPLE_FEATURE_ID)
        assert event is not None
        assert event.id == SAMPLE_FEATURE_ID["id"]

    def test_accepts_or_feature(self, adapter: NWSAdapter) -> None:
        event = adapter._normalize_feature(SAMPLE_FEATURE_OR)
        assert event is not None
        assert event.id == SAMPLE_FEATURE_OR["id"]

    def test_rejects_ca_feature(self, adapter: NWSAdapter) -> None:
        event = adapter._normalize_feature(SAMPLE_FEATURE_CA)
        assert event is None


class TestSeverityMapping:
    """Tests for severity mapping."""

    def test_extreme(self) -> None:
        assert SEVERITY_MAP["Extreme"] == 4

    def test_severe(self) -> None:
        assert SEVERITY_MAP["Severe"] == 3

    def test_moderate(self) -> None:
        assert SEVERITY_MAP["Moderate"] == 2

    def test_minor(self) -> None:
        assert SEVERITY_MAP["Minor"] == 1

    def test_unknown(self) -> None:
        assert SEVERITY_MAP["Unknown"] is None

    def test_unknown_severity_in_feature(self, tmp_path: Path) -> None:
        config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=60,
            settings={
                "contact_email": "test@example.com",
                # No region = accept all features
            },
            updated_at=datetime.now(timezone.utc),
        )
        mock_config_store = MagicMock()
        adapter = NWSAdapter(config, mock_config_store, tmp_path / "test.db")
        event = adapter._normalize_feature(SAMPLE_FEATURE_UNKNOWN_SEVERITY)
        assert event is not None
        assert event.severity is None


class TestSubjectDerivation:
    """Tests for NATS subject derivation."""

    @pytest.fixture
    def adapter(self, tmp_path: Path) -> NWSAdapter:
        config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=60,
            settings={
                "contact_email": "test@example.com",
                # No region = accept all features
            },
            updated_at=datetime.now(timezone.utc),
        )
        mock_config_store = MagicMock()
        return NWSAdapter(config, mock_config_store, tmp_path / "test.db")

    def test_county_subject(self, adapter: NWSAdapter) -> None:
        event = adapter._normalize_feature(SAMPLE_FEATURE_ID)
        assert event is not None
        subject = adapter.subject_for(event)
        # Primary region should be alphabetically first
        # Could be county or zone depending on sort order
        assert subject.startswith("central.wx.alert.us.id.")

    def test_zone_subject(self, adapter: NWSAdapter) -> None:
        # Create feature with only zone codes
        feature = {
            "id": "urn:test:zone",
            "geometry": None,
            "properties": {
                "event": "Test Alert",
                "sent": "2026-05-15T12:00:00Z",
                "severity": "Minor",
                "geocode": {
                    "SAME": [],
                    "UGC": ["IDZ033"],
                },
            },
        }
        event = adapter._normalize_feature(feature)
        assert event is not None
        subject = adapter.subject_for(event)
        assert "zone" in subject


class TestRegionsSorted:
    """Tests for regions list sorting."""

    @pytest.fixture
    def adapter(self, tmp_path: Path) -> NWSAdapter:
        config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=60,
            settings={
                "contact_email": "test@example.com",
                # No region = accept all features
            },
            updated_at=datetime.now(timezone.utc),
        )
        mock_config_store = MagicMock()
        return NWSAdapter(config, mock_config_store, tmp_path / "test.db")

    def test_regions_alphabetically_sorted(self, adapter: NWSAdapter) -> None:
        event = adapter._normalize_feature(SAMPLE_FEATURE_ID)
        assert event is not None
        assert event.geo.regions == sorted(event.geo.regions)

    def test_primary_region_is_first(self, adapter: NWSAdapter) -> None:
        event = adapter._normalize_feature(SAMPLE_FEATURE_ID)
        assert event is not None
        assert len(event.geo.regions) > 0
        assert event.geo.primary_region == event.geo.regions[0]


class TestDeduplication:
    """Tests for event deduplication."""

    @pytest.fixture
    def adapter(self, tmp_path: Path) -> NWSAdapter:
        config = AdapterConfig(
            name="nws",
            enabled=True,
            cadence_s=60,
            settings={
                "contact_email": "test@example.com",
                # No region = accept all features
            },
            updated_at=datetime.now(timezone.utc),
        )
        mock_config_store = MagicMock()
        return NWSAdapter(config, mock_config_store, tmp_path / "test.db")

    def test_same_feature_same_id(self, adapter: NWSAdapter) -> None:
        """Normalizing the same feature twice returns same Event.id."""
        event1 = adapter._normalize_feature(SAMPLE_FEATURE_ID)
        event2 = adapter._normalize_feature(SAMPLE_FEATURE_ID)
        assert event1 is not None
        assert event2 is not None
        assert event1.id == event2.id

    def test_sweep_only_deletes_own_adapter_rows(
        self, adapter: NWSAdapter, tmp_path: Path
    ) -> None:
        """Regression (v0.9.19.1): sweep_old_ids must be adapter-scoped.

        NWS previously ran an unscoped global DELETE that purged *every*
        adapter's published_ids older than 8 days; the inherited base method
        scopes the delete to ``adapter = ?``.
        """
        adapter._db = sqlite3.connect(tmp_path / "dedup.db")
        adapter._db.execute(
            "CREATE TABLE published_ids (adapter TEXT, event_id TEXT, "
            "first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            "last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            "PRIMARY KEY (adapter, event_id))"
        )
        for adp in ("nws", "eonet"):
            adapter._db.execute(
                "INSERT INTO published_ids (adapter, event_id, last_seen) "
                "VALUES (?, 'old', datetime('now', '-9 days'))",
                (adp,),
            )
        adapter._db.commit()
        assert adapter.dedup_sweep_days == 8
        assert adapter.sweep_old_ids() == 1  # only the nws row
        survivors = {
            r[0] for r in adapter._db.execute("SELECT adapter FROM published_ids")
        }
        assert survivors == {"eonet"}  # foreign adapter's row survives


class TestGeometry:
    """Tests for geometry computation."""

    def test_centroid_polygon(self) -> None:
        geom = {
            "type": "Polygon",
            "coordinates": [[
                [-116.5, 43.5],
                [-116.0, 43.5],
                [-116.0, 44.0],
                [-116.5, 44.0],
                [-116.5, 43.5],
            ]]
        }
        centroid = _compute_centroid(geom)
        assert centroid is not None
        # Average of 5 vertices (including closing point)
        # lon: (-116.5 + -116.0 + -116.0 + -116.5 + -116.5) / 5 = -116.3
        # lat: (43.5 + 43.5 + 44.0 + 44.0 + 43.5) / 5 = 43.7
        assert -116.4 < centroid[0] < -116.2
        assert 43.6 < centroid[1] < 43.8

    def test_bbox_polygon(self) -> None:
        geom = {
            "type": "Polygon",
            "coordinates": [[
                [-116.5, 43.5],
                [-116.0, 43.5],
                [-116.0, 44.0],
                [-116.5, 44.0],
                [-116.5, 43.5],
            ]]
        }
        bbox = _compute_bbox(geom)
        assert bbox is not None
        assert bbox == (-116.5, 43.5, -116.0, 44.0)

    def test_centroid_none_geometry(self) -> None:
        assert _compute_centroid(None) is None

    def test_bbox_none_geometry(self) -> None:
        assert _compute_bbox(None) is None
