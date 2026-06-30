"""Unit tests for GenericHttpAdapter.

Pure unit tests: no database mocking required (we use a real sqlite tmp file
for dedup tests, same as test_usgs_quake.py), and HTTP is patched.
"""

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError

from central.adapters.generic_http import (
    FieldMapping,
    GenericHttpAdapter,
    GenericHttpSettings,
    _dig,
)
from central.config_models import AdapterConfig
from central.models import Event, Geo
from central.streams import STREAMS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_domain() -> str:
    """Return a known valid domain (first event-bearing stream)."""
    for s in STREAMS:
        domain = s.subject_filter.split(".")[1]
        if domain != "meta":
            return domain
    raise RuntimeError("No non-meta stream found in STREAMS")


def make_config(
    name: str = "test_generic",
    domain: str | None = None,
    extra_settings: dict | None = None,
) -> AdapterConfig:
    domain = domain or _valid_domain()
    settings: dict = {
        "url": "https://example.com/feed.json",
        "domain": domain,
        "id_path": "id",
        "items_path": "features",
        "geometry_path": "geometry",
    }
    if extra_settings:
        settings.update(extra_settings)
    return AdapterConfig(
        name=name,
        kind="generic_http",
        enabled=True,
        cadence_s=300,
        settings=settings,
        updated_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def temp_db_path():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        yield Path(f.name)


@pytest.fixture
def mock_config_store():
    return MagicMock()


# ---------------------------------------------------------------------------
# _dig
# ---------------------------------------------------------------------------

class TestDig:
    def test_simple_key(self):
        assert _dig({"a": 1}, "a") == 1

    def test_nested_keys(self):
        assert _dig({"a": {"b": {"c": 42}}}, "a.b.c") == 42

    def test_list_index(self):
        assert _dig({"a": [10, 20, 30]}, "a.1") == 20

    def test_list_index_zero(self):
        assert _dig({"items": [{"x": 99}]}, "items.0.x") == 99

    def test_missing_key_returns_none(self):
        assert _dig({"a": 1}, "b") is None

    def test_missing_nested_returns_none(self):
        assert _dig({"a": {"b": 1}}, "a.c") is None

    def test_out_of_range_index_returns_none(self):
        assert _dig({"a": [1, 2]}, "a.5") is None

    def test_none_root_returns_none(self):
        assert _dig(None, "a.b") is None

    def test_non_dict_mid_path_returns_none(self):
        assert _dig({"a": 42}, "a.b") is None

    def test_empty_list(self):
        assert _dig([], "0") is None


# ---------------------------------------------------------------------------
# Domain validation
# ---------------------------------------------------------------------------

class TestDomainValidation:
    def test_unknown_domain_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            GenericHttpSettings(
                url="https://example.com/feed",
                domain="totally_unknown_domain_xyz",
                id_path="id",
            )
        err_str = str(exc_info.value)
        assert "unknown domain" in err_str
        # Should list valid options in the error message
        assert "wx" in err_str or "fire" in err_str

    def test_known_domain_ok(self):
        s = GenericHttpSettings(
            url="https://example.com/feed",
            domain=_valid_domain(),
            id_path="id",
        )
        assert s.domain == _valid_domain()

    def test_wx_domain_valid(self):
        s = GenericHttpSettings(
            url="https://example.com/feed",
            domain="wx",
            id_path="id",
        )
        assert s.domain == "wx"

    def test_fire_domain_valid(self):
        s = GenericHttpSettings(
            url="https://example.com/feed",
            domain="fire",
            id_path="id",
        )
        assert s.domain == "fire"


# ---------------------------------------------------------------------------
# Category composition
# ---------------------------------------------------------------------------

class TestCategoryComposition:
    @pytest.mark.asyncio
    async def test_category_default_suffix(self, temp_db_path, mock_config_store):
        config = make_config(domain="wx")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        geojson = {
            "features": [
                {
                    "id": "item-1",
                    "geometry": {"type": "Point", "coordinates": [-116.0, 43.0]},
                }
            ]
        }
        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = geojson
            events = [e async for e in adapter.poll()]

        assert len(events) == 1
        assert events[0].category == "wx.alert"

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_category_custom_suffix(self, temp_db_path, mock_config_store):
        config = make_config(domain="wx", extra_settings={"category_suffix": "warning"})
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        geojson = {
            "features": [
                {
                    "id": "item-wx-warn",
                    "geometry": {"type": "Point", "coordinates": [-116.0, 43.0]},
                }
            ]
        }
        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = geojson
            events = [e async for e in adapter.poll()]

        assert events[0].category == "wx.warning"
        await adapter.shutdown()


# ---------------------------------------------------------------------------
# GeoJSON path — geometry + centroid
# ---------------------------------------------------------------------------

GEOJSON_FIXTURE = {
    "type": "FeatureCollection",
    "features": [
        {
            "id": "feat-001",
            "type": "Feature",
            "properties": {
                "title": "Test Alert",
                "severity": 2,
                "updated": "2025-01-15T12:00:00Z",
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-116.2, 43.7],
            },
        },
        {
            "id": "feat-002",
            "type": "Feature",
            "properties": {
                "title": "Polygon Alert",
                "severity": 3,
                "updated": "2025-01-15T13:00:00Z",
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-116, 43], [-115, 43], [-115, 44], [-116, 44], [-116, 43]]],
            },
        },
    ],
}


class TestGeoJsonPath:
    @pytest.mark.asyncio
    async def test_point_geometry_and_centroid(self, temp_db_path, mock_config_store):
        config = make_config(
            domain="wx",
            extra_settings={
                "items_path": "features",
                "id_path": "id",
                "geometry_path": "geometry",
                "title_path": "properties.title",
                "severity_path": "properties.severity",
                "field_mappings": [
                    {"source_path": "properties.title", "dest_key": "title"},
                ],
            },
        )
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = GEOJSON_FIXTURE
            events = [e async for e in adapter.poll()]

        assert len(events) == 2

        point_event = next(e for e in events if e.id == "feat-001")
        # geo.geometry set
        assert point_event.geo.geometry == {
            "type": "Point",
            "coordinates": [-116.2, 43.7],
        }
        # centroid extracted from Point
        assert point_event.geo.centroid == (-116.2, 43.7)

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_polygon_geometry_no_centroid(self, temp_db_path, mock_config_store):
        config = make_config(domain="wx")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = GEOJSON_FIXTURE
            events = [e async for e in adapter.poll()]

        poly_event = next(e for e in events if e.id == "feat-002")
        # geometry is set
        assert poly_event.geo.geometry is not None
        assert poly_event.geo.geometry["type"] == "Polygon"
        # centroid is NOT set (non-Point geometry)
        assert poly_event.geo.centroid is None

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_field_mappings_populate_data(self, temp_db_path, mock_config_store):
        config = make_config(
            domain="wx",
            extra_settings={
                "id_path": "id",
                "title_path": "properties.title",
                "field_mappings": [
                    {"source_path": "properties.severity", "dest_key": "level"},
                    {"source_path": "properties.updated", "dest_key": "updated_at"},
                ],
            },
        )
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = GEOJSON_FIXTURE
            events = [e async for e in adapter.poll()]

        e = next(ev for ev in events if ev.id == "feat-001")
        assert e.data["title"] == "Test Alert"
        assert e.data["level"] == 2
        assert e.data["updated_at"] == "2025-01-15T12:00:00Z"

        await adapter.shutdown()


# ---------------------------------------------------------------------------
# JSON (non-GeoJSON) path — lat_path / lon_path
# ---------------------------------------------------------------------------

JSON_FIXTURE = {
    "alerts": [
        {"uid": "a1", "name": "Alert One", "lat": 43.5, "lon": -116.1, "sev": 1},
        {"uid": "a2", "name": "Alert Two", "lat": 44.0, "lon": -115.5, "sev": 3},
    ]
}


class TestJsonLatLonPath:
    @pytest.mark.asyncio
    async def test_lat_lon_centroid(self, temp_db_path, mock_config_store):
        config = make_config(
            domain="fire",
            extra_settings={
                "items_path": "alerts",
                "id_path": "uid",
                "geometry_path": "",          # disable geometry_path extraction
                "lat_path": "lat",
                "lon_path": "lon",
                "title_path": "name",
                "severity_path": "sev",
            },
        )
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = JSON_FIXTURE
            events = [e async for e in adapter.poll()]

        assert len(events) == 2
        e1 = next(e for e in events if e.id == "a1")
        # centroid is (lon, lat) per GeoJSON convention
        assert e1.geo.centroid == (-116.1, 43.5)
        assert e1.data["title"] == "Alert One"
        assert e1.severity == 1

        await adapter.shutdown()


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

class TestDedup:
    @pytest.mark.asyncio
    async def test_second_poll_yields_nothing(self, temp_db_path, mock_config_store):
        config = make_config(domain="wx")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        geojson = {
            "features": [
                {"id": "dedup-1", "geometry": None},
                {"id": "dedup-2", "geometry": None},
            ]
        }
        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = geojson

            first = [e async for e in adapter.poll()]
            second = [e async for e in adapter.poll()]

        assert len(first) == 2
        assert len(second) == 0

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_new_item_in_second_poll_is_yielded(
        self, temp_db_path, mock_config_store
    ):
        config = make_config(domain="wx")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        first_batch = {"features": [{"id": "old-1", "geometry": None}]}
        second_batch = {
            "features": [
                {"id": "old-1", "geometry": None},
                {"id": "new-2", "geometry": None},
            ]
        }

        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = first_batch
            [e async for e in adapter.poll()]  # consume first

            mf.return_value = second_batch
            second = [e async for e in adapter.poll()]

        assert len(second) == 1
        assert second[0].id == "new-2"

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_instance_name_used_for_dedup(
        self, temp_db_path, mock_config_store
    ):
        """Two adapter instances share the same db but scope by instance name."""
        config_a = make_config(name="instance_a", domain="wx")
        config_b = make_config(name="instance_b", domain="fire")

        adapter_a = GenericHttpAdapter(config_a, mock_config_store, temp_db_path)
        adapter_b = GenericHttpAdapter(config_b, mock_config_store, temp_db_path)
        await adapter_a.startup()
        await adapter_b.startup()

        # Publish "shared-id" under instance_a
        adapter_a.mark_published("shared-id")

        # same id is NOT published under instance_b
        assert not adapter_b.is_published("shared-id")
        # and IS published under instance_a
        assert adapter_a.is_published("shared-id")

        await adapter_a.shutdown()
        await adapter_b.shutdown()


# ---------------------------------------------------------------------------
# subject_for
# ---------------------------------------------------------------------------

class TestSubjectFor:
    @pytest.mark.asyncio
    async def test_subject_no_enrichment(self, temp_db_path, mock_config_store):
        """Without enrichment data, subject should end in 'unknown'."""
        config = make_config(domain="wx")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        event = Event(
            id="subj-1",
            adapter="test_generic",
            category="wx.alert",
            time=datetime.now(timezone.utc),
            geo=Geo(centroid=(-116.0, 43.0)),
            data={},
        )
        subject = adapter.subject_for(event)
        assert subject == "central.wx.unknown"

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_subject_with_us_enrichment(self, temp_db_path, mock_config_store):
        """With US geocoder enrichment, subject should be central.<domain>.us.<state>."""
        config = make_config(domain="fire")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        event = Event(
            id="subj-2",
            adapter="test_generic",
            category="fire.alert",
            time=datetime.now(timezone.utc),
            geo=Geo(centroid=(-116.0, 43.0)),
            data={
                "_enriched": {
                    "geocoder": {
                        "country": "United States",
                        "state": "Idaho",
                    }
                }
            },
        )
        subject = adapter.subject_for(event)
        assert subject == "central.fire.us.id"

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_subject_format_domain_region(self, temp_db_path, mock_config_store):
        """Subject always matches central.<domain>.<region> — NOT central.<category>.<region>."""
        config = make_config(domain="quake", extra_settings={"category_suffix": "event.minor"})
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        event = Event(
            id="subj-3",
            adapter="test_generic",
            category="quake.event.minor",
            time=datetime.now(timezone.utc),
            geo=Geo(),
            data={},
        )
        subject = adapter.subject_for(event)
        # Should be central.quake.unknown, NOT central.quake.event.minor.unknown
        assert subject == "central.quake.unknown"
        assert subject.startswith("central.quake.")

        await adapter.shutdown()


# ---------------------------------------------------------------------------
# time_path parsing
# ---------------------------------------------------------------------------

class TestTimePath:
    @pytest.mark.asyncio
    async def test_time_path_none_uses_now(self, temp_db_path, mock_config_store):
        before = datetime.now(timezone.utc)
        config = make_config(domain="wx", extra_settings={"time_path": None})
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        geojson = {"features": [{"id": "t1", "geometry": None}]}
        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = geojson
            events = [e async for e in adapter.poll()]

        after = datetime.now(timezone.utc)
        assert len(events) == 1
        assert before <= events[0].time <= after

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_time_path_parsed_iso(self, temp_db_path, mock_config_store):
        config = make_config(
            domain="wx",
            extra_settings={
                "time_path": "properties.updated",
                "id_path": "id",
                "items_path": "features",
            },
        )
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        geojson = {
            "features": [
                {
                    "id": "t2",
                    "geometry": None,
                    "properties": {"updated": "2025-06-15T08:30:00Z"},
                }
            ]
        }
        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = geojson
            events = [e async for e in adapter.poll()]

        assert len(events) == 1
        assert events[0].time == datetime(2025, 6, 15, 8, 30, 0, tzinfo=timezone.utc)

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_time_path_missing_value_uses_now(
        self, temp_db_path, mock_config_store
    ):
        """When time_path is set but the value is absent, fall back to now."""
        before = datetime.now(timezone.utc)
        config = make_config(
            domain="wx",
            extra_settings={"time_path": "properties.ts"},
        )
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        geojson = {"features": [{"id": "t3", "geometry": None, "properties": {}}]}
        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = geojson
            events = [e async for e in adapter.poll()]

        after = datetime.now(timezone.utc)
        assert len(events) == 1
        assert before <= events[0].time <= after

        await adapter.shutdown()


# ---------------------------------------------------------------------------
# apply_config
# ---------------------------------------------------------------------------

class TestApplyConfig:
    @pytest.mark.asyncio
    async def test_apply_config_updates_url(self, temp_db_path, mock_config_store):
        config = make_config(domain="wx")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        assert adapter._settings.url == "https://example.com/feed.json"

        new_config = make_config(
            domain="wx", extra_settings={"url": "https://other.example.com/data.json"}
        )
        await adapter.apply_config(new_config)

        assert adapter._settings.url == "https://other.example.com/data.json"

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_apply_config_updates_domain(self, temp_db_path, mock_config_store):
        config = make_config(domain="wx")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        new_config = make_config(domain="fire")
        await adapter.apply_config(new_config)

        assert adapter._settings.domain == "fire"

        await adapter.shutdown()


# ---------------------------------------------------------------------------
# Instance name scoping
# ---------------------------------------------------------------------------

class TestInstanceName:
    def test_instance_name_is_config_name(self, temp_db_path, mock_config_store):
        """adapter.name must be the instance name, not the class name."""
        config = make_config(name="my_noaa_alerts", domain="wx")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        assert adapter.name == "my_noaa_alerts"
        assert GenericHttpAdapter.name == "generic_http"

    @pytest.mark.asyncio
    async def test_event_adapter_field_is_instance_name(
        self, temp_db_path, mock_config_store
    ):
        config = make_config(name="my_source", domain="wx")
        adapter = GenericHttpAdapter(config, mock_config_store, temp_db_path)
        await adapter.startup()

        geojson = {"features": [{"id": "inst-1", "geometry": None}]}
        with patch.object(adapter, "_fetch", new_callable=AsyncMock) as mf:
            mf.return_value = geojson
            events = [e async for e in adapter.poll()]

        assert len(events) == 1
        assert events[0].adapter == "my_source"

        await adapter.shutdown()
