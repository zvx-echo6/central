"""Tests for NASA EONET adapter."""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.config_models import AdapterConfig
from central.models import Event, Geo

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "eonet_sample.json"


def _fixture_text() -> str:
    return FIXTURE_PATH.read_text()


def _fixture_json() -> dict:
    return json.loads(_fixture_text())


def _config(settings: dict | None = None) -> AdapterConfig:
    return AdapterConfig(
        name="eonet",
        enabled=True,
        cadence_s=1800,
        settings=settings or {},
        updated_at=datetime.now(timezone.utc),
    )


class TestEONETHelpers:
    def test_camelcase_subject_conversion(self):
        """Verify the camelCase -> lower_snake_case conversion for every default category id.

        Inputs are read from _DEFAULT_CATEGORIES, the single source of truth — no
        per-test hardcoded list of category strings.
        """
        from central.adapters.eonet import _DEFAULT_CATEGORIES, _subject_category

        for cat_id in _DEFAULT_CATEGORIES:
            subj = _subject_category(cat_id)
            assert re.match(r"^[a-z_]+$", subj), f"{cat_id} -> {subj}: must be lower_snake_case"
            # Round-trip: removing underscores from the converted form must yield
            # the lowercased upstream id. Catches both missed and spurious boundaries.
            assert subj.replace("_", "") == cat_id.lower(), f"{cat_id} -> {subj}: round-trip failed"

    def test_empty_category_subject(self):
        from central.adapters.eonet import EONETAdapter, _subject_category

        assert _subject_category(None) == "unknown"
        assert _subject_category("") == "unknown"

        # Through subject_for: a category with no upstream component yields .unknown.unknown
        adapter = EONETAdapter(_config(), MagicMock(), Path("/tmp/never_used.db"))
        event = Event(
            id="X",
            adapter="eonet",
            category="disaster.eonet.unknown",
            time=datetime.now(timezone.utc),
            severity=0,
            geo=Geo(),
            data={},
        )
        assert adapter.subject_for(event).endswith(".unknown.unknown")

    def test_dedup_key_includes_latest_geometry_date(self):
        from central.adapters.eonet import _dedup_key

        date_a = "2026-05-14T11:04:00Z"
        date_b = "2026-05-15T00:00:00Z"
        event_id = "EONET_TEST_1"

        key_a = _dedup_key(event_id, date_a)
        assert date_a in key_a
        assert event_id in key_a

        # Different timeline date -> different dedup key
        assert _dedup_key(event_id, date_b) != key_a


class TestEONETSettings:
    def test_category_allowlist_default_is_full_set(self):
        """The default allowlist equals _DEFAULT_CATEGORIES — no parallel literal anywhere."""
        from central.adapters.eonet import EONETSettings, _DEFAULT_CATEGORIES

        assert EONETSettings().category_allowlist == _DEFAULT_CATEGORIES


class TestEONETAdapter:
    def test_class_attrs_complete(self):
        from central.adapters.eonet import EONETAdapter, EONETSettings

        assert EONETAdapter.name == "eonet"
        assert isinstance(EONETAdapter.display_name, str) and EONETAdapter.display_name
        assert isinstance(EONETAdapter.description, str) and EONETAdapter.description
        assert EONETAdapter.settings_schema is EONETSettings
        assert EONETAdapter.requires_api_key is None
        assert EONETAdapter.api_key_field is None
        assert EONETAdapter.wizard_order is None
        assert EONETAdapter.default_cadence_s == 1800

    @pytest.mark.asyncio
    async def test_geometry_singular_key(self, tmp_path: Path):
        """Adapter reads 'geometry' (singular) per upstream divergence from the spec brief."""
        from central.adapters.eonet import EONETAdapter

        fix = _fixture_json()
        # Sanity-check the fixture itself is shaped per upstream:
        assert all("geometry" in e for e in fix["events"]), "fixture must use 'geometry' (singular)"
        assert all("geometries" not in e for e in fix["events"]), "fixture must not use 'geometries'"

        adapter = EONETAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        # If the adapter were reading 'geometries' instead, centroids would be absent.
        assert any(e.geo.centroid is not None for e in events)

    @pytest.mark.asyncio
    async def test_lonlat_coordinate_order(self, tmp_path: Path):
        """Upstream coordinates [lon, lat] (GeoJSON) map directly to Geo.centroid=(lon, lat)."""
        from central.adapters.eonet import EONETAdapter

        fix = _fixture_json()
        src = next(e for e in fix["events"] if e.get("geometry"))
        lon_in, lat_in = src["geometry"][0]["coordinates"]
        # Sanity-check orientation of fixture datum so the assertion below isn't trivially passing.
        # The first fixture event is in the western/northern hemisphere (Iowa).
        assert lon_in < 0, "fixture event 0 should have western-hemisphere lon"
        assert 0 < lat_in < 90, "fixture event 0 should have northern lat in (0,90)"

        adapter = EONETAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        emitted = next(e for e in events if e.id == src["id"])
        assert emitted.geo.centroid is not None
        out_lon, out_lat = emitted.geo.centroid
        assert out_lon == lon_in, "first centroid element must equal upstream lon (no swap)"
        assert out_lat == lat_in, "second centroid element must equal upstream lat (no swap)"

    @pytest.mark.asyncio
    async def test_country_unknown_when_no_geocoder(self, tmp_path: Path):
        """No geocoder enrichment -> subject ends with '.unknown'."""
        from central.adapters.eonet import EONETAdapter

        adapter = EONETAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert events, "fixture should produce at least one emitted event"
        for e in events:
            assert adapter.subject_for(e).endswith(".unknown"), e.category

    @pytest.mark.asyncio
    async def test_magnitude_value_surfaced(self, tmp_path: Path):
        """magnitudeValue from the most-recent geometry point is surfaced on Event.data."""
        from central.adapters.eonet import EONETAdapter

        adapter = EONETAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        with_mag = [e for e in events if e.data.get("magnitudeValue") is not None]
        assert with_mag, "fixture should contain at least one event with magnitudeValue"
        for e in with_mag:
            assert "magnitudeUnit" in e.data

    @pytest.mark.asyncio
    async def test_category_allowlist_filters(self, tmp_path: Path):
        """Narrowing category_allowlist drops events outside the allowlist."""
        from central.adapters.eonet import EONETAdapter

        fix = _fixture_json()
        # Pick the first fixture event's category as the sole allowed category.
        target = fix["events"][0]["categories"][0]["id"]

        adapter = EONETAdapter(
            _config({"category_allowlist": [target]}),
            MagicMock(),
            tmp_path / "cursors.db",
        )
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert events, "fixture should include at least one event matching the target category"
        for e in events:
            assert e.data["category_id"] == target

    @pytest.mark.asyncio
    async def test_dedup_suppresses_repeat_poll(self, tmp_path: Path):
        """Second poll with identical upstream yields no new events (composite dedup hits)."""
        from central.adapters.eonet import EONETAdapter

        adapter = EONETAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        first_pass = [e async for e in adapter.poll()]
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert first_pass
        assert second_pass == []

    @pytest.mark.asyncio
    async def test_fall_off_emits_removed_subject(self, tmp_path: Path):
        """Event in observed_before but absent from this poll -> removal emitted."""
        from central.adapters.eonet import EONETAdapter, _subject_category

        fix = _fixture_json()
        first_event = fix["events"][0]
        second_fix = {**fix, "events": fix["events"][1:]}

        adapter = EONETAdapter(_config(), MagicMock(), tmp_path / "cursors.db")

        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        first_pass = [e async for e in adapter.poll()]
        assert any(e.id == first_event["id"] for e in first_pass)

        adapter._fetch = AsyncMock(return_value=json.dumps(second_fix))
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        tombstones = [e for e in second_pass if e.category.endswith(".removed")]
        assert len(tombstones) == 1
        ts = tombstones[0]
        assert ts.id == f"{first_event['id']}:removed"
        assert ts.data["reason"] == "missing_from_feed"

        # Subject pattern: subtype BEFORE 'removed' per §8 canonical pattern.
        # Subscriber filtering on central.disaster.eonet.<cat>.> must match the
        # removal subject central.disaster.eonet.<cat>.removed.unknown.
        expected_cat = _subject_category(first_event["categories"][0]["id"])
        subj = adapter.subject_for(ts)
        assert subj.startswith(f"central.disaster.eonet.{expected_cat}.")
        assert ".removed." in subj
        assert subj.endswith(".unknown")
