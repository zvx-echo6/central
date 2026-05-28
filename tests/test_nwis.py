"""Tests for USGS NWIS adapter (OGC API)."""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.config_models import AdapterConfig
from central.models import Event, Geo

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "nwis_latest_sample.json"


def _fixture_text() -> str:
    return FIXTURE_PATH.read_text()


def _fixture_json() -> dict:
    return json.loads(_fixture_text())


def _config(settings: dict | None = None) -> AdapterConfig:
    return AdapterConfig(
        name="nwis",
        enabled=True,
        cadence_s=900,
        settings=settings or {},
        updated_at=datetime.now(timezone.utc),
    )


class TestNWISHelpers:
    def test_subject_decomposes_usgs(self):
        """USGS-05420500 -> agency='usgs', bare='05420500'; subject endswith .00060.usgs.05420500."""
        from central.adapters.nwis import NWISAdapter, _subject_tokens_for_id

        agency, bare = _subject_tokens_for_id("USGS-05420500")
        assert agency == "usgs"
        assert bare == "05420500"

        adapter = NWISAdapter(_config(), MagicMock(), Path("/tmp/never_used.db"))
        event = Event(
            id="USGS-05420500:00060:2026-05-19T15:30:00+00:00",
            adapter="nwis",
            category=f"hydro.00060.{agency}.{bare}",
            time=datetime(2026, 5, 19, 15, 30, tzinfo=timezone.utc),
            severity=0,
            geo=Geo(),
            data={},
        )
        assert adapter.subject_for(event).endswith(".00060.usgs.05420500.unknown")

    def test_subject_decomposes_non_usgs(self):
        """MO005-400105093591601 -> agency='mo005', bare='400105093591601'; subject reflects both."""
        from central.adapters.nwis import NWISAdapter, _subject_tokens_for_id

        agency, bare = _subject_tokens_for_id("MO005-400105093591601")
        assert agency == "mo005"
        assert bare == "400105093591601"

        adapter = NWISAdapter(_config(), MagicMock(), Path("/tmp/never_used.db"))
        event = Event(
            id="MO005-400105093591601:00060:t",
            adapter="nwis",
            category=f"hydro.00060.{agency}.{bare}",
            time=datetime.now(timezone.utc),
            severity=0,
            geo=Geo(),
            data={},
        )
        assert adapter.subject_for(event).endswith(".00060.mo005.400105093591601.unknown")

    def test_subject_unprefixed_id_falls_back(self):
        """ID with no dash falls back to agency='unknown'."""
        from central.adapters.nwis import NWISAdapter, _subject_tokens_for_id

        bare_input = "STANDALONE12345"
        agency, bare = _subject_tokens_for_id(bare_input)
        assert agency == "unknown"
        assert bare == bare_input.lower()

        adapter = NWISAdapter(_config(), MagicMock(), Path("/tmp/never_used.db"))
        event = Event(
            id="X",
            adapter="nwis",
            category=f"hydro.00060.{agency}.{bare}",
            time=datetime.now(timezone.utc),
            severity=0,
            geo=Geo(),
            data={},
        )
        subj = adapter.subject_for(event)
        assert subj.endswith(f".00060.unknown.{bare}.unknown")

    def test_dedup_key_composite(self):
        """Same id+param+time -> same key; different time -> different key."""
        from central.adapters.nwis import _dedup_key

        a = _dedup_key("USGS-05420500", "00060", "2026-05-19T15:30:00+00:00")
        b = _dedup_key("USGS-05420500", "00060", "2026-05-19T15:30:00+00:00")
        c = _dedup_key("USGS-05420500", "00060", "2026-05-19T15:45:00+00:00")
        assert a == b
        assert a != c
        # All three components are present in the key
        assert "USGS-05420500" in a
        assert "00060" in a
        assert "2026-05-19T15:30:00+00:00" in a


class TestNWISSettings:
    def test_parameter_codes_default_is_full_set(self):
        """Default equals _DEFAULT_PARAMETER_CODES — no parallel literal anywhere."""
        from central.adapters.nwis import NWISSettings, _DEFAULT_PARAMETER_CODES

        assert NWISSettings().parameter_codes == _DEFAULT_PARAMETER_CODES


class TestNWISAdapter:
    def test_class_attrs_complete(self):
        from central.adapters.nwis import NWISAdapter, NWISSettings

        assert NWISAdapter.name == "nwis"
        assert isinstance(NWISAdapter.display_name, str) and NWISAdapter.display_name
        assert isinstance(NWISAdapter.description, str) and NWISAdapter.description
        assert NWISAdapter.settings_schema is NWISSettings
        assert NWISAdapter.requires_api_key is None
        assert NWISAdapter.api_key_field is None
        assert NWISAdapter.wizard_order is None
        assert NWISAdapter.default_cadence_s == 900

    @pytest.mark.asyncio
    async def test_lonlat_coordinate_order(self, tmp_path: Path):
        """Upstream coords [lon, lat] -> Geo.centroid=(lon, lat); no axis swap."""
        from central.adapters.nwis import NWISAdapter

        fix = _fixture_json()
        src = fix["features"][0]
        lon_in, lat_in = src["geometry"]["coordinates"]
        # Sanity: fixture event 0 is in western/northern hemisphere.
        assert lon_in < 0
        assert 0 < lat_in < 90

        adapter = NWISAdapter(
            _config({"parameter_codes": [src["properties"]["parameter_code"]]}),
            MagicMock(),
            tmp_path / "cursors.db",
        )
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        target_site = src["properties"]["monitoring_location_id"]
        emitted = next(e for e in events if e.data["monitoring_location_id"] == target_site)
        assert emitted.geo.centroid == (lon_in, lat_in)

    @pytest.mark.asyncio
    async def test_parameter_allowlist_filters(self, tmp_path: Path):
        """parameter_codes setting filters which queries we issue per poll.

        With one allowed code, _fetch should be called exactly once
        (per page; fixture has no next link → one call total).
        """
        from central.adapters.nwis import NWISAdapter

        fix = _fixture_json()
        target = fix["features"][0]["properties"]["parameter_code"]

        adapter = NWISAdapter(
            _config({"parameter_codes": [target]}),
            MagicMock(),
            tmp_path / "cursors.db",
        )
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        # Isolate polling fetches from v0.8.0 per-event site/stats enrichment
        # (which also calls _fetch); enrichment is covered in test_nwis_enrichment.
        adapter._enrich_event = AsyncMock(return_value=None)
        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert events
        # All emitted events carry the only allowed parameter_code.
        for e in events:
            assert e.data["parameter_code"] == target
        # Per-page fetch invoked once per parameter_code in the allowlist.
        assert adapter._fetch.await_count == 1

    @pytest.mark.asyncio
    async def test_pagination_follows_next_link(self, tmp_path: Path):
        """Adapter follows OGC 'next' link until absent."""
        from central.adapters.nwis import NWISAdapter

        fix = _fixture_json()
        target = fix["features"][0]["properties"]["parameter_code"]

        page1 = {
            "type": "FeatureCollection",
            "features": fix["features"][:2],
            "numberReturned": 2,
            "links": [
                {"rel": "next", "href": "https://api.waterdata.usgs.gov/ogcapi/v0/collections/latest-continuous/items?cursor=PAGE2"}
            ],
        }
        page2 = {
            "type": "FeatureCollection",
            "features": fix["features"][2:],
            "numberReturned": len(fix["features"]) - 2,
            "links": [],
        }

        adapter = NWISAdapter(
            _config({"parameter_codes": [target]}),
            MagicMock(),
            tmp_path / "cursors.db",
        )
        adapter._fetch = AsyncMock(side_effect=[json.dumps(page1), json.dumps(page2)])
        # Isolate polling fetches from v0.8.0 per-event site/stats enrichment.
        adapter._enrich_event = AsyncMock(return_value=None)
        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        # Both pages drained, fetch called exactly twice.
        assert adapter._fetch.await_count == 2
        # Events from both pages emitted.
        sites = {e.data["monitoring_location_id"] for e in events}
        expected_sites = {f["properties"]["monitoring_location_id"] for f in fix["features"]}
        assert sites == expected_sites

    @pytest.mark.asyncio
    async def test_no_removal_pattern(self, tmp_path: Path):
        """Sites missing from a later poll do NOT emit a .removed event in v1."""
        from central.adapters.nwis import NWISAdapter

        fix = _fixture_json()
        target = fix["features"][0]["properties"]["parameter_code"]

        adapter = NWISAdapter(
            _config({"parameter_codes": [target]}),
            MagicMock(),
            tmp_path / "cursors.db",
        )
        # First poll: full fixture.
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        first_pass = [e async for e in adapter.poll()]
        assert first_pass

        # Second poll: empty result (every previously-seen site is now missing).
        empty_page = {"type": "FeatureCollection", "features": [], "links": []}
        adapter._fetch = AsyncMock(return_value=json.dumps(empty_page))
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        # No removal events of any flavor.
        assert all(".removed" not in e.category for e in second_pass)
        assert second_pass == []

    @pytest.mark.asyncio
    async def test_dedup_suppresses_repeat_poll(self, tmp_path: Path):
        """Second poll with identical fixture yields no new events (composite dedup hits)."""
        from central.adapters.nwis import NWISAdapter

        fix = _fixture_json()
        target = fix["features"][0]["properties"]["parameter_code"]

        adapter = NWISAdapter(
            _config({"parameter_codes": [target]}),
            MagicMock(),
            tmp_path / "cursors.db",
        )
        adapter._fetch = AsyncMock(return_value=_fixture_text())
        await adapter.startup()
        first_pass = [e async for e in adapter.poll()]
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert first_pass
        assert second_pass == []


class TestNWISPreview:
    """Preview hook (PR G.5) — exercised without starting the adapter."""

    @pytest.mark.asyncio
    async def test_preview_returns_none_without_region(self, tmp_path: Path):
        from central.adapters.nwis import NWISAdapter, NWISSettings

        adapter = NWISAdapter(_config({}), MagicMock(), tmp_path / "cursors.db")
        result = await adapter.preview_for_settings(NWISSettings())
        assert result is None

    @pytest.mark.asyncio
    async def test_preview_returns_rows_with_region(self, tmp_path: Path):
        """Given a bbox, preview returns one dict per monitoring-locations feature
        with the contract column order: site_id, name, site_type, state."""
        from central.adapters.nwis import NWISAdapter, NWISSettings

        sample_response = json.dumps({
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "USGS-05420500",
                    "geometry": {"type": "Point", "coordinates": [-90.25, 41.78]},
                    "properties": {
                        "monitoring_location_name": "MISSISSIPPI RIVER AT CLINTON, IA",
                        "site_type_code": "ST",
                        "state_name": "Iowa",
                    },
                },
                {
                    "type": "Feature",
                    "id": "USGS-05454500",
                    "geometry": {"type": "Point", "coordinates": [-91.65, 41.66]},
                    "properties": {
                        "monitoring_location_name": "IOWA RIVER AT IOWA CITY, IA",
                        "site_type_code": "ST",
                        "state_name": "Iowa",
                    },
                },
            ],
        })

        adapter = NWISAdapter(
            _config({"region": {"west": -94.0, "south": 40.0, "east": -93.0, "north": 41.0}}),
            MagicMock(),
            tmp_path / "cursors.db",
        )
        adapter._fetch_preview_text = AsyncMock(return_value=sample_response)
        settings = NWISSettings(region=adapter.region)
        rows = await adapter.preview_for_settings(settings)

        assert rows is not None
        assert len(rows) == 2
        # Column order is part of the contract — first row's keys must match exactly.
        expected_keys = ["site_id", "name", "site_type", "state"]
        assert list(rows[0].keys()) == expected_keys
        # Row content reflects fixture data.
        assert rows[0]["site_id"] == "USGS-05420500"
        assert rows[0]["name"] == "MISSISSIPPI RIVER AT CLINTON, IA"
        assert rows[0]["site_type"] == "ST"
        assert rows[0]["state"] == "Iowa"
        assert rows[1]["site_id"] == "USGS-05454500"

    @pytest.mark.asyncio
    async def test_preview_propagates_http_error(self, tmp_path: Path):
        """Preview must not swallow upstream errors — the framework needs them
        to render the operator-visible 'Preview unavailable: …' banner."""
        from central.adapters.nwis import NWISAdapter, NWISSettings

        adapter = NWISAdapter(
            _config({"region": {"west": -94.0, "south": 40.0, "east": -93.0, "north": 41.0}}),
            MagicMock(),
            tmp_path / "cursors.db",
        )
        adapter._fetch_preview_text = AsyncMock(side_effect=RuntimeError("upstream 503"))
        settings = NWISSettings(region=adapter.region)
        with pytest.raises(RuntimeError, match="upstream 503"):
            await adapter.preview_for_settings(settings)
