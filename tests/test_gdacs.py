"""Tests for GDACS adapter."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from central.config_models import AdapterConfig
from central.models import Event


# Frozen RSS fixture mirroring real GDACS shape (namespaces + element layout).
SAMPLE_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:geo="http://www.w3.org/2003/01/geo/wgs84_pos#" xmlns:gdacs="http://www.gdacs.org" xmlns:georss="http://www.georss.org/georss">
  <channel>
    <title>GDACS RSS information</title>
    <link>https://www.gdacs.org/</link>
    <description>Near real-time notification</description>
    <pubDate>Tue, 19 May 2026 06:35:01 GMT</pubDate>
    <item>
      <title>Green wildfire in Greece 18/05/2026 11:00 UTC</title>
      <description>Wildfire in Attica region of Greece.</description>
      <link>https://www.gdacs.org/report.aspx?eventtype=WF&amp;eventid=2002001</link>
      <pubDate>Mon, 18 May 2026 11:10:00 GMT</pubDate>
      <gdacs:iscurrent>true</gdacs:iscurrent>
      <gdacs:fromdate>Mon, 18 May 2026 11:00:00 GMT</gdacs:fromdate>
      <gdacs:datemodified>Tue, 19 May 2026 04:00:00 GMT</gdacs:datemodified>
      <guid isPermaLink="false">WF2002001</guid>
      <geo:Point>
        <geo:lat>38.0</geo:lat>
        <geo:long>23.7</geo:long>
      </geo:Point>
      <gdacs:bbox>21.7 25.7 36.0 40.0</gdacs:bbox>
      <gdacs:eventtype>WF</gdacs:eventtype>
      <gdacs:alertlevel>Green</gdacs:alertlevel>
      <gdacs:alertscore>0</gdacs:alertscore>
      <gdacs:eventid>2002001</gdacs:eventid>
      <gdacs:iso3>GRC</gdacs:iso3>
      <gdacs:country>Greece</gdacs:country>
    </item>
    <item>
      <title>Orange drought in United States of America 15/04/2026</title>
      <description>Multi-state drought.</description>
      <link>https://www.gdacs.org/report.aspx?eventtype=DR&amp;eventid=3003001</link>
      <pubDate>Wed, 15 Apr 2026 00:00:00 GMT</pubDate>
      <gdacs:iscurrent>true</gdacs:iscurrent>
      <gdacs:fromdate>Wed, 15 Apr 2026 00:00:00 GMT</gdacs:fromdate>
      <guid isPermaLink="false">DR3003001</guid>
      <geo:Point>
        <geo:lat>39.5</geo:lat>
        <geo:long>-98.5</geo:long>
      </geo:Point>
      <gdacs:bbox>-110.0 -90.0 32.0 45.0</gdacs:bbox>
      <gdacs:eventtype>DR</gdacs:eventtype>
      <gdacs:alertlevel>Orange</gdacs:alertlevel>
      <gdacs:alertscore>1.5</gdacs:alertscore>
      <gdacs:eventid>3003001</gdacs:eventid>
      <gdacs:iso3>USA</gdacs:iso3>
      <gdacs:country>United States of America</gdacs:country>
    </item>
    <item>
      <title>Green earthquake in Vanuatu</title>
      <description>EQ Vanuatu</description>
      <link>https://www.gdacs.org/report.aspx?eventtype=EQ&amp;eventid=1541360</link>
      <pubDate>Tue, 19 May 2026 02:41:13 GMT</pubDate>
      <gdacs:iscurrent>true</gdacs:iscurrent>
      <gdacs:fromdate>Tue, 19 May 2026 02:29:24 GMT</gdacs:fromdate>
      <guid isPermaLink="false">EQ1541360</guid>
      <geo:Point>
        <geo:lat>-18.15</geo:lat>
        <geo:long>168.09</geo:long>
      </geo:Point>
      <gdacs:eventtype>EQ</gdacs:eventtype>
      <gdacs:alertlevel>Green</gdacs:alertlevel>
      <gdacs:eventid>1541360</gdacs:eventid>
      <gdacs:iso3>VUT</gdacs:iso3>
      <gdacs:country>Vanuatu</gdacs:country>
    </item>
    <item>
      <title>Synthetic unknown eventtype</title>
      <description>XX synthetic test</description>
      <link>https://www.gdacs.org/report.aspx?eventtype=XX&amp;eventid=999999</link>
      <pubDate>Tue, 19 May 2026 00:00:00 GMT</pubDate>
      <gdacs:iscurrent>true</gdacs:iscurrent>
      <gdacs:fromdate>Tue, 19 May 2026 00:00:00 GMT</gdacs:fromdate>
      <guid isPermaLink="false">XX999999</guid>
      <gdacs:eventtype>XX</gdacs:eventtype>
      <gdacs:alertlevel>Green</gdacs:alertlevel>
      <gdacs:eventid>999999</gdacs:eventid>
      <gdacs:country>Nowhere</gdacs:country>
    </item>
  </channel>
</rss>"""


# Same items but WF turned to iscurrent=false (tombstone scenario)
SAMPLE_RSS_WF_RETIRED = SAMPLE_RSS.replace(
    "<gdacs:iscurrent>true</gdacs:iscurrent>\n      <gdacs:fromdate>Mon, 18 May 2026 11:00:00 GMT",
    "<gdacs:iscurrent>false</gdacs:iscurrent>\n      <gdacs:fromdate>Mon, 18 May 2026 11:00:00 GMT",
    1,
)


# Just the DR + EQ + XX items, with WF removed entirely (missing-from-feed scenario)
SAMPLE_RSS_WF_MISSING = SAMPLE_RSS.replace(
    """<item>
      <title>Green wildfire in Greece 18/05/2026 11:00 UTC</title>
      <description>Wildfire in Attica region of Greece.</description>
      <link>https://www.gdacs.org/report.aspx?eventtype=WF&amp;eventid=2002001</link>
      <pubDate>Mon, 18 May 2026 11:10:00 GMT</pubDate>
      <gdacs:iscurrent>true</gdacs:iscurrent>
      <gdacs:fromdate>Mon, 18 May 2026 11:00:00 GMT</gdacs:fromdate>
      <gdacs:datemodified>Tue, 19 May 2026 04:00:00 GMT</gdacs:datemodified>
      <guid isPermaLink="false">WF2002001</guid>
      <geo:Point>
        <geo:lat>38.0</geo:lat>
        <geo:long>23.7</geo:long>
      </geo:Point>
      <gdacs:bbox>21.7 25.7 36.0 40.0</gdacs:bbox>
      <gdacs:eventtype>WF</gdacs:eventtype>
      <gdacs:alertlevel>Green</gdacs:alertlevel>
      <gdacs:alertscore>0</gdacs:alertscore>
      <gdacs:eventid>2002001</gdacs:eventid>
      <gdacs:iso3>GRC</gdacs:iso3>
      <gdacs:country>Greece</gdacs:country>
    </item>
    """,
    "",
    1,
)


def _config(settings: dict | None = None) -> AdapterConfig:
    return AdapterConfig(
        name="gdacs",
        enabled=True,
        cadence_s=600,
        settings=settings or {"event_types": ["WF", "DR", "FL", "VO", "TC"]},
        updated_at=datetime.now(timezone.utc),
    )


class TestGDACSHelpers:
    def test_severity_from_alertlevel_green_orange_red(self):
        from central.adapters.gdacs import severity_from_alertlevel

        assert severity_from_alertlevel("Green") == 1
        assert severity_from_alertlevel("Orange") == 2
        assert severity_from_alertlevel("Red") == 3
        assert severity_from_alertlevel(None) == 0
        assert severity_from_alertlevel("") == 0
        assert severity_from_alertlevel("Unknown") == 0
        # case-insensitive
        assert severity_from_alertlevel("green") == 1
        assert severity_from_alertlevel("RED") == 3

    def test_subject_for_lowercase_country(self):
        from central.adapters.gdacs import subject_for_country

        assert subject_for_country("United States") == "united-states"
        assert subject_for_country("Greece") == "greece"
        assert subject_for_country("Solomon Islands") == "solomon-islands"

    def test_subject_for_unknown_country(self):
        from central.adapters.gdacs import subject_for_country

        assert subject_for_country(None) == "unknown"
        assert subject_for_country("") == "unknown"
        assert subject_for_country("   ") == "unknown"

    def test_subject_for_multi_country_takes_first(self):
        from central.adapters.gdacs import subject_for_country

        assert subject_for_country("Mozambique, Madagascar") == "mozambique"

    def test_parse_gdacs_bbox(self):
        from central.adapters.gdacs import parse_gdacs_bbox

        # GDACS format: lonmin lonmax latmin latmax
        # Geo.bbox: (minLon, minLat, maxLon, maxLat)
        result = parse_gdacs_bbox("21.7 25.7 36.0 40.0")
        assert result == (21.7, 36.0, 25.7, 40.0)
        assert parse_gdacs_bbox(None) is None
        assert parse_gdacs_bbox("") is None
        assert parse_gdacs_bbox("not numbers") is None


class TestGDACSAdapter:
    def test_class_attrs_complete(self):
        from central.adapters.gdacs import GDACSAdapter, GDACSSettings

        assert GDACSAdapter.name == "gdacs"
        assert isinstance(GDACSAdapter.display_name, str) and GDACSAdapter.display_name
        assert isinstance(GDACSAdapter.description, str) and GDACSAdapter.description
        assert GDACSAdapter.settings_schema is GDACSSettings
        assert GDACSAdapter.requires_api_key is None
        assert GDACSAdapter.api_key_field is None
        assert GDACSAdapter.wizard_order is None
        assert GDACSAdapter.default_cadence_s == 600

    @pytest.mark.asyncio
    async def test_normalization_basic_wf(self, tmp_path: Path):
        from central.adapters.gdacs import GDACSAdapter

        adapter = GDACSAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        adapter._fetch = AsyncMock(return_value=SAMPLE_RSS)

        await adapter.startup()
        events: list[Event] = [e async for e in adapter.poll()]
        await adapter.shutdown()

        # WF + DR should yield; EQ + XX filtered.
        assert len(events) == 2
        wf = next(e for e in events if e.data["eventtype"] == "WF")
        assert wf.adapter == "gdacs"
        assert wf.category == "disaster.wf"
        assert wf.id == "WF2002001"
        assert wf.severity == 1  # Green
        assert wf.data["country"] == "Greece"
        assert wf.data["iso3"] == "GRC"
        assert wf.geo.centroid == (23.7, 38.0)
        assert wf.geo.bbox == (21.7, 36.0, 25.7, 40.0)
        assert wf.geo.primary_region == "GRC"
        assert wf.geo.regions == ["GRC"]

        dr = next(e for e in events if e.data["eventtype"] == "DR")
        assert dr.severity == 2  # Orange
        assert dr.category == "disaster.dr"
        assert dr.data["iso3"] == "USA"

    @pytest.mark.asyncio
    async def test_eq_filtered_by_default(self, tmp_path: Path):
        from central.adapters.gdacs import GDACSAdapter

        adapter = GDACSAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        adapter._fetch = AsyncMock(return_value=SAMPLE_RSS)

        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        # No EQ in default allowlist; EQ1541360 must not appear.
        assert all(e.id != "EQ1541360" for e in events)
        assert all(e.data["eventtype"] != "EQ" for e in events)

    @pytest.mark.asyncio
    async def test_unknown_eventtype_filtered(self, tmp_path: Path):
        from central.adapters.gdacs import GDACSAdapter

        adapter = GDACSAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        adapter._fetch = AsyncMock(return_value=SAMPLE_RSS)

        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert all(e.data["eventtype"] != "XX" for e in events)

    @pytest.mark.asyncio
    async def test_settings_event_types_override(self, tmp_path: Path):
        from central.adapters.gdacs import GDACSAdapter

        adapter = GDACSAdapter(
            _config({"event_types": ["EQ"]}),
            MagicMock(),
            tmp_path / "cursors.db",
        )
        adapter._fetch = AsyncMock(return_value=SAMPLE_RSS)

        await adapter.startup()
        events = [e async for e in adapter.poll()]
        await adapter.shutdown()

        # Only EQ should yield now.
        assert len(events) == 1
        assert events[0].id == "EQ1541360"
        assert events[0].category == "disaster.eq"

    @pytest.mark.asyncio
    async def test_dedup_by_guid(self, tmp_path: Path):
        from central.adapters.gdacs import GDACSAdapter

        adapter = GDACSAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        adapter._fetch = AsyncMock(return_value=SAMPLE_RSS)

        await adapter.startup()
        first_pass = [e async for e in adapter.poll()]
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        assert len(first_pass) == 2
        assert len(second_pass) == 0

    @pytest.mark.asyncio
    async def test_fall_off_iscurrent_false(self, tmp_path: Path):
        """Item seen iscurrent=true then iscurrent=false -> tombstone."""
        from central.adapters.gdacs import GDACSAdapter

        adapter = GDACSAdapter(_config(), MagicMock(), tmp_path / "cursors.db")

        adapter._fetch = AsyncMock(return_value=SAMPLE_RSS)
        await adapter.startup()
        first_pass = [e async for e in adapter.poll()]
        assert any(e.id == "WF2002001" for e in first_pass)

        adapter._fetch = AsyncMock(return_value=SAMPLE_RSS_WF_RETIRED)
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        tombstones = [e for e in second_pass if e.category.endswith(".removed")]
        assert len(tombstones) == 1
        ts = tombstones[0]
        assert ts.id == "WF2002001:removed"
        assert ts.category == "disaster.wf.removed"
        assert ts.data["reason"] == "iscurrent_false"
        # Subject form: central.disaster.<eventtype>.removed.<country>
        assert adapter.subject_for(ts) == "central.disaster.wf.removed.greece"

    @pytest.mark.asyncio
    async def test_fall_off_missing_from_feed(self, tmp_path: Path):
        """Item seen, then completely missing from feed -> tombstone."""
        from central.adapters.gdacs import GDACSAdapter

        adapter = GDACSAdapter(_config(), MagicMock(), tmp_path / "cursors.db")

        adapter._fetch = AsyncMock(return_value=SAMPLE_RSS)
        await adapter.startup()
        _ = [e async for e in adapter.poll()]

        adapter._fetch = AsyncMock(return_value=SAMPLE_RSS_WF_MISSING)
        second_pass = [e async for e in adapter.poll()]
        await adapter.shutdown()

        tombstones = [e for e in second_pass if e.category.endswith(".removed")]
        assert len(tombstones) == 1
        assert tombstones[0].id == "WF2002001:removed"
        assert tombstones[0].category == "disaster.wf.removed"
        assert tombstones[0].data["reason"] == "missing_from_feed"

    @pytest.mark.asyncio
    async def test_subject_for_returns_country_path(self, tmp_path: Path):
        from central.adapters.gdacs import GDACSAdapter
        from central.models import Geo

        adapter = GDACSAdapter(_config(), MagicMock(), tmp_path / "cursors.db")
        event = Event(
            id="WF2002001",
            adapter="gdacs",
            category="disaster.wf",
            time=datetime(2026, 5, 18, 11, tzinfo=timezone.utc),
            severity=1,
            geo=Geo(),
            data={"eventtype": "WF", "country": "Greece"},
        )
        assert adapter.subject_for(event) == "central.disaster.wf.greece"

        event_unknown = Event(
            id="DR1",
            adapter="gdacs",
            category="disaster.dr",
            time=datetime(2026, 5, 18, tzinfo=timezone.utc),
            severity=0,
            geo=Geo(),
            data={"eventtype": "DR", "country": None},
        )
        assert adapter.subject_for(event_unknown) == "central.disaster.dr.unknown"
