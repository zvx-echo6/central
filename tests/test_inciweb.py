"""Tests for InciWeb adapter."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.config_models import AdapterConfig
from central.models import Event, Geo


# Real RSS snippet from InciWeb (frozen fixture)
SAMPLE_RSS_CONTENT = """<?xml version="1.0" encoding="utf-8"?>
<rss xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0" xml:base="http://inciweb.wildfire.gov/">
  <channel>
    <title>InciWeb</title>
    <link>http://inciweb.wildfire.gov/</link>
    <description>Inciweb Fire Incidents</description>
    <language>en</language>
    <item>
      <title>MNMNS Stewart Trail</title>
      <link>http://inciweb.wildfire.gov/incident-information/mnmns-stewart-trail</link>
      <description>Last updated: 2026-05-18

---

The type of incident is Wildfire and involves the following unit(s) Minnesota Department of Natural Resources.

---

State: Minnesota

---

Coordinates:

Latitude: 47° 3 17  Longitude: 91° 38 6

---

NOTE: All fire perimeters and points are approximations.

---

Incident Overview: The Stewart Trail Fire was detected during the afternoon hours on Friday, May 15, 2026.&amp;nbsp;A temporary flight restriction (TFR) is in place.</description>
      <pubDate>Fri, 15 May 2026 08:48:11 EDT</pubDate>
      <dc:creator>llangeberg</dc:creator>
      <guid isPermaLink="false">327828</guid>
    </item>
    <item>
      <title>CACNP Santa Rosa Island Fire</title>
      <link>http://inciweb.wildfire.gov/incident-information/cacnp-santa-rosa-island-fire</link>
      <description>Last updated: 2026-05-18

---

The type of incident is Wildfire and involves the following unit(s) Channel Islands National Park.

---

State: California

---

Coordinates:

Latitude: 33° 55 2  Longitude: 120° 5 10

---

NOTE: All fire perimeters and points are approximations.

---

Incident Overview: On Friday, May 15, 2026, an aircraft flying over Santa Rosa Island in Channel Islands National Park reported a wildfire.&lt;br&gt;&lt;p&gt;This is a &lt;strong&gt;full-suppression&lt;/strong&gt; human-caused wildfire and is under investigation.&lt;/p&gt;&amp;nbsp;</description>
      <pubDate>Sat, 16 May 2026 12:09:07 EDT</pubDate>
      <dc:creator>mtheune</dc:creator>
      <guid isPermaLink="false">327838</guid>
    </item>
    <item>
      <title>Some Fire Without Coordinates</title>
      <link>http://inciweb.wildfire.gov/incident-information/no-coords-fire</link>
      <description>Last updated: 2026-05-18

---

The type of incident is Wildfire.

---

State: Unknown State

---

Incident Overview: This is a test incident without coordinates.</description>
      <pubDate>Mon, 18 May 2026 09:00:00 EDT</pubDate>
      <dc:creator>test</dc:creator>
      <guid isPermaLink="false">999999</guid>
    </item>
    <item>
      <title>Florida Fire Outside Bbox</title>
      <link>http://inciweb.wildfire.gov/incident-information/florida-fire</link>
      <description>Last updated: 2026-05-18

---

State: Florida

---

Coordinates:

Latitude: 26° 0 0  Longitude: 80° 0 0

---

Incident Overview: This fire is in Florida, outside the CONUS west bbox.</description>
      <pubDate>Mon, 18 May 2026 10:00:00 EDT</pubDate>
      <dc:creator>test</dc:creator>
      <guid isPermaLink="false">888888</guid>
    </item>
  </channel>
</rss>"""


class TestInciWebHelpers:
    """Tests for InciWeb helper functions."""

    def test_parse_coordinates_from_description(self):
        """Parse coordinates from description text."""
        from central.adapters.inciweb import parse_coordinates_from_description

        description = """Coordinates:

Latitude: 47° 3 17  Longitude: 91° 38 6"""

        result = parse_coordinates_from_description(description)
        assert result is not None
        lon, lat = result
        # 47° 3' 17" = 47.054722...
        assert 47.0 < lat < 47.1
        # 91° 38' 6" = -91.635 (west longitude)
        assert -92.0 < lon < -91.0

    def test_parse_coordinates_no_match(self):
        """No coordinates in description returns None."""
        from central.adapters.inciweb import parse_coordinates_from_description

        result = parse_coordinates_from_description("No coordinates here")
        assert result is None

    def test_parse_state_from_description(self):
        """Parse state name and return 2-letter code."""
        from central.adapters.inciweb import parse_state_from_description

        description = """---

State: Minnesota

---"""
        assert parse_state_from_description(description) == "MN"

    def test_parse_state_from_description_new_mexico(self):
        """Parse multi-word state name."""
        from central.adapters.inciweb import parse_state_from_description

        description = """State: New Mexico

---"""
        assert parse_state_from_description(description) == "NM"

    def test_parse_state_from_description_no_match(self):
        """Unknown state name returns None."""
        from central.adapters.inciweb import parse_state_from_description

        description = """State: Unknown State

---"""
        assert parse_state_from_description(description) is None

    def test_strip_html(self):
        """HTML tags are stripped, entities decoded."""
        from central.adapters.inciweb import strip_html

        html = "This is &amp;nbsp;a <strong>test</strong> with <br>line breaks."
        result = strip_html(html)
        assert "<" not in result
        assert ">" not in result
        assert "&nbsp;" not in result
        assert "&amp;" not in result
        assert "test" in result


class TestInciWebAdapter:
    """Tests for InciWeb adapter."""

    @pytest.fixture
    def mock_config(self) -> AdapterConfig:
        return AdapterConfig(
            name="inciweb",
            enabled=True,
            cadence_s=600,
            settings={
                "region": {"north": 49.0, "south": 31.0, "east": -102.0, "west": -124.0}
            },
            updated_at=datetime.now(timezone.utc),
        )

    @pytest.fixture
    def mock_config_no_region(self) -> AdapterConfig:
        return AdapterConfig(
            name="inciweb",
            enabled=True,
            cadence_s=600,
            settings={},
            updated_at=datetime.now(timezone.utc),
        )

    @pytest.fixture
    def mock_config_store(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def cursor_db_path(self, tmp_path: Path) -> Path:
        return tmp_path / "cursors.db"

    @pytest.mark.asyncio
    async def test_normalization_with_georss_point(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Items with coordinates are correctly normalized."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = AsyncMock(return_value=SAMPLE_RSS_CONTENT)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # Bbox is west=-124, east=-102 (CONUS west)
        # Minnesota at -91 longitude is OUTSIDE bbox (east of -102)
        # California at -120 longitude is INSIDE bbox
        # Florida at -80 longitude is OUTSIDE bbox
        # Unknown state without coords passes through
        assert len(events) == 2

        # Check California event
        ca_event = next(e for e in events if e.data["guid"] == "327838")
        assert ca_event.id == "327838"
        assert ca_event.adapter == "inciweb"
        assert ca_event.category == "fire.narrative.inciweb"
        assert ca_event.severity == 0
        assert ca_event.geo.primary_region == "US-CA"
        assert ca_event.geo.centroid is not None

    @pytest.mark.asyncio
    async def test_normalization_without_georss_point(
        self, mock_config_no_region: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Items without coordinates have centroid=None."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config_no_region, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = AsyncMock(return_value=SAMPLE_RSS_CONTENT)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # All 4 items pass (no region filter)
        assert len(events) == 4

        # Check item without coords
        no_coords_event = next(e for e in events if e.data["guid"] == "999999")
        assert no_coords_event.geo.centroid is None
        assert no_coords_event.geo.regions == []
        assert no_coords_event.geo.primary_region is None

    def test_state_parse_from_title(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """State parsing from description produces correct region."""
        from central.adapters.inciweb import parse_state_from_description

        # Test California
        assert parse_state_from_description("State: California\n") == "CA"
        # Test Minnesota
        assert parse_state_from_description("State: Minnesota\n---") == "MN"
        # Test multi-word
        assert parse_state_from_description("State: New York\n") == "NY"
        # Test unknown
        assert parse_state_from_description("State: Narnia\n") is None

    @pytest.mark.asyncio
    async def test_html_stripping(
        self, mock_config_no_region: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """HTML is stripped from description, raw preserved in description_html."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config_no_region, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = AsyncMock(return_value=SAMPLE_RSS_CONTENT)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # California item has HTML tags in description
        ca_event = next(e for e in events if e.data["guid"] == "327838")

        # Plain text should not have HTML tags
        assert "<br>" not in ca_event.data["description"]
        assert "<p>" not in ca_event.data["description"]
        assert "<strong>" not in ca_event.data["description"]
        assert "&nbsp;" not in ca_event.data["description"]

        # Raw HTML should be preserved
        assert "&lt;br&gt;" in ca_event.data["description_html"] or "<br>" in ca_event.data["description_html"]

    def test_subject_for_with_state(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """subject_for returns correct subject with state."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config, mock_config_store, cursor_db_path)

        event = Event(
            id="test-id",
            adapter="inciweb",
            category="fire.narrative.inciweb",
            time=datetime.now(timezone.utc),
            severity=0,
            geo=Geo(primary_region="US-CA"),
            data={"title": "Test Fire", "description": "Test"},
        )

        subject = adapter.subject_for(event)
        assert subject == "central.fire.narrative.inciweb.ca"

    def test_subject_for_without_state(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """subject_for returns unknown when no state."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config, mock_config_store, cursor_db_path)

        event = Event(
            id="test-id",
            adapter="inciweb",
            category="fire.narrative.inciweb",
            time=datetime.now(timezone.utc),
            severity=0,
            geo=Geo(),
            data={"title": "Test Fire", "description": "Test"},
        )

        subject = adapter.subject_for(event)
        assert subject == "central.fire.narrative.inciweb.unknown"

    @pytest.mark.asyncio
    async def test_dedup_same_guid(
        self, mock_config_no_region: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """is_published/mark_published provides dedup functionality."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config_no_region, mock_config_store, cursor_db_path)
        await adapter.startup()

        # Initially not published
        assert adapter.is_published("327828") is False

        # Mark as published
        adapter.mark_published("327828")

        # Now it should be published
        assert adapter.is_published("327828") is True

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_bbox_filters_point_outside(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Items with coords outside bbox are filtered; items without coords pass."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config, mock_config_store, cursor_db_path)
        await adapter.startup()

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.text = AsyncMock(return_value=SAMPLE_RSS_CONTENT)

        with patch.object(adapter._session, "get", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock())):
            events = [e async for e in adapter.poll()]

        await adapter.shutdown()

        # Florida (-80 longitude) should be filtered out
        guids = {e.data["guid"] for e in events}
        assert "888888" not in guids  # Florida, outside bbox

        # Item without coords should pass through
        assert "999999" in guids

    @pytest.mark.asyncio
    async def test_apply_config_region_change(
        self, mock_config: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """apply_config updates region."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config, mock_config_store, cursor_db_path)

        assert adapter.region is not None
        assert adapter.region.north == 49.0

        new_config = AdapterConfig(
            name="inciweb",
            enabled=True,
            cadence_s=600,
            settings={
                "region": {"north": 50.0, "south": 35.0, "east": -100.0, "west": -120.0}
            },
            updated_at=datetime.now(timezone.utc),
        )
        await adapter.apply_config(new_config)

        assert adapter.region.north == 50.0
        assert adapter.region.south == 35.0

    @pytest.mark.asyncio
    async def test_dedup_in_poll_loop(
        self, mock_config_no_region: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Dedup integration: second poll with same items yields zero events."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config_no_region, mock_config_store, cursor_db_path)
        await adapter.startup()

        # Single-item RSS for clarity
        single_item_rss = """<?xml version="1.0" encoding="utf-8"?>
<rss xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0">
  <channel>
    <title>InciWeb</title>
    <item>
      <title>Test Fire</title>
      <link>http://inciweb.wildfire.gov/test</link>
      <description>State: California</description>
      <pubDate>Mon, 18 May 2026 09:00:00 EDT</pubDate>
      <guid isPermaLink="false">DEDUP-TEST-001</guid>
    </item>
  </channel>
</rss>"""

        def make_mock_response():
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.raise_for_status = MagicMock()
            mock_response.text = AsyncMock(return_value=single_item_rss)
            mock_response.headers = {"Last-Modified": None, "ETag": None}
            return mock_response

        # First poll: should yield 1 event
        with patch.object(
            adapter._session, "get",
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=make_mock_response()),
                __aexit__=AsyncMock()
            )
        ):
            events_first = [e async for e in adapter.poll()]

        assert len(events_first) == 1
        assert events_first[0].data["guid"] == "DEDUP-TEST-001"

        # Verify mark_published was called
        assert adapter.is_published("DEDUP-TEST-001") is True

        # Second poll: same item should be skipped (dedup)
        with patch.object(
            adapter._session, "get",
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=make_mock_response()),
                __aexit__=AsyncMock()
            )
        ):
            events_second = [e async for e in adapter.poll()]

        assert len(events_second) == 0  # Dedup prevents re-yield

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_conditional_304_yields_zero(
        self, mock_config_no_region: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """HTTP 304 Not Modified returns empty list and yields zero events."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config_no_region, mock_config_store, cursor_db_path)
        await adapter.startup()

        # Mock 304 response
        mock_response = AsyncMock()
        mock_response.status = 304
        mock_response.raise_for_status = MagicMock()

        with patch.object(
            adapter._session, "get",
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=mock_response),
                __aexit__=AsyncMock()
            )
        ):
            events = [e async for e in adapter.poll()]

        assert len(events) == 0

        await adapter.shutdown()

    @pytest.mark.asyncio
    async def test_conditional_headers_sent_after_first_poll(
        self, mock_config_no_region: AdapterConfig, mock_config_store: MagicMock, cursor_db_path: Path
    ):
        """Conditional fetch headers sent on second poll after first captures them."""
        from central.adapters.inciweb import InciWebAdapter

        adapter = InciWebAdapter(mock_config_no_region, mock_config_store, cursor_db_path)
        await adapter.startup()

        # First response with Last-Modified and ETag
        first_response = AsyncMock()
        first_response.status = 200
        first_response.raise_for_status = MagicMock()
        first_response.text = AsyncMock(return_value="""<?xml version="1.0"?>
<rss version="2.0"><channel><title>Test</title></channel></rss>""")
        first_response.headers = {
            "Last-Modified": "Tue, 19 May 2026 03:00:00 GMT",
            "ETag": "\"abc123\"",
        }

        # Track headers sent on second request
        captured_headers = {}

        def capture_get(*args, **kwargs):
            captured_headers.update(kwargs.get("headers", {}))
            second_response = AsyncMock()
            second_response.status = 304
            second_response.raise_for_status = MagicMock()
            return AsyncMock(
                __aenter__=AsyncMock(return_value=second_response),
                __aexit__=AsyncMock()
            )

        # First poll
        with patch.object(
            adapter._session, "get",
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=first_response),
                __aexit__=AsyncMock()
            )
        ):
            [e async for e in adapter.poll()]

        # Verify adapter captured the headers
        assert adapter._last_modified == "Tue, 19 May 2026 03:00:00 GMT"
        assert adapter._etag == "\"abc123\""

        # Second poll with header capture
        with patch.object(adapter._session, "get", side_effect=capture_get):
            [e async for e in adapter.poll()]

        # Verify conditional headers were sent
        assert captured_headers.get("If-Modified-Since") == "Tue, 19 May 2026 03:00:00 GMT"
        assert captured_headers.get("If-None-Match") == "\"abc123\""

        await adapter.shutdown()
