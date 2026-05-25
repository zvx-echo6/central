"""Tests for v0.7.3 offset pagination: offset/limit parsing, offset-vs-cursor
query shape, and the _build_pagination windowing math. No live DB (captured SQL).
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.gui import routes


# --- offset / limit parsing -------------------------------------------------

def test_offset_parse_valid():
    parsed, err = routes._parse_events_params({"offset": "100", "limit": "50", "time": "all"})
    assert err is None and parsed["offset"] == 100 and parsed["limit"] == 50


def test_offset_negative_errors():
    parsed, err = routes._parse_events_params({"offset": "-1", "time": "all"})
    assert parsed is None and "offset" in err


def test_offset_noninteger_errors():
    parsed, err = routes._parse_events_params({"offset": "abc", "time": "all"})
    assert parsed is None and "offset" in err


def test_limit_max_is_250():
    parsed, err = routes._parse_events_params({"limit": "250", "time": "all"})
    assert err is None and parsed["limit"] == 250
    over, err2 = routes._parse_events_params({"limit": "251", "time": "all"})
    assert over is None and "250" in err2


def test_default_offset_selects_mode():
    """GUI passes default_offset=0 -> offset-mode; events.json omits -> cursor-mode."""
    gui, _ = routes._parse_events_params({"time": "all"}, default_offset=0)
    assert gui["offset"] == 0
    api, _ = routes._parse_events_params({"time": "all"})
    assert api["offset"] is None


# --- query shape (captured SQL) ---------------------------------------------

async def _capture(parsed):
    captured = {}

    async def fake_fetch(query, *args):
        captured["query"] = query
        captured["params"] = list(args)
        return []

    conn = MagicMock()
    conn.fetch = fake_fetch
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    with patch("central.gui.routes.get_pool", return_value=pool):
        await routes._fetch_events(parsed)
    return captured


@pytest.mark.asyncio
async def test_offset_mode_query_uses_limit_offset_and_window_count():
    parsed, _ = routes._parse_events_params({"time": "all"}, default_offset=100)
    cap = await _capture(parsed)
    assert "LIMIT $" in cap["query"] and "OFFSET $" in cap["query"]
    assert "count(*) OVER() AS total_count" in cap["query"]
    assert 100 in cap["params"]  # offset bound as a param


@pytest.mark.asyncio
async def test_cursor_mode_query_has_no_offset_no_window_count():
    parsed, _ = routes._parse_events_params({"time": "all"})  # no offset -> cursor-mode
    cap = await _capture(parsed)
    assert "OFFSET" not in cap["query"]
    assert "count(*) OVER()" not in cap["query"]


# --- _build_pagination math -------------------------------------------------

def test_build_pagination_basic_windowing():
    pg = routes._build_pagination(2341, 100, 50)
    assert pg["page"] == 3 and pg["total_pages"] == 47
    assert pg["start"] == 101 and pg["end"] == 150
    assert pg["prev_offset"] == 50 and pg["next_offset"] == 150
    nums = [p["page"] for p in pg["pages"] if "page" in p]
    assert 1 in nums and 47 in nums and 3 in nums
    assert any(p.get("ellipsis") for p in pg["pages"])
    assert pg["per_page_options"] == [25, 50, 100, 250]


def test_build_pagination_first_page_has_no_prev():
    pg = routes._build_pagination(120, 0, 50)
    assert pg["page"] == 1 and pg["prev_offset"] is None and pg["next_offset"] == 50
    assert pg["start"] == 1 and pg["end"] == 50 and pg["total_pages"] == 3


def test_build_pagination_last_page_has_no_next():
    pg = routes._build_pagination(120, 100, 50)
    assert pg["page"] == 3 and pg["next_offset"] is None and pg["end"] == 120


def test_build_pagination_zero_results():
    pg = routes._build_pagination(0, 0, 50)
    assert pg["total"] == 0 and pg["total_pages"] == 1
    assert pg["start"] == 0 and pg["end"] == 0
    assert pg["prev_offset"] is None and pg["next_offset"] is None


def test_build_pagination_single_page_no_next():
    pg = routes._build_pagination(30, 0, 50)
    assert pg["total_pages"] == 1 and pg["next_offset"] is None and pg["end"] == 30
