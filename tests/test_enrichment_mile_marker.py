"""Tests for v0.10.6 mile-marker regex extraction.

Coverage strategy:
- Real ITD samples drive the parametrize for high/medium/no-match tiers --
  these are the comments that actually appear on CENTRAL_TRAFFIC events
  and include the red-herring classes (phone numbers, project key numbers,
  state-highway numbers, date/time numbers) that the regex must reject.
- Crafted samples cover the M.P./MM/milemarker/bare-mile patterns the spec
  requires for future per-state-DOT adapters even though they're not in
  live ITD data today.
"""

from __future__ import annotations

import pytest

from central.enrichment.mile_marker import extract


# --- High-confidence: single unambiguous match (real ITD comments) -----------


@pytest.mark.parametrize("text, expected_value", [
    ("Emergency vehicles blocking the right lane and right shoulder, "
     "eastbound I-84 near milepost 32.5. Keep left.", 32.5),
    ("Crash on westbound I-84 at milepost 54.  One right lane blocked.", 54.0),
    ("Crash westbound I-84 milepost 42 blocking the right two lanes. "
     "Expect delays, use caution and keep left.", 42.0),
    ("A crash is blocking all lanes on Highway 21, near milepost 10, "
     "before Lucky Peak State Park.", 10.0),
    ("All directions of travel blocked SH 21 milepost 15 due to a crash.", 15.0),
])
def test_high_confidence_real_samples(text, expected_value):
    result = extract(text)
    assert result is not None
    assert result["value"] == expected_value
    assert result["source"] == "comment_regex"
    assert result["confidence"] == "high"


def test_red_herring_road_numbers_in_real_sample():
    """US-20 / E 200 St must NOT shadow the actual milepost 320."""
    text = ("A crash is blocking the rightmost lane of US-20 at milepost 320, "
            "near E 200 St. Keep Left.")
    result = extract(text)
    assert result is not None
    assert result["value"] == 320.0
    assert result["confidence"] == "high"


# --- Medium-confidence: range / multi-match (real ITD comments) --------------


@pytest.mark.parametrize("text, expected_first_value", [
    ("6/6 - 6/8 Southbound Left Lane Closure (slow) from MP 80 to MP 81.", 80.0),
    ("Northbound Left Lane Closure from MP 72.6 to MP 76.25 from "
     "7:00 PM to 6:00 AM.", 72.6),
])
def test_medium_confidence_real_samples(text, expected_first_value):
    result = extract(text)
    assert result is not None
    assert result["value"] == expected_first_value
    assert result["confidence"] == "medium"


# --- No-match: real ITD comments that must NOT yield a value -----------------


@pytest.mark.parametrize("text", [
    "40th St will be closed for work on water lines.",
    "Bridge Repair",
    "Bridge Maintenance. ITD- Phil Etchart, 208-490-4593, "
    "Jason Fisher, 208-420-8328.",
    "ITD Project Key Number 21832 McCammon IC to Old US-91",
    "Sunday, June 28, 2026, from approximately 9:45 AM to 10:30 AM. "
    "Traffic restrictions will be lifted as the motorcade passes.",
])
def test_no_match_real_samples(text):
    assert extract(text) is None


# --- Crafted high-confidence patterns (spec, not in live ITD data) -----------


@pytest.mark.parametrize("text, expected_value", [
    ("Eastbound MP 32", 32.0),
    ("Closure at M.P. 32", 32.0),
    ("Crash near M.P 32 today", 32.0),
    ("Slow at MM 32", 32.0),
    ("Slow at M.M. 32", 32.0),
    ("milepost 32", 32.0),
    ("mile post 32", 32.0),
    ("mile-post 32", 32.0),
    ("milemarker 32", 32.0),
    ("mile marker 32", 32.0),
    ("mile-marker 32", 32.0),
    ("milepost 32.5", 32.5),
])
def test_crafted_high_confidence_patterns(text, expected_value):
    result = extract(text)
    assert result is not None
    assert result["value"] == expected_value
    assert result["confidence"] == "high"


# --- Crafted medium-confidence: multiple unambiguous matches -----------------


def test_crafted_medium_confidence_multiple_mp():
    result = extract("Closure from MP 5 to MP 9.")
    assert result is not None
    assert result["value"] == 5.0
    assert result["confidence"] == "medium"


def test_crafted_medium_mixed_keywords():
    """Mixed unambiguous keyword forms both count -> medium, first wins."""
    result = extract("milepost 5 and mile marker 10 affected.")
    assert result is not None
    assert result["value"] == 5.0
    assert result["confidence"] == "medium"


# --- Crafted low-confidence: bare 'mile N' (spec, not in live data) ----------


def test_crafted_low_confidence_bare_mile():
    """Bare 'mile N' without MP/milepost context -- extract at 'low'."""
    result = extract("Crash near mile 14")
    assert result is not None
    assert result["value"] == 14.0
    assert result["confidence"] == "low"


def test_crafted_low_confidence_bare_mile_with_decimal():
    result = extract("Slowdown near mile 14.5 today.")
    assert result is not None
    assert result["value"] == 14.5
    assert result["confidence"] == "low"


# --- Crafted: tier precedence ------------------------------------------------


def test_high_keyword_beats_bare_mile_in_same_text():
    """If a high-conf keyword matches, bare 'mile N' is not consulted."""
    result = extract("Crash near milepost 22, also affecting mile 14 detour.")
    assert result is not None
    assert result["value"] == 22.0
    assert result["confidence"] == "high"


# --- Edge cases --------------------------------------------------------------


def test_empty_string():
    assert extract("") is None


def test_none_input():
    assert extract(None) is None


def test_numbers_without_keyword_never_match():
    """Standalone numbers without an MP/mile keyword must not match."""
    assert extract("Highway 21, US-20, 208-555-1234, exit 84.") is None


def test_case_insensitive():
    """Keywords must match regardless of capitalization."""
    result = extract("CRASH at MILEPOST 50.")
    assert result is not None
    assert result["value"] == 50.0
    assert result["confidence"] == "high"


def test_substring_keywords_do_not_match():
    """'amp', 'stamp', 'miles', 'milestone' must not match the keyword regex."""
    assert extract("The amp 50 was loud.") is None
    assert extract("Stamp 50 on the document.") is None
    assert extract("Miles 50 traveled.") is None
    assert extract("Milestone 50 reached.") is None


def test_result_dict_shape():
    """Result has exactly {value: float, source: 'comment_regex', confidence: str}."""
    result = extract("milepost 32.5")
    assert result is not None
    assert set(result.keys()) == {"value", "source", "confidence"}
    assert isinstance(result["value"], float)
    assert result["source"] == "comment_regex"
    assert result["confidence"] in {"high", "medium", "low"}
