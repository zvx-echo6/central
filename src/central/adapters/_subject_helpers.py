"""Shared helpers for regional subject routing (v0.9.20).

Provides subject-token builders for adapters that route events by
state (US) or country (international). The us.<state> / <country>
pattern matches the NWS precedent and avoids ISO-2 collisions
(e.g., 'id' = Idaho vs Indonesia).
"""

from typing import Any

# US state name -> 2-letter code mapping. Photon returns full names;
# USGS site metadata returns full names. This is the canonical lookup.
# Includes 50 states + DC + territories (PR, VI, GU, AS, MP).
US_STATE_NAME_TO_CODE: dict[str, str] = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
    # DC
    "district of columbia": "dc",
    "washington dc": "dc",
    "washington d.c.": "dc",
    # Territories
    "puerto rico": "pr",
    "virgin islands": "vi",
    "u.s. virgin islands": "vi",
    "guam": "gu",
    "american samoa": "as",
    "northern mariana islands": "mp",
}

# Exact country strings that route to the US branch.
# Excludes "United States Virgin Islands" etc. which have their own territory codes.
_US_COUNTRY_NAMES = frozenset({"united states", "united states of america"})


def subject_for_country(country: str | None) -> str:
    """Lowercase, hyphenate spaces. 'unknown' for missing/empty.

    Takes first segment if comma-separated (handles Photon multi-value).
    Extracted from gdacs.py for shared use.
    """
    if not country:
        return "unknown"
    first = country.split(",")[0].strip()
    if not first:
        return "unknown"
    return first.lower().replace(" ", "-")


def _state_name_to_code(state_name: str | None) -> str | None:
    """Convert full state name to 2-letter code. Returns None if not found."""
    if not state_name:
        return None
    normalized = state_name.strip().lower()
    # Direct lookup
    if normalized in US_STATE_NAME_TO_CODE:
        return US_STATE_NAME_TO_CODE[normalized]
    # Already a 2-letter code? Validate against known codes.
    if len(normalized) == 2 and normalized in US_STATE_NAME_TO_CODE.values():
        return normalized
    return None


def _country_from_geocoder(event_data: dict[str, Any]) -> str | None:
    """Extract country from geocoder enrichment, null-safe.

    Handles all null-bundle variants: _enriched missing, geocoder missing,
    geocoder=None, country missing.
    """
    enriched = event_data.get("_enriched") or {}
    geocoder = enriched.get("geocoder") or {}
    return geocoder.get("country")


def subject_for_region(event_data: dict[str, Any]) -> str:
    """Build the regional subject token from enriched geocoder data.

    Returns:
        "us.<state>" for US events with resolved state (e.g., "us.id")
        "us.unknown" for US events with unresolved state
        "<country>" for international events (e.g., "japan", "mexico")
        "unknown" if no country signal at all

    The distinction: "unknown" = geocoder returned nothing; "us.unknown" =
    known US event but state lookup failed. Consumers subscribing to
    central.>.us.> will match us.unknown but not bare unknown.

    Reads from event.data["_enriched"]["geocoder"] which is populated
    by the Photon enrichment pipeline.
    """
    enriched = event_data.get("_enriched") or {}
    geocoder = enriched.get("geocoder") or {}

    country = geocoder.get("country")
    state = geocoder.get("state")

    # US event: use us.<state> pattern
    if country and country.lower().strip() in _US_COUNTRY_NAMES:
        state_code = _state_name_to_code(state)
        if state_code:
            return f"us.{state_code}"
        return "us.unknown"

    # International event: use country token
    if country:
        return subject_for_country(country)

    return "unknown"
