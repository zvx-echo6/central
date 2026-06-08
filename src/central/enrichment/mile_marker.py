"""Mile-marker extraction from free-text comment fields.

Used by DOT adapters (itd_511 today; future per-state DOTs) to pull a
mile-marker value out of upstream freeform comments. Returns ``None`` when
no match -- the caller is expected to omit the field entirely rather than
write a null placeholder, so subscribers can distinguish "no MP mentioned"
from "MP extraction ran and found nothing".

Confidence tiers (v0.10.6 spec):

- ``high``:   exactly one unambiguous keyword+number match (``milepost`` /
              ``MP`` / ``MM`` / ``mile marker`` etc.)
- ``medium``: two or more unambiguous matches in the same comment
              (e.g. a range like ``MP 80 to MP 81``); first match wins
- ``low``:    no unambiguous match but a bare ``mile N`` token is present;
              consumers may choose to ignore low-confidence extractions

Shared module by design -- regex is universal, not Idaho-specific, so
future per-state-DOT adapters (Wyoming, Montana, etc.) call
``from central.enrichment.mile_marker import extract``.
"""

from __future__ import annotations

import re
from typing import Any

# Unambiguous keyword forms. Each branch is a keyword family; the trailing
# ``\s+ (\d+ (?:\.\d+)?)`` captures the value. ``\b`` anchors guard against
# substring matches inside larger words (so "amp 5" / "stamp 5" / "miles 5"
# never match).
_KEYWORD_PATTERN = re.compile(
    r"""
    \b
    (?:
        m \.? p \.?            |   # MP, M.P., MP., M.P
        m \.? m \.?            |   # MM, M.M., MM., M.M
        mile [\s-]* post       |   # milepost, mile post, mile-post
        mile [\s-]* marker         # milemarker, mile marker, mile-marker
    )
    \s+
    ( \d+ (?: \. \d+ )? )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Low-confidence fallback: bare ``mile N``. Word-boundary at start prevents
# the prefix from matching inside ``milepost`` / ``milemarker`` / ``miles``;
# the ``\s+`` between ``mile`` and the digit further excludes those words.
_BARE_MILE_PATTERN = re.compile(
    r"\bmile\s+(\d+(?:\.\d+)?)\b",
    re.IGNORECASE,
)

_SOURCE = "comment_regex"


def extract(text: str | None) -> dict[str, Any] | None:
    """Return ``{value, source, confidence}`` if a mile marker is found, else ``None``.

    Pure function, no I/O. Never raises on malformed input -- callers can
    pass a raw upstream string with no try/except.
    """
    if not text:
        return None

    keyword_hits = _KEYWORD_PATTERN.findall(text)
    if len(keyword_hits) == 1:
        return {"value": float(keyword_hits[0]), "source": _SOURCE, "confidence": "high"}
    if len(keyword_hits) > 1:
        return {"value": float(keyword_hits[0]), "source": _SOURCE, "confidence": "medium"}

    bare_hits = _BARE_MILE_PATTERN.findall(text)
    if bare_hits:
        return {"value": float(bare_hits[0]), "source": _SOURCE, "confidence": "low"}

    return None
