"""Shared utilities for NOAA SWPC space weather adapters."""

import re
from datetime import datetime, timezone

from pydantic import BaseModel

SWPC_ALERTS_URL = "https://services.swpc.noaa.gov/products/alerts.json"
SWPC_KINDEX_URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
SWPC_PROTONS_URL = "https://services.swpc.noaa.gov/json/goes/primary/integral-protons-1-day.json"


class SWPCSettings(BaseModel):
    """Settings schema for SWPC adapters. No operator-tunable knobs today."""


def parse_swpc_timestamp(raw: str | None, endpoint_kind: str) -> datetime | None:
    """Normalize SWPC timestamp strings to UTC datetime.

    endpoint_kind shapes:
        alerts  -> "2026-05-19 05:14:59.780"   (space-separated, no TZ; UTC per message body)
        kindex  -> "2026-05-12T00:00:00"       (ISO without TZ; UTC by convention)
        protons -> "2026-05-18T05:35:00Z"      (ISO with Z)
    """
    if not raw:
        return None
    if endpoint_kind == "alerts":
        try:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    if endpoint_kind == "kindex":
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if endpoint_kind == "protons":
        raw_norm = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        dt = datetime.fromisoformat(raw_norm)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    raise ValueError(f"unknown endpoint_kind: {endpoint_kind!r}")


def severity_from_kp(kp: float | int | None) -> int:
    """Map planetary K-index value (0-9) to severity 0-4 via the G-scale.

    Kp 5 = G1 = severity 1, Kp 6 = G2 = severity 2, Kp 7 = G3 = severity 3,
    Kp 8 = G4 = severity 4, Kp 9 = G5 = severity 4 (capped).
    """
    if kp is None:
        return 0
    if kp < 5:
        return 0
    if kp < 6:
        return 1
    if kp < 7:
        return 2
    if kp < 8:
        return 3
    return 4


_ALERT_KP_PATTERN = re.compile(r"^K0([5-9])[AW]$")


def severity_from_alert_product_id(product_id: str | None) -> int:
    """Best-effort severity for an alert from its product_id G-scale.

    Product IDs of form K0[5-9][AW] identify Kp-based geomagnetic storm
    alerts and warnings (K05A=G1, K06A=G2, K07A=G3, K08A=G4, K09A=G5).
    All other product IDs return 0.
    """
    if not product_id:
        return 0
    m = _ALERT_KP_PATTERN.match(product_id.upper())
    if not m:
        return 0
    return severity_from_kp(int(m.group(1)))
