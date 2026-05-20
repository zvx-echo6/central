"""SQLite-backed enrichment cache with rounded-coords keys + TTL.

Keyed on (enricher_name, lat_rounded, lon_rounded) where coordinates are
rounded to 4 decimal places (~11 m). Uses stdlib sqlite3 off the event loop
via asyncio.to_thread (no async-sqlite dependency in the project). A fresh
connection is opened per operation — sqlite3 connections are not safe to
share across threads, and to_thread may run ops on different pool threads.
"""

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_COORD_PRECISION = 4

_SCHEMA = """
CREATE TABLE IF NOT EXISTS enrichment_cache (
    enricher_name TEXT NOT NULL,
    lat_rounded REAL NOT NULL,
    lon_rounded REAL NOT NULL,
    payload_json TEXT NOT NULL,
    cached_at TEXT NOT NULL,
    PRIMARY KEY (enricher_name, lat_rounded, lon_rounded)
)
"""


def round_coord(value: float) -> float:
    """Round a coordinate to the cache-key precision (4 dp)."""
    return round(float(value), _COORD_PRECISION)


class EnrichmentCache:
    """Thread-offloaded sqlite cache for enrichment bundles."""

    def __init__(self, db_path: str | Path, ttl_s: int = 86400) -> None:
        self._db_path = Path(db_path)
        self._ttl_s = ttl_s
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path, timeout=30)

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.execute(_SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # --- sync bodies (run inside asyncio.to_thread) ------------------------

    def _get_sync(self, enricher_name: str, lat: float, lon: float) -> dict[str, Any] | None:
        lat_r = round_coord(lat)
        lon_r = round_coord(lon)
        conn = self._connect()
        try:
            cur = conn.execute(
                """
                SELECT payload_json, cached_at FROM enrichment_cache
                WHERE enricher_name = ? AND lat_rounded = ? AND lon_rounded = ?
                """,
                (enricher_name, lat_r, lon_r),
            )
            row = cur.fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        payload_json, cached_at_iso = row
        if self._is_expired(cached_at_iso):
            return None
        return json.loads(payload_json)

    def _set_sync(
        self, enricher_name: str, lat: float, lon: float, payload: dict[str, Any]
    ) -> None:
        lat_r = round_coord(lat)
        lon_r = round_coord(lon)
        now_iso = datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO enrichment_cache
                    (enricher_name, lat_rounded, lon_rounded, payload_json, cached_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (enricher_name, lat_rounded, lon_rounded) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    cached_at = excluded.cached_at
                """,
                (enricher_name, lat_r, lon_r, json.dumps(payload), now_iso),
            )
            conn.commit()
        finally:
            conn.close()

    def _is_expired(self, cached_at_iso: str) -> bool:
        try:
            cached_at = datetime.fromisoformat(cached_at_iso)
        except ValueError:
            return True
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        age_s = (datetime.now(timezone.utc) - cached_at).total_seconds()
        return age_s > self._ttl_s

    # --- async surface -----------------------------------------------------

    async def get(self, enricher_name: str, lat: float, lon: float) -> dict[str, Any] | None:
        """Return the cached bundle, or None on miss / expiry."""
        return await asyncio.to_thread(self._get_sync, enricher_name, lat, lon)

    async def set(
        self, enricher_name: str, lat: float, lon: float, payload: dict[str, Any]
    ) -> None:
        """Cache a bundle (idempotent upsert on the rounded-coords key)."""
        await asyncio.to_thread(self._set_sync, enricher_name, lat, lon, payload)
