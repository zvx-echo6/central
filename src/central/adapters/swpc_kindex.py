"""NOAA SWPC Planetary K-Index adapter."""

import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from central.adapter import SourceAdapter
from central.adapters.swpc_common import (
    SWPC_KINDEX_URL,
    SWPCSettings,
    parse_swpc_timestamp,
    severity_from_kp,
)
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo

logger = logging.getLogger(__name__)


class SWPCKindexAdapter(SourceAdapter):
    """NOAA SWPC planetary K-index adapter."""

    name = "swpc_kindex"
    display_name = "NOAA SWPC — Planetary K-Index"
    description = "Planetary K-index measurements at 3-hour cadence from NOAA SWPC."
    settings_schema = SWPCSettings
    requires_api_key = None
    api_key_field = None
    wizard_order = None
    default_cadence_s = 600

    def __init__(
        self,
        config: AdapterConfig,
        config_store: ConfigStore,
        cursor_db_path: Path,
    ) -> None:
        self._config_store = config_store
        self._cursor_db_path = cursor_db_path
        self._session: aiohttp.ClientSession | None = None
        self._db: sqlite3.Connection | None = None

    async def startup(self) -> None:
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
        )
        self._db = sqlite3.connect(self._cursor_db_path)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS published_ids (
                adapter TEXT NOT NULL,
                event_id TEXT NOT NULL,
                first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (adapter, event_id)
            )
        """)
        self._db.execute("""
            CREATE INDEX IF NOT EXISTS published_ids_last_seen
            ON published_ids (last_seen)
        """)
        self._db.commit()
        logger.info("SWPC kindex adapter started")

    async def shutdown(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None
        logger.info("SWPC kindex adapter shut down")

    async def apply_config(self, new_config: AdapterConfig) -> None:
        logger.info("SWPC kindex config updated")

    def is_published(self, event_id: str) -> bool:
        if not self._db:
            return False
        cur = self._db.execute(
            "SELECT 1 FROM published_ids WHERE adapter = ? AND event_id = ?",
            (self.name, event_id),
        )
        return cur.fetchone() is not None

    def mark_published(self, event_id: str) -> None:
        if not self._db:
            return
        self._db.execute(
            """
            INSERT INTO published_ids (adapter, event_id, first_seen, last_seen)
            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (adapter, event_id) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP
            """,
            (self.name, event_id),
        )
        self._db.commit()

    def sweep_old_ids(self) -> int:
        if not self._db:
            return 0
        cur = self._db.execute(
            "DELETE FROM published_ids WHERE adapter = ? AND last_seen < datetime('now', '-14 days')",
            (self.name,),
        )
        self._db.commit()
        count = cur.rowcount
        if count > 0:
            logger.info("SWPC kindex swept old dedup entries", extra={"count": count})
        return count

    def subject_for(self, event: Event) -> str:
        return "central.space.kindex"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, TimeoutError)),
    )
    async def _fetch(self) -> list[dict[str, Any]]:
        if not self._session:
            raise RuntimeError("Session not initialized")
        async with self._session.get(
            SWPC_KINDEX_URL, headers={"User-Agent": "Central/0.4"}
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        logger.info("SWPC kindex fetch completed", extra={"item_count": len(data)})
        return data

    async def poll(self) -> AsyncIterator[Event]:
        if not self._db:
            raise RuntimeError("Database not initialized")

        try:
            items = await self._fetch()
        except Exception as e:
            logger.error("SWPC kindex fetch failed", extra={"error": str(e)})
            raise

        events_yielded = 0
        for item in items:
            time_tag = item.get("time_tag")
            kp = item.get("Kp")
            if not time_tag or kp is None:
                continue

            event_id = time_tag
            if self.is_published(event_id):
                continue

            event_time = parse_swpc_timestamp(time_tag, "kindex") or datetime.now(timezone.utc)

            event = Event(
                id=event_id,
                adapter=self.name,
                category="space.kindex",
                time=event_time,
                severity=severity_from_kp(kp),
                geo=Geo(),
                data={
                    "time_tag": time_tag,
                    "Kp": kp,
                    "a_running": item.get("a_running"),
                    "station_count": item.get("station_count"),
                },
            )

            yield event
            self.mark_published(event_id)
            events_yielded += 1

        self.sweep_old_ids()
        logger.info("SWPC kindex poll completed", extra={"events_yielded": events_yielded})
