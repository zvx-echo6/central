"""Generic HTTP / GeoJSON adapter — operator-instantiable, no Python required.

One class (kind="generic_http") supports many config.adapters rows.  Each
row carries a distinct ``name`` (instance identity) and a ``settings`` dict
that drives a separate poll loop.  PR3 ships the GUI create route; this PR
ships only the class.

v1: public feeds only; auth is a follow-up.
"""

import logging
import sqlite3
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import aiohttp
from pydantic import BaseModel, field_validator
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from central.adapter import SourceAdapter
from central.adapters._subject_helpers import subject_for_region
from central.config_models import AdapterConfig
from central.config_store import ConfigStore
from central.models import Event, Geo
from central.streams import STREAMS

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Derive the valid domain set from the stream registry at import time.
# Subject filters are always "central.<domain>.>" so we split on "." and
# take index 1.
# ---------------------------------------------------------------------------
_VALID_DOMAINS: frozenset[str] = frozenset(
    s.subject_filter.split(".")[1] for s in STREAMS
)

_DEDUP_DDL = (
    "CREATE TABLE IF NOT EXISTS published_ids ("
    "adapter TEXT NOT NULL, event_id TEXT NOT NULL, "
    "first_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
    "PRIMARY KEY (adapter, event_id))"
)
_DEDUP_IDX = (
    "CREATE INDEX IF NOT EXISTS published_ids_last_seen "
    "ON published_ids (last_seen)"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dig(obj: Any, path: str) -> Any:
    """Walk a nested dict/list by a dotted path string.

    Each segment is tried as a dict key first; if the current node is a
    list/tuple the segment is coerced to an integer index.  Returns None on
    any miss (missing key, out-of-range index, wrong node type).

    Examples::

        _dig({"a": {"b": 1}}, "a.b")       # 1
        _dig({"a": [10, 20]}, "a.1")        # 20
        _dig({"a": 1}, "a.b")              # None
        _dig(None, "anything")              # None
    """
    parts = path.split(".")
    cur = obj
    for part in parts:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, (list, tuple)):
            try:
                cur = cur[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return cur


# ---------------------------------------------------------------------------
# Settings schema
# ---------------------------------------------------------------------------

class FieldMapping(BaseModel):
    """Map one path inside a source item to a key in Event.data.

    Mirrors TomTomFlowSettings / TileCoord so the GUI model_list widget
    renders it identically.
    """
    source_path: str  # dotted path into the source item
    dest_key: str     # key written into Event.data


class GenericHttpSettings(BaseModel):
    """Settings schema for GenericHttpAdapter instances."""

    url: str
    """Endpoint to poll (GET, no auth in v1)."""

    format: Literal["geojson", "json"] = "geojson"
    """Response format hint (currently informational; both paths use the same
    JSON fetch; the geometry_path / lat_path controls shape extraction)."""

    items_path: str = "features"
    """Dotted path to the array of items inside the response object."""

    domain: str
    """Event domain; must match an existing NATS stream (e.g. ``wx``, ``fire``,
    ``quake``).  Determines which JetStream stream the event lands in."""

    @field_validator("domain")
    @classmethod
    def _validate_domain(cls, v: str) -> str:
        if v not in _VALID_DOMAINS:
            valid = ", ".join(sorted(_VALID_DOMAINS))
            raise ValueError(
                f"unknown domain '{v}'; must be one of: {valid}"
            )
        return v

    category_suffix: str = "alert"
    """Appended to domain to form Event.category: ``{domain}.{category_suffix}``."""

    id_path: str
    """Dotted path to a stable, unique identifier within each item (required
    for deduplication)."""

    time_path: str | None = None
    """Dotted path to an ISO-8601 timestamp string.  When None, poll time
    (UTC now) is used."""

    title_path: str | None = None
    """Dotted path to a human-readable title → written to ``data['title']``."""

    geometry_path: str = "geometry"
    """(GeoJSON) dotted path to a GeoJSON geometry object within each item.
    Ignored when the resolved value is not a dict."""

    lat_path: str | None = None
    """(JSON) dotted path to a numeric latitude.  Used only when
    geometry_path does not resolve to a geometry dict."""

    lon_path: str | None = None
    """(JSON) dotted path to a numeric longitude.  See lat_path."""

    severity_path: str | None = None
    """Dotted path to an integer severity (0-4).  When None, severity is
    omitted from the event."""

    field_mappings: list[FieldMapping] = []
    """Extra source-path → dest-key pairs written into Event.data."""


# ---------------------------------------------------------------------------
# Adapter class
# ---------------------------------------------------------------------------

class GenericHttpAdapter(SourceAdapter):
    """Config-driven REST/GeoJSON source adapter.

    A single class that operators can instantiate many times via distinct
    ``config.adapters`` rows (each row has a unique ``name`` / instance
    identity and ``kind="generic_http"``).  No Python required to add a
    new source.
    """

    # Class-level identity (kind) — used by discover_adapters() as the
    # registry key.  Each *instance* overrides self.name = config.name in
    # __init__ so dedup and logs are scoped to the instance.
    name = "generic_http"
    display_name = "Generic HTTP Source"
    description = (
        "Config-driven adapter that polls any public REST or GeoJSON endpoint "
        "and maps response fields to Central events.  Create one row per "
        "source; no Python required."
    )
    settings_schema = GenericHttpSettings
    default_cadence_s = 300
    data_class = "event"
    wizard_order = None   # not in the setup wizard; created via operator GUI
    enrichment_locations = []
    bypass_bbox_filter = False

    def __init__(
        self,
        config: AdapterConfig,
        config_store: ConfigStore,   # unused; accepted for signature uniformity
        cursor_db_path: Path,
    ) -> None:
        # Override class-level name with the *instance* name so that the
        # inherited dedup helpers (is_published / mark_published / sweep_old_ids)
        # scope their SQL queries to this instance, not the generic_http class.
        self.name = config.name
        self._cursor_db_path = cursor_db_path
        self._session: aiohttp.ClientSession | None = None
        self._db: sqlite3.Connection | None = None
        self._settings = GenericHttpSettings(**config.settings)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def startup(self) -> None:
        """Initialize HTTP session and dedup tracker."""
        self._session = aiohttp.ClientSession(
            headers={"User-Agent": "Central/1.0 (generic_http adapter)"},
            timeout=aiohttp.ClientTimeout(total=30),
        )
        self._db = sqlite3.connect(str(self._cursor_db_path))
        self._db.execute(_DEDUP_DDL)
        self._db.execute(_DEDUP_IDX)
        self._db.commit()
        self.sweep_old_ids()
        logger.info(
            "generic_http adapter started",
            extra={"adapter": self.name, "url": self._settings.url},
        )

    async def shutdown(self) -> None:
        """Close HTTP session and database."""
        if self._session:
            await self._session.close()
            self._session = None
        if self._db:
            self._db.close()
            self._db = None
        logger.info("generic_http adapter shut down", extra={"adapter": self.name})

    # ------------------------------------------------------------------
    # Configuration hot-reload
    # ------------------------------------------------------------------

    async def apply_config(self, new_config: AdapterConfig) -> None:
        """Re-parse settings in place (no reconstruction needed)."""
        self._settings = GenericHttpSettings(**new_config.settings)
        logger.info(
            "generic_http config applied",
            extra={"adapter": self.name, "url": self._settings.url},
        )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=15),
        retry=retry_if_exception_type((aiohttp.ClientError,)),
        reraise=True,
    )
    async def _fetch(self, url: str) -> dict[str, Any]:
        """GET ``url`` and return parsed JSON.  Retried up to 3 times."""
        if not self._session:
            raise RuntimeError("Session not initialized")
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            return await resp.json(content_type=None)

    # ------------------------------------------------------------------
    # Poll
    # ------------------------------------------------------------------

    async def poll(self) -> AsyncIterator[Event]:
        """Fetch the configured URL and yield new Events."""
        self.sweep_old_ids()

        s = self._settings
        try:
            raw = await self._fetch(s.url)
        except Exception as exc:
            logger.error(
                "generic_http fetch failed",
                extra={"adapter": self.name, "url": s.url, "error": str(exc)},
            )
            raise

        items = _dig(raw, s.items_path)
        if not isinstance(items, list):
            logger.warning(
                "generic_http items_path did not resolve to a list",
                extra={
                    "adapter": self.name,
                    "items_path": s.items_path,
                    "got": type(items).__name__,
                },
            )
            return

        new_count = 0
        for item in items:
            event = self._item_to_event(item)
            if event is None:
                continue
            if self.is_published(event.id):
                continue
            yield event
            self.mark_published(event.id)
            new_count += 1

        logger.info(
            "generic_http poll completed",
            extra={"adapter": self.name, "count": new_count},
        )

    # ------------------------------------------------------------------
    # Item → Event
    # ------------------------------------------------------------------

    def _item_to_event(self, item: Any) -> Event | None:
        """Convert a single source item to an Event, or None to skip."""
        s = self._settings

        # --- id (required) ---
        raw_id = _dig(item, s.id_path)
        if raw_id is None:
            logger.warning(
                "generic_http item missing id",
                extra={"adapter": self.name, "id_path": s.id_path},
            )
            return None
        event_id = str(raw_id)

        # --- category ---
        category = f"{s.domain}.{s.category_suffix}"

        # --- time ---
        if s.time_path:
            raw_time = _dig(item, s.time_path)
            if raw_time is not None:
                try:
                    # Python 3.11+ fromisoformat handles "Z"; older needs replace.
                    event_time = datetime.fromisoformat(
                        str(raw_time).replace("Z", "+00:00")
                    )
                    if event_time.tzinfo is None:
                        event_time = event_time.replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    logger.warning(
                        "generic_http could not parse time; using now",
                        extra={
                            "adapter": self.name,
                            "time_path": s.time_path,
                            "raw": raw_time,
                        },
                    )
                    event_time = datetime.now(timezone.utc)
            else:
                event_time = datetime.now(timezone.utc)
        else:
            event_time = datetime.now(timezone.utc)

        # --- geo ---
        geo_kwargs: dict[str, Any] = {}

        # Try GeoJSON geometry first.
        if s.geometry_path:
            geom = _dig(item, s.geometry_path)
            if isinstance(geom, dict):
                geo_kwargs["geometry"] = geom
                # Also set centroid when geometry is a simple Point.
                if geom.get("type") == "Point":
                    coords = geom.get("coordinates") or []
                    if len(coords) >= 2:
                        try:
                            geo_kwargs["centroid"] = (
                                float(coords[0]),
                                float(coords[1]),
                            )
                        except (TypeError, ValueError):
                            pass

        # Fall back to explicit lat/lon paths.
        if "geometry" not in geo_kwargs and s.lat_path and s.lon_path:
            raw_lat = _dig(item, s.lat_path)
            raw_lon = _dig(item, s.lon_path)
            if raw_lat is not None and raw_lon is not None:
                try:
                    geo_kwargs["centroid"] = (float(raw_lon), float(raw_lat))
                except (TypeError, ValueError):
                    pass

        geo = Geo(**geo_kwargs)

        # --- data ---
        data: dict[str, Any] = {}
        if s.title_path:
            title = _dig(item, s.title_path)
            if title is not None:
                data["title"] = title

        for fm in s.field_mappings:
            data[fm.dest_key] = _dig(item, fm.source_path)

        # --- severity ---
        severity: int | None = None
        if s.severity_path:
            raw_sev = _dig(item, s.severity_path)
            if raw_sev is not None:
                try:
                    severity = int(raw_sev)
                except (TypeError, ValueError):
                    pass

        return Event(
            id=event_id,
            adapter=self.name,
            category=category,
            time=event_time,
            geo=geo,
            data=data,
            severity=severity,
        )

    # ------------------------------------------------------------------
    # Subject routing
    # ------------------------------------------------------------------

    def subject_for(self, event: Event) -> str:
        """Return the NATS subject for a generic-http event.

        Pattern: ``central.<domain>.<region>``

        Region is derived from ``event.data["_enriched"]["geocoder"]``
        identically to usgs_quake — the Photon enrichment pipeline populates
        that key at publish time.  Returns "unknown" when enrichment hasn't
        run yet or found no geo signal.
        """
        region = subject_for_region(event.data)
        return f"central.{self._settings.domain}.{region}"
