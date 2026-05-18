# Changelog

## v0.3.0 — Phase 1b (2026-05-18)

Operator console. FastAPI + Jinja2 + Pico + HTMX. Self-hosted,
Tailscale-gated by default, no application-level auth beyond
the operator session.

### Added
- Operator console (`central-gui` systemd service on port 8000)
- Login + session auth (argon2id, 90-day DB-backed sessions)
- Dashboard: events 24h by adapter, stream sizes,
  last-poll-time per adapter
- Adapters list and edit page (cadence + per-adapter settings),
  with Leaflet region picker and click-to-draw rectangles
- Streams view with retention chips (1d / 7d / 14d / 30d /
  365d / custom)
- API keys management (list / add / rotate / delete,
  encrypted at rest via `crypto.encrypt`, plaintext never
  logged or stored)
- First-run wizard (5 steps: operator, system, keys, adapters,
  finish) with deferred-commit pattern — no DB writes until
  Finish runs as a single transaction
- Events feed page (`/events`) — paginated, filterable by
  adapter / category / time range / map viewport, with
  color-coded geometry overlay, click-to-popup, and
  expandable row details showing full event payload
- Paginated events JSON API (`/events.json`) — cursor-based
  pagination, same filter surface as the HTML feed

### Changed
- CSRF tokens are now session-bound (synchronizer token
  pattern), replacing the previous fastapi-csrf-protect
  library. Eliminates a rotation race that broke first-load
  submissions
- First-run wizard is a single atomic transaction at Finish,
  not per-step DB writes. Back navigation works; abandoned
  wizards leave no orphan rows

### Fixed
- Adapter editor's JSONB double-encoding bug (write path
  called `json.dumps` before asyncpg's codec, corrupting
  the settings column)
- Dashboard polls card was reading from the wrong NATS
  subject and using a durable consumer instead of
  `get_last_msg`, leaking zombie consumers
- Browser-noise paths (/favicon.ico, /apple-touch-icon.png,
  /robots.txt) return 204 directly, preventing parallel
  requests from racing the CSRF cookie on first page load
- SubResource Integrity hashes for leaflet-draw assets
  corrected (previous values were fabricated and silently
  blocked by browsers)

### Infrastructure
- New `config.sessions` column: `csrf_token` (per-session
  synchronizer)
- Composite index on `public.events (time DESC, id DESC)`
  for cursor pagination
- `central-gui` systemd service

## v0.2.0 — Phase 1a (2026-05-16)

Three live data sources, configurable infrastructure, hot-reload
everywhere.

### Added
- FIRMS fire hotspot adapter (VIIRS_SNPP_NRT + VIIRS_NOAA20_NRT)
- USGS earthquake adapter (GeoJSON feed)
- Operational config in Postgres `config` schema
  (adapters, api_keys, streams) with LISTEN/NOTIFY hot-reload
- Per-stream retention management (config.streams)
  with supervisor-managed max_bytes auto-tuning
- Unified region selection (bbox) across all adapters
- Adapter registry pattern for one-line addition of new sources
- `apply_config()` per-adapter for generic hot-reload of settings

### Changed
- NWS adapter migrated from `states` list to bbox region filter
- Polygon-vs-bbox intersection (shapely) replaces centroid filter
- CloudEvents protocol constants moved from operational config
  to code constants
- Operational config retired from `central.toml` to Postgres
  (`config.adapters`, `config.api_keys`); TOML removed

### Fixed
- Cadence-decrease hot-reload (AsyncLimiter inside NWS adapter
  removed; supervisor is single owner of cadence)
- `last_completed_poll` preserved across adapter enable/disable
  cycles to honor the rate-limit guarantee

### Infrastructure
- CENTRAL_FIRE stream (7d default retention)
- CENTRAL_QUAKE stream (7d default retention)
