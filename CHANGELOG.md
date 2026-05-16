# Changelog

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
