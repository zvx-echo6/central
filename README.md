# Central

Central is the data hub spine for the Echo6 infrastructure. Adapters normalize upstream sources into a canonical event shape, publish CloudEvents to NATS/JetStream, and archive to TimescaleDB for historical query. Single-LXC deployment.

## Status

**Operational** (v0.14.6) — deployed on utility CT 104 (`central.echo6.mesh`).

~22 adapters live across traffic, wildfire, weather, space-weather, hydrology, earthquakes, avalanche, disasters, and satellite. CloudEvents published to NATS/JetStream, archived to TimescaleDB/PostGIS. FastAPI + HTMX GUI/API on :8000.

Three systemd units on a single LXC:
- `central-supervisor` — adapter lifecycle manager
- `central-archive` — NATS consumer persisting events to TimescaleDB
- `central-gui` — FastAPI + HTMX web interface / API (:8000)

## Architecture

- Python 3.12 (uv-managed)
- NATS + JetStream for live event bus
- TimescaleDB + PostGIS for archive and geospatial query
- One supervisor process managing adapter lifecycle
- One archive consumer process persisting events to TimescaleDB
- Both processes systemd-managed

## Testing

See [docs/test-database.md](docs/test-database.md) for test database setup.

## License

MIT. See LICENSE.
