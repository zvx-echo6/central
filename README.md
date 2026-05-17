# Central

Central is the data hub spine for the infrastructure. Adapters normalize upstream sources into a canonical event shape, publish CloudEvents to NATS/JetStream, and archive to TimescaleDB for historical query. Single-LXC deployment.

## Status

Phase 0 — scaffold. Not yet operational.

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
