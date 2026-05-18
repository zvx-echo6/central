# Central Data Hub - Environment Reference

## Development Locations

### Active Development: CT104 (Central LXC)

All development work happens on the Central LXC container:

| Property | Value |
|----------|-------|
| **Hostname** | `central` |
| **Tailscale IP** | `100.64.0.12` |
| **LAN IP** | `192.168.1.104` |
| **SSH access** | `zvx@central` or `zvx@100.64.0.12` |
| **Repository path** | `/opt/central` |
| **Python venv** | `/opt/central/.venv` |
| **Services** | `central-supervisor`, `central-archive` |

### Parked Clone: Cortex

The cortex VM at `/home/zvx/projects/central` contains a clone that is
**not actively used for development**. It may be retired in the future.
Do not make changes there.

### Local Workstation: matt-desktop

The Windows workstation (matt-desktop) has no Central repository clones.
The directory `C:\Users\mtthw\central_work\` is scratch space only and
should not be used for commits.


## Network and Service Bindings

### Bind Address

`central-gui` binds to `0.0.0.0` by design. Network gating is the
operator's responsibility (firewall, Tailscale, etc.), not the app's.
Do not switch to `127.0.0.1` or to a specific interface — operators
choose their bind via whatever network they want to expose the service on.

### NATS Listener Ports

The default `nats-server.conf` listens on more than just :4222:

| Port | Protocol | Used by Central? |
|------|----------|------------------|
| 4222 | NATS client | Yes (all) |
| 8080 | WebSocket | No (Phase 0 leftover) |
| 8222 | HTTP monitoring | No (manual ops only) |
| 1883 | MQTT | No (Phase 0 leftover) |

None of the unused ports cause active harm — they listen but no consumer
connects. Operators can remove them from `nats-server.conf` if they want
a tighter footprint. Documenting so future contributors don't grep for
"MQTT integration" and come up confused.

## Repository

| Property | Value |
|----------|-------|
| **Origin** | `git@github.com:zvx-echo6/central.git` |
| **Main branch** | `main` |
| **Default user** | `central` (on CT104) |

## Services

### central-supervisor

The main adapter scheduler and event publisher. Polls upstream APIs,
normalizes events, and publishes to NATS JetStream.

```bash
# Status
systemctl status central-supervisor

# Logs
journalctl -u central-supervisor -f

# Restart (requires sudo)
sudo systemctl restart central-supervisor
```

### central-archive

Consumes events from NATS JetStream and archives to PostgreSQL/TimescaleDB.

```bash
# Status
systemctl status central-archive

# Logs
journalctl -u central-archive -f
```

## Database

## Environment Variables

Environment variables are stored in `/etc/central/central.env` and loaded by
systemd services via `EnvironmentFile=`.

| Variable | Required | Description |
|----------|----------|-------------|
| `CENTRAL_CSRF_SECRET` | Yes (for GUI) | Secret key for CSRF token signing. Generate with `python3 -c "import secrets; print(secrets.token_urlsafe(32))"` |

### Generating CSRF Secret

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

Add the generated value to `/etc/central/central.env`:

```bash
CENTRAL_CSRF_SECRET=<generated-secret>
```

Ensure the file has restricted permissions:

```bash
sudo chmod 640 /etc/central/central.env
sudo chown central:central /etc/central/central.env
```

PostgreSQL 16 with TimescaleDB runs on CT104:

```bash
# Connect as central user
psql -h localhost -U central -d central

# Check adapter config
SELECT name, cadence_s, enabled FROM config.adapters;

# Check recent events
SELECT id, time, category FROM events ORDER BY time DESC LIMIT 10;
```

## SSH Access from Windows

From matt-desktop, connect via Tailscale:

```bash
# Direct connection
ssh zvx@100.64.0.12

# Using hostname (if Tailscale DNS configured)
ssh zvx@central
```

Note: The `zvx` user requires password for sudo operations.
