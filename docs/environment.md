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
