# Systemd Unit Files

These unit files configure Central services for systemd.

## Installation

```bash
# Copy unit files
sudo cp central-supervisor.service /etc/systemd/system/
sudo cp central-archive.service /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable and start services
sudo systemctl enable --now central-supervisor
sudo systemctl enable --now central-archive
```

## Configuration

Both services load environment variables from `/etc/central/central.env`:

```bash
CENTRAL_DB_DSN=postgresql://central:password@localhost/central
CENTRAL_NATS_URL=nats://localhost:4222
CENTRAL_CONFIG_SOURCE=db
CENTRAL_MASTER_KEY_PATH=/etc/central/master.key
```

## Service Dependencies

- **central-supervisor**: Requires NATS server
- **central-archive**: Requires NATS server and PostgreSQL

## Logs

```bash
journalctl -u central-supervisor -f
journalctl -u central-archive -f
```
