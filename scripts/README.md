# scripts/

## deploy.sh — tag-based deploy for central on CT 104

Codifies the manual deploy procedure for the `central` service running on
Proxmox CT 104 (`utility`, Tailscale `100.64.0.12`).

### What it does

1. **Preflight** — checks that `/opt/central` is a git repo, captures the
   currently-deployed ref for rollback messaging, reports unit status (warns but
   does not abort on inactive units), runs `central-migrate --check` to gate
   on migration drift, and takes a pre-deploy `pg_dump` backup.
2. **Deploy** — fetches from `origin`, verifies the requested ref exists,
   checks it out as a detached HEAD, and runs `uv sync` to update the venv
   against the checked-out `uv.lock`.
3. **Confirm** — shows a `--dry-run` migration preview and prompts for
   confirmation before applying any changes to the running system (skip with
   `-y`).
4. **Apply** — runs `central-migrate` to apply pending SQL migrations, then
   restarts all three systemd units (`central-supervisor`, `central-archive`,
   `central-gui`).
5. **Verify** — confirms all units are active, re-runs `central-migrate
   --check` for a clean post-deploy state, and polls `http://localhost:8000/health`
   (up to 5 retries, 2 s apart) for an HTTP 200.
6. **ERR trap** — on any unexpected failure, prints a ROLLBACK block with
   the exact commands to re-checkout the previous ref, re-sync the venv,
   restart services, and (if needed) restore from the pre-deploy dump.

### Usage

Run on CT 104 as a user with passwordless `sudo` (e.g. `zvx`):

```
/opt/central/scripts/deploy.sh <tag-or-ref> [-y|--yes]
```

- `<tag-or-ref>` — any Git tag, branch, or commit SHA (tags are the standard
  deploy unit; e.g. `v0.14.5`).
- `-y` / `--yes` — skip the interactive confirmation prompt (safe for
  automation once you have reviewed the dry-run output manually).

### Pre-flight backup

Before applying any changes, the script takes a `pg_dump -Fc` of the
`central` database and writes it to `/var/backups/central/`. The 10 newest
dumps are retained; older ones are pruned automatically.

**Migrations are forward-only.** There are no down-scripts. The `pg_dump` is
the only automated mechanism for rolling back the database. If you need to
revert after migrations have run, restore from the dump printed in the SUCCESS
(or ERR-trap) output.

### One-time cutover steps

Some releases require manual cutover steps that cannot be automated (e.g.
removing a deprecated EONET region key from `config.adapters`). These are
intentionally out of scope for this script. See the vault runbook
`central-deploy-cutover.md` for guidance on release-specific procedures.

### Bootstrap caveat

This script is version-controlled inside the `central` repository. The very
first deploy that introduces it must still be performed manually (the script
ships in the repo it deploys and cannot deploy itself from scratch).
