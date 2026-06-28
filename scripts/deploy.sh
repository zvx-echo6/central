#!/usr/bin/env bash
# deploy.sh — tag-based deploy for the central service on CT 104
#
# Usage: deploy.sh <tag-or-ref> [-y|--yes]
#
# Must run on CT 104 as a sudo-capable user (e.g. zvx).
# Performs a detached-HEAD checkout of <tag-or-ref>, syncs the uv venv,
# runs a pre-flight migration drift check, takes a pg_dump backup, runs
# migrations, restarts all three systemd units, and verifies health.
#
# One-time cutover steps (e.g. EONET region-key removal) are NOT handled
# here — see the vault runbook central-deploy-cutover.md.
#
# Bootstrap caveat: the very first deploy that introduces this script is
# still manual (the script ships inside the repo it deploys).

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEPLOY_DIR=/opt/central
VENV="$DEPLOY_DIR/.venv"
CENTRAL_USER=central
UV=/usr/local/bin/uv
BACKUP_DIR=/var/backups/central
UNITS=(central-supervisor central-archive central-gui)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() {
    echo ""
    echo "==> $*"
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

# Run a command as $CENTRAL_USER via sudo.
cen() {
    sudo -u "$CENTRAL_USER" "$@"
}

# ---------------------------------------------------------------------------
# ERR trap — fired on any unhandled non-zero exit inside set -e
# ---------------------------------------------------------------------------
on_error() {
    local exit_code=$?
    echo "" >&2
    echo "================================================================" >&2
    echo "  DEPLOY FAILED (exit $exit_code)" >&2
    echo "================================================================" >&2
    echo "" >&2
    echo "  ROLLBACK GUIDE:" >&2
    echo "" >&2
    echo "  1. Checkout the previously-deployed ref:" >&2
    echo "     sudo -u $CENTRAL_USER git -C $DEPLOY_DIR checkout ${PREV_REF:-<unknown>}" >&2
    echo "     sudo -u $CENTRAL_USER bash -c \"cd $DEPLOY_DIR && $UV sync\"" >&2
    echo "     sudo systemctl restart ${UNITS[*]}" >&2
    echo "" >&2
    echo "  2. DB rollback (migrations are forward-only, no down-scripts):" >&2
    echo "     The ONLY automated DB rollback is the pg_dump taken before deploy." >&2
    if [[ -n "${DUMP:-}" ]]; then
        echo "     Dump path: $DUMP" >&2
        echo "     Restore:   sudo -u $CENTRAL_USER pg_restore -d central -Fc --clean '$DUMP'" >&2
    else
        echo "     (Dump was not yet created — no DB changes were made.)" >&2
    fi
    echo "" >&2
    echo "================================================================" >&2
}

trap on_error ERR

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
REF=""
SKIP_CONFIRM=0

for arg in "$@"; do
    case "$arg" in
        -y|--yes)
            SKIP_CONFIRM=1
            ;;
        -h|--help)
            echo "Usage: $0 <tag-or-ref> [-y|--yes]"
            echo "  <tag-or-ref>   Git tag, branch, or commit to deploy."
            echo "  -y / --yes     Skip interactive confirmation prompt."
            exit 0
            ;;
        -*)
            die "Unknown option: $arg"
            ;;
        *)
            if [[ -z "$REF" ]]; then
                REF="$arg"
            else
                die "Unexpected argument: $arg"
            fi
            ;;
    esac
done

if [[ -z "$REF" ]]; then
    echo "Usage: $0 <tag-or-ref> [-y|--yes]" >&2
    exit 2
fi

# ---------------------------------------------------------------------------
# PREFLIGHT
# ---------------------------------------------------------------------------

log "PREFLIGHT"

# 1. Verify DEPLOY_DIR is a git repo.
[[ -d "$DEPLOY_DIR/.git" ]] \
    || die "$DEPLOY_DIR/.git not found — is this the right deploy directory?"

# 2. Capture current ref for rollback messaging.
PREV_REF="$(cen git -C "$DEPLOY_DIR" describe --tags --always 2>/dev/null || echo unknown)"
log "Currently deployed: $PREV_REF  →  deploying: $REF"

# 3. Report unit status (warn, don't die — a redeploy may be fixing unhealthy units).
log "Current service status"
for unit in "${UNITS[@]}"; do
    status="$(systemctl is-active "$unit" 2>/dev/null || true)"
    if [[ "$status" != "active" ]]; then
        echo "  WARNING: $unit is $status (will attempt restart anyway)"
    else
        echo "  $unit: $status"
    fi
done

# 4. Migration drift gate — refuse to deploy onto a drifted migration state.
log "Migration drift check (--check)"
cen "$VENV/bin/central-migrate" --check \
    || die "Migration drift detected (central-migrate --check exited non-zero). Resolve drift before deploying."

# 5. Ensure backup directory exists and is owned by $CENTRAL_USER.
log "Ensuring backup directory: $BACKUP_DIR"
sudo mkdir -p "$BACKUP_DIR"
sudo chown "$CENTRAL_USER:$CENTRAL_USER" "$BACKUP_DIR"

# 6. Take a pre-deploy pg_dump backup.
TS="$(date -u +%Y%m%dT%H%M%SZ)"
# Sanitise REF for use in a filename (replace / and : with _).
REF_SAFE="${REF//\//_}"
REF_SAFE="${REF_SAFE//:/_}"
DUMP="$BACKUP_DIR/central-pre-${REF_SAFE}-${TS}.pgdump"

log "Taking pre-deploy backup: $DUMP"
# Run as $CENTRAL_USER so the file is owned by that user; redirect inside sudo.
cen bash -c "pg_dump -Fc central > '$DUMP'"

# Verify the dump is non-empty (>1 KB sanity check).
if [[ ! -f "$DUMP" ]]; then
    die "Dump file was not created: $DUMP"
fi
dump_size="$(stat -c%s "$DUMP" 2>/dev/null || stat -f%z "$DUMP" 2>/dev/null || echo 0)"
if [[ "$dump_size" -lt 1024 ]]; then
    die "Dump file is suspiciously small (${dump_size} bytes): $DUMP — aborting."
fi
echo "  Backup written: $DUMP (${dump_size} bytes)"

# 7. Prune old backups — keep the 10 newest central-pre-*.pgdump files.
log "Pruning old backups (keep 10 newest)"
# ls -t lists newest first; tail -n +11 skips the 10 newest → these are the old ones.
old_backups="$(ls -t "$BACKUP_DIR"/central-pre-*.pgdump 2>/dev/null | tail -n +11 || true)"
if [[ -n "$old_backups" ]]; then
    echo "$old_backups" | while IFS= read -r f; do
        echo "  Removing old backup: $f"
        rm -f "$f"
    done
else
    echo "  No old backups to prune."
fi

# ---------------------------------------------------------------------------
# DEPLOY
# ---------------------------------------------------------------------------

log "DEPLOY"

# 8. Fetch latest tags and refs from origin.
log "Fetching from origin (tags)"
cen git -C "$DEPLOY_DIR" fetch origin --tags

# 9. Verify the requested ref resolves to a commit (fail fast with a clear message).
log "Resolving ref: $REF"
cen git -C "$DEPLOY_DIR" rev-parse --verify "${REF}^{commit}" > /dev/null \
    || die "Ref '$REF' does not resolve to a commit. Check the tag/branch name and try again."

# 10. Checkout the ref as a detached HEAD (standard deploy mode).
log "Checking out: $REF"
cen git -C "$DEPLOY_DIR" checkout "$REF"

# 11. Sync the venv against the checked-out lockfile (uv respects uv.lock).
log "Syncing venv (uv sync)"
cen bash -c "cd '$DEPLOY_DIR' && '$UV' sync"

# 12. Migration preview — show what would run without applying.
log "Migration dry-run (preview)"
cen "$VENV/bin/central-migrate" --dry-run

# 13. CONFIRM gate — unless -y was passed.
#
# NOTE: At this point the code and venv are already updated to the new ref,
# but services have NOT been restarted and migrations have NOT been applied.
# If the operator aborts here, the new code is staged on disk but production
# is still running the old code. To return to a clean state manually:
#   sudo -u central git -C /opt/central checkout <previous-ref>
#   sudo -u central bash -c "cd /opt/central && /usr/local/bin/uv sync"
if [[ "$SKIP_CONFIRM" -eq 0 ]]; then
    echo ""
    read -r -p "Apply migrations and restart services? [y/N] " confirm < /dev/tty
    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
        echo ""
        echo "Aborted by operator."
        echo ""
        echo "  Code and venv are now at: $REF"
        echo "  Services are still running: $PREV_REF"
        echo "  Migrations have NOT been applied."
        echo ""
        echo "  To stage-abort cleanly (revert code on disk):"
        echo "    sudo -u $CENTRAL_USER git -C $DEPLOY_DIR checkout $PREV_REF"
        echo "    sudo -u $CENTRAL_USER bash -c \"cd $DEPLOY_DIR && $UV sync\""
        exit 0
    fi
fi

# 14. Apply migrations.
log "Applying migrations"
cen "$VENV/bin/central-migrate"

# 15. Restart all three systemd units.
log "Restarting services"
sudo systemctl restart "${UNITS[@]}"

# ---------------------------------------------------------------------------
# VERIFY
# ---------------------------------------------------------------------------

log "VERIFY"

# 16. Confirm all units are active after restart.
log "Checking unit status"
failed_units=()
for unit in "${UNITS[@]}"; do
    status="$(systemctl is-active "$unit" 2>/dev/null || true)"
    echo "  $unit: $status"
    if [[ "$status" != "active" ]]; then
        failed_units+=("$unit")
    fi
done
if [[ "${#failed_units[@]}" -gt 0 ]]; then
    die "Unit(s) not active after restart: ${failed_units[*]}"
fi

# 17. Post-deploy migration check — should be clean (0 pending).
log "Post-deploy migration check (--check)"
cen "$VENV/bin/central-migrate" --check \
    || die "Post-deploy migration check failed — schema may be inconsistent."

# 18. Health check — up to 5 retries with 2-second sleep between attempts.
log "Health check: http://localhost:8000/health"
HEALTH_OK=0
for attempt in 1 2 3 4 5; do
    http_status="$(curl -sf -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null || true)"
    if [[ "$http_status" == "200" ]]; then
        echo "  Attempt $attempt: HTTP $http_status — OK"
        HEALTH_OK=1
        break
    else
        echo "  Attempt $attempt: HTTP ${http_status:-no-response} — retrying in 2s..."
        sleep 2
    fi
done

if [[ "$HEALTH_OK" -eq 0 ]]; then
    die "Health check failed after 5 attempts (http://localhost:8000/health did not return 200)."
fi

# ---------------------------------------------------------------------------
# SUCCESS
# ---------------------------------------------------------------------------
DEPLOYED_DESC="$(cen git -C "$DEPLOY_DIR" describe --tags --always 2>/dev/null || echo "$REF")"

echo ""
echo "================================================================"
echo "  SUCCESS"
echo "================================================================"
echo "  Deployed:     $DEPLOYED_DESC"
echo "  Previous:     $PREV_REF"
echo "  Pre-deploy backup: $DUMP"
echo ""
echo "  REMINDER: one-time cutover steps (e.g. EONET region-key removal)"
echo "  are NOT performed by this script. See vault runbook:"
echo "  central-deploy-cutover.md"
echo "================================================================"
