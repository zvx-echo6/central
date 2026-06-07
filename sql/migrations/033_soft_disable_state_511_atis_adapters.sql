-- v0.10.3.1: soft-disable state_511_atis + state_511_atis_cameras (Castle Rock
-- legacy shape EOL; superseded by itd_511 + itd_511_cameras from migration 031).
--
-- The original v0.10.3 plan was a hard DELETE in 032, but events_adapter_fkey
-- (ON DELETE RESTRICT) blocked it: 66 state_511_atis events + 2379
-- state_511_atis_cameras events still reference the adapter rows. Matt's
-- explicit rule was "preserve historical events as a record", so the rows must
-- stay -- they just become soft-deleted tombstones.
--
-- Effect on the supervisor: list_enabled_adapters() filters on enabled=true,
-- so these rows fall out of the startup adapter set naturally. The
-- "Unknown adapter type" WARNING lines from v0.10.3's degraded deploy go away
-- on the next supervisor restart.
--
-- Idempotent: re-running this on already-disabled rows updates paused_at to
-- the current timestamp, which is harmless (the row was already disabled).
--
-- Note: cursors.db cleanup ran as part of the v0.10.3 deploy already; not
-- repeated here.

UPDATE config.adapters
SET enabled = false,
    paused_at = NOW()
WHERE name IN ('state_511_atis', 'state_511_atis_cameras');
