-- v0.10.3: rip out state_511_atis + state_511_atis_cameras (Castle Rock legacy
-- shape EOL; superseded by itd_511 + itd_511_cameras from migration 031 / v0.10.0).
--
-- v0.10.3.1: Superseded by 033_soft_disable_state_511_atis_adapters.sql due to
-- the FK constraint events_adapter_fkey ON DELETE RESTRICT. Historical events
-- preserve referential integrity; the rows remain as soft-deleted tombstones
-- (enabled=false + paused_at=NOW()). This file is preserved as part of the
-- append-only migration ledger; running it on a fresh database still throws
-- the same FK error if any state_511_atis* events exist.
--
-- Idempotent: the DELETE succeeds whether the rows are present or not. Historical
-- events in public.events stay (preserved as historical record per Matt's call);
-- only the config.adapters rows that would otherwise be hot-reloaded into the
-- supervisor are removed.
--
-- Note: cursors.db cleanup (published_ids for both adapters) is a SQLite-side
-- step handled at deploy time, NOT in this Postgres migration.

DELETE FROM config.adapters
WHERE name IN ('state_511_atis', 'state_511_atis_cameras');
