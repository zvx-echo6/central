-- Migration 034: widen monitoring-area default to cover all of Idaho (v0.10.9)
--
-- v0.10.2's migration 030 set monitor_north=44.5 as the default. Idaho actually
-- extends to 49.0N (Canadian border), so the original default cut off the entire
-- panhandle -- Coeur d'Alene, Lewiston, Sandpoint, Moscow, and the McCall region.
-- Tonight's investigation (v0.10.9 ticket) caught the impact at scale: the
-- supervisor's publish-time bbox filter was dropping ~56 itd_511 events per
-- 60-second poll (~5,376/day), the entire northern half of Idaho's roadwork,
-- closures, and incidents. The NWS adapter was also losing Idaho UGC zone
-- alerts north of 44.5 for the same reason (v0.10.7 followup-ticket (b)).
--
-- This migration does two things:
--   1. ALTER COLUMN ... SET DEFAULT 49.0 -- so fresh installs get the full
--      Idaho extent baked in.
--   2. UPDATE ... WHERE monitor_north = 44.5 -- so existing deployments still
--      at the v0.10.2 dev value get bumped. The WHERE 44.5 guard preserves
--      any operator who deliberately narrowed (e.g. set north=44.0 for a
--      regional test) -- those rows are untouched.
--
-- Idempotent: running again is a no-op on both statements once row is at 49.0.

ALTER TABLE config.system ALTER COLUMN monitor_north SET DEFAULT 49.0;
UPDATE config.system SET monitor_north = 49.0 WHERE monitor_north = 44.5;
