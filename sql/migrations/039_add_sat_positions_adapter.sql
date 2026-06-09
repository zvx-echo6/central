-- Migration 039: register sat_positions adapter + bump CENTRAL_SAT cap (v0.12.0)
--
-- Live global satellite-position publisher: one telemetry event per tracked
-- NORAD ID per poll on subject central.sat.position.<norad_id>. Complement
-- to satpass_predict (observer-anchored alerts -- v0.11.1): sat_positions
-- is the *global* counterpart, "where is sat X right now" rather than
-- "when is sat X overhead at observer Y".
--
-- Publishes on the existing CENTRAL_SAT stream via the supervisor's
-- STREAM_CATEGORY_DOMAINS extension ("CENTRAL_SAT": ("tle", "pass",
-- "position")) -- no new stream is created.
--
-- Cadence 60s (1 minute) by default. LEO sub-satellite points drift
-- visibly at minute scale (~7.7 km/s * 60s = 462 km of ground track), so
-- 60s ticks give a watchable live map. GEO sats barely move at minute
-- scale, but cadence is operator-tunable per-adapter -- if a future
-- operator pin-list contains only GEO sats they can drop cadence to
-- 300s with no code change.
--
-- Ships disabled (enabled=false) -- celestrak_tle must be enabled and
-- have polled at least once for sat_positions to have TLE data to
-- propagate. Operator enables via GUI after celestrak_tle is producing.
--
-- Settings shape:
--   track_only_norad_ids: empty list = track every NORAD ID with a fresh
--     TLE in the events table (default behavior, derive-from-celestrak_tle).
--     Operator-pinned non-empty list restricts the set.
--   max_tle_age_days: 14 = TLEs older than 14d are skipped to keep
--     SGP4 propagation drift bounded. Operator can tighten (e.g. 3d) for
--     drag-sensitive LEO accuracy or widen for stale-tolerant feeds.
--
-- CENTRAL_SAT max_bytes bump from 1 GiB -> 5 GiB. Sizing rationale:
-- celestrak_tle alone produced ~190 sats * 1 envelope/day ~= 190 events/day
-- ~= ~1.4k events/week; that fit in 1 GiB easily.
-- sat_positions adds ~190 sats * 1440 ticks/day (60s cadence)
-- ~= 273,600 events/day ~= 1.92M events/week. At ~1 KB per envelope
-- including CloudEvents wrapper, that's ~1.9 GiB/week. Plus the existing
-- TLE + pass envelopes the stream already carries: ~3 GiB headroom needed.
-- 5 GiB (5368709120 bytes) gives operator-tunable margin without
-- over-provisioning.
--
-- Idempotent: ON CONFLICT clauses preserve any operator-tuned state.

UPDATE config.streams
   SET max_bytes = 5368709120
 WHERE name = 'CENTRAL_SAT';

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'sat_positions',
    false,
    60,
    '{
      "track_only_norad_ids": [],
      "max_tle_age_days": 14
    }'::jsonb
)
ON CONFLICT (name) DO NOTHING;
