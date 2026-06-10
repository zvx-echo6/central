-- Migration 041: register sat_orbits adapter (v0.13.0)
--
-- Forward-orbit-track publisher. One LineString telemetry event per tracked
-- satellite per poll, projecting the next 90 minutes of sub-satellite
-- ground track. Line counterpart to sat_positions (which publishes the
-- current sub-sat POINT per minute). Drives the "each sat's path" map
-- view Matt asked for after enabling the satellite family.
--
-- Subject: central.sat.orbit.<norad_id>. Stream: existing CENTRAL_SAT.
-- The supervisor's STREAM_CATEGORY_DOMAINS["CENTRAL_SAT"] extends from
-- ("tle", "pass", "position") to ("tle", "pass", "position", "orbit")
-- in code (not migration) so the retention sweep covers orbit events.
--
-- No max_bytes bump needed on CENTRAL_SAT. Volume estimate: 6 sats x 12
-- polls/hour (300s cadence) x 24 hours = 1728 events/day at ~5 KB each
-- (~90 vertices) = ~8.5 MB/day. Negligible against the 5 GiB cap from
-- v0.12.0.
--
-- Ships disabled (enabled=false). celestrak_tle must be enabled and
-- polling before sat_orbits has TLE data to propagate; missing-TLE path
-- is graceful (INFO log + zero events).
--
-- Idempotent: ON CONFLICT preserves operator-tuned state.

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'sat_orbits',
    false,
    300,
    '{
      "track_only_norad_ids": [],
      "forward_minutes": 90,
      "sample_seconds": 60,
      "max_tle_age_days": 14
    }'::jsonb
)
ON CONFLICT (name) DO NOTHING;
