-- Migration 038: register satpass_predict adapter row (v0.11.1)
--
-- Server-side complement to meshAI's per-user client-side pass computation.
-- Reads the latest TLE per norad_id from the events table (celestrak_tle
-- adapter, v0.11.0) and emits one Event per (observer, satellite, AOS)
-- tuple within a 24h horizon. Publishes on the existing CENTRAL_SAT
-- stream via the supervisor's STREAM_CATEGORY_DOMAINS extension
-- ("CENTRAL_SAT": ("tle", "pass")) -- no new stream is needed and
-- migration does NOT touch config.streams.
--
-- Ships disabled (`enabled=false`) -- operator-configures observers via
-- GUI, then enables. Default settings include one Treasure Valley observer
-- as a worked example operators can edit/extend in place.
--
-- Cadence 3600s (1h): the adapter recomputes the 24h horizon every hour.
-- New TLEs landing between polls are naturally picked up at the next poll.
--
-- Idempotent: ON CONFLICT (name) DO NOTHING preserves any operator-tuned
-- state (settings changed by hand, enabled flag flipped, cadence override).

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'satpass_predict',
    false,
    3600,
    '{
      "observers": [
        {"name": "Treasure Valley", "slug": "treasure-valley",
         "state": "ID", "lat": 43.6, "lon": -116.2, "elev_m": 0}
      ],
      "min_elevation_deg": 10,
      "horizon_hours": 24
    }'::jsonb
)
ON CONFLICT (name) DO NOTHING;
