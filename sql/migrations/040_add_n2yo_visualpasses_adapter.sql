-- Migration 040: register n2yo_visualpasses adapter (v0.12.1)
--
-- Server-side complement to v0.11.1 satpass_predict. n2yo's visualpasses
-- endpoint adds sun illumination + visual magnitude that SGP4-from-TLE
-- alone cannot compute. Subject collision with satpass_predict on
-- central.sat.pass.us.<state>.<observer_slug> is intentional; consumers
-- disambiguate via data.category (pass.n2yo_visualpasses vs
-- pass.satpass_predict). v0.10.8 category-discriminated Nats-Msg-Id keeps
-- the JetStream dedup windows distinct.
--
-- No stream changes: CENTRAL_SAT already routes via the "pass" token in
-- STREAM_CATEGORY_DOMAINS["CENTRAL_SAT"] = ("tle", "pass", "position").
--
-- No api_keys seeding: Matt adds the "n2yo" alias via the GUI /api-keys
-- page (Add -> alias "n2yo" -> paste key) before enabling the adapter.
-- Missing-key behavior is graceful (log INFO + zero-yield, no exception),
-- so the row can land in config.adapters before the key does without
-- breaking anything.
--
-- Ships disabled. Default 6 observers x 6 sats x 24 polls/day = 864
-- transactions/day, under n2yo's free 1000/day quota cap.
--
-- Idempotent: ON CONFLICT (name) DO NOTHING preserves operator-tuned state.

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'n2yo_visualpasses',
    false,
    3600,
    '{
      "observers": [
        {"name": "Filer",           "slug": "filer",           "state": "ID", "lat": 42.57, "lon": -114.60, "elev_m": 1200},
        {"name": "Boise",           "slug": "boise",           "state": "ID", "lat": 43.62, "lon": -116.20, "elev_m": 825},
        {"name": "Idaho Falls",     "slug": "idaho-falls",     "state": "ID", "lat": 43.49, "lon": -112.04, "elev_m": 1438},
        {"name": "Ogden",           "slug": "ogden",           "state": "UT", "lat": 41.22, "lon": -111.97, "elev_m": 1330},
        {"name": "Salt Lake City",  "slug": "salt-lake-city",  "state": "UT", "lat": 40.76, "lon": -111.89, "elev_m": 1290},
        {"name": "Provo",           "slug": "provo",           "state": "UT", "lat": 40.23, "lon": -111.66, "elev_m": 1387}
      ],
      "norad_ids": [25544, 25338, 28654, 33591, 27607, 43017],
      "days_ahead": 2,
      "min_visibility_seconds": 300,
      "api_key_alias": "n2yo"
    }'::jsonb
)
ON CONFLICT (name) DO NOTHING;
