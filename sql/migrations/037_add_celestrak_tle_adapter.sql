-- Migration 037: seed CENTRAL_SAT stream + register celestrak_tle adapter (v0.11.0)
--
-- New top-level satellite-tracking domain. The adapter publishes TLEs
-- (orbital state) for satellites in configured CelesTrak groups
-- (defaults: stations, weather, amateur) so mesh consumers can compute
-- passes locally with their own observer geolocation. v0.11.1 (followup)
-- will add the satpass_predict adapter for fixed-observer pass alerts.
--
-- Stream config: 7-day retention, 1 GiB max_bytes -- mirrors
-- CENTRAL_FIRE / CENTRAL_QUAKE / CENTRAL_AVY defaults. TLE volume is
-- predictable (~75 sats × 4 polls/day = 300 events/day) so the cap is
-- generous; operator can tighten via /streams.
--
-- Adapter ships disabled (`enabled=false`) -- operator enables via GUI
-- after merge. Default settings groups = ["stations", "weather",
-- "amateur"]; extra_norad_ids empty.
--
-- Idempotent on both rows: ON CONFLICT DO NOTHING preserves any
-- operator-tuned state (e.g. settings or enabled flag changed by hand).

INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_SAT', 604800, 1073741824)
ON CONFLICT (name) DO NOTHING;

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'celestrak_tle',
    false,
    14400,
    '{"groups": ["stations", "weather", "amateur"], "extra_norad_ids": []}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
