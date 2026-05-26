-- Migration: 029_add_state_511_atis_cameras_adapter
-- Adds the CENTRAL_TRAFFIC_CAMERAS JetStream stream (telemetry; central.traffic_cameras.>)
-- AND the state_511_atis_cameras adapter row. NEW event-bearing stream ->
-- central-archive restart required at deploy (feedback_new_stream_needs_archive_restart).
-- 7-day retention. Ships disabled; public-unauth (no api key). Idaho only.
-- Additive-only: idempotent via ON CONFLICT DO NOTHING.

INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_TRAFFIC_CAMERAS', 604800, 1073741824)
ON CONFLICT (name) DO NOTHING;

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'state_511_atis_cameras',
    false,
    600,
    '{"states": [{"code": "ID", "base_url": "https://511.idaho.gov"}]}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
