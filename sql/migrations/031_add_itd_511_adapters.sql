-- Migration: 031_add_itd_511_adapters
-- Adds the itd_511 (event-class) and itd_511_cameras (telemetry-class) adapters
-- onto the EXISTING CENTRAL_TRAFFIC and CENTRAL_TRAFFIC_CAMERAS streams. NO
-- new streams -> no central-archive restart needed
-- (feedback_new_stream_needs_archive_restart). Both ship disabled; operator
-- enables via GUI after registering the 'idaho_511' API key
-- (python -m set_api_key, alias 'idaho_511').
-- itd_511 polls 60s (events) + 300s (advisories sub-poll); ~1.3 calls/min
-- combined, well under ITD's documented 10/60s budget.
-- Additive-only: idempotent via ON CONFLICT DO NOTHING.

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'itd_511',
    false,
    60,
    '{"api_key_alias": "idaho_511"}'::jsonb
)
ON CONFLICT (name) DO NOTHING;

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'itd_511_cameras',
    false,
    600,
    '{"api_key_alias": "idaho_511"}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
