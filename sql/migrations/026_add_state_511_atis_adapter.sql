-- Migration: 026_add_state_511_atis_adapter
-- Adds the state_511_atis adapter (Castle Rock ATIS 511) onto the EXISTING
-- CENTRAL_TRAFFIC stream. No new stream -> no central-archive restart needed
-- (see feedback_new_stream_needs_archive_restart). Ships disabled with the one
-- verified Idaho deployment; future verified Castle Rock states are settings rows.
-- Additive-only: idempotent via ON CONFLICT DO NOTHING.

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'state_511_atis',
    false,
    300,
    '{"states": [{"code": "ID", "base_url": "https://511.idaho.gov"}]}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
