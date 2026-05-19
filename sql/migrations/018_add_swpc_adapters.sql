-- Migration: 018_add_swpc_adapters
-- Add NOAA SWPC space weather adapters to config.adapters.
-- All three ship disabled; operator enables individually via GUI.
-- Idempotent: uses ON CONFLICT DO NOTHING.

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES
    ('swpc_alerts',  false, 300, '{}'::jsonb),
    ('swpc_kindex',  false, 600, '{}'::jsonb),
    ('swpc_protons', false, 600, '{}'::jsonb)
ON CONFLICT (name) DO NOTHING;
