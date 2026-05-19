-- Migration: 020_add_gdacs_adapter
-- Adds the GDACS adapter row to config.adapters.
-- Ships disabled; operator enables via GUI.
-- Default event_types excludes EQ (USGS is canonical for earthquakes on central.quake.>).
-- Idempotent: uses ON CONFLICT DO NOTHING.

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'gdacs',
    false,
    600,
    jsonb_build_object('event_types', jsonb_build_array('WF', 'DR', 'FL', 'VO', 'TC'))
)
ON CONFLICT (name) DO NOTHING;
