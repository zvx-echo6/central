-- Migration: 017_add_inciweb_adapter
-- Add InciWeb adapter to config.adapters
-- Idempotent: uses ON CONFLICT DO NOTHING

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'inciweb',
    false,  -- Ships disabled; operator enables via GUI
    600,
    jsonb_build_object(
        'region', jsonb_build_object(
            'north', 49.0,
            'south', 31.0,
            'east', -102.0,
            'west', -124.0
        )
    )
)
ON CONFLICT (name) DO NOTHING;
