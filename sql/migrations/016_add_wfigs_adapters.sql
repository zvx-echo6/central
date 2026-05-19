-- Migration: 016_add_wfigs_adapters
-- Add WFIGS incident and perimeter adapters to config.adapters
-- Idempotent: uses ON CONFLICT DO NOTHING

-- WFIGS Incidents adapter
INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'wfigs_incidents',
    false,  -- Ships disabled; operator enables via GUI
    300,
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

-- WFIGS Perimeters adapter
INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'wfigs_perimeters',
    false,  -- Ships disabled; operator enables via GUI
    300,
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
