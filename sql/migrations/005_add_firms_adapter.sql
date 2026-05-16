-- Migration: 005_add_firms_adapter
-- Seeds FIRMS adapter configuration and CENTRAL_FIRE stream.

-- Seed FIRMS adapter row
INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'firms',
    true,
    300,
    jsonb_build_object(
        'region', jsonb_build_object(
            'north', 49.5,
            'south', 31.0,
            'east', -102.0,
            'west', -124.5
        ),
        'api_key_alias', 'firms',
        'satellites', jsonb_build_array(
            'VIIRS_SNPP_NRT',
            'VIIRS_NOAA20_NRT'
        )
    )
);

-- Seed CENTRAL_FIRE stream row
INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_FIRE', 604800, 1073741824);
