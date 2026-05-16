-- Migration: 006_add_usgs_quake_adapter
-- Seeds USGS earthquake adapter configuration and CENTRAL_QUAKE stream.

-- Seed USGS quake adapter row
INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'usgs_quake',
    true,
    60,
    jsonb_build_object(
        'region', jsonb_build_object(
            'north', 49.5,
            'south', 31.0,
            'east', -102.0,
            'west', -124.5
        ),
        'feed', 'all_hour'
    )
);

-- Seed CENTRAL_QUAKE stream row (7d retention)
INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_QUAKE', 604800, 1073741824);
