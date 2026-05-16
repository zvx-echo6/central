-- Migration: 004_nws_states_to_bbox
-- Converts NWS adapter settings from states list to region bbox.
-- Bbox covers ID/OR/WA/MT/WY/UT/NV with buffer.

UPDATE config.adapters
SET settings = jsonb_set(
    settings - 'states',  -- Remove states key
    '{region}',
    '{"north": 49.5, "south": 31.0, "east": -102.0, "west": -124.5}'::jsonb
)
WHERE name = 'nws';
