-- Migration: 028_add_tomtom_incidents_adapter
-- Adds the tomtom_incidents adapter onto the EXISTING CENTRAL_TRAFFIC stream
-- (central.traffic.incident.<state>). No new stream -> no central-archive restart.
-- Reuses the existing "tomtom" api key. Ships disabled; operator enables via GUI.
-- NOTE: TomTom incidentDetails rejects any bbox > 10,000 km^2, so coverage is
-- per-metro bboxes (Treasure Valley here), NOT statewide. Expansion = more bbox
-- rows, but mind the 2,500/mo free-tier cap: N_bboxes * (43200/cadence_min) <= 2500.
-- Additive-only: idempotent via ON CONFLICT DO NOTHING.

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'tomtom_incidents',
    false,
    1800,
    '{"api_key_alias": "tomtom", "bboxes": [{"name": "treasure_valley", "min_lon": -116.85, "min_lat": 43.30, "max_lon": -115.65, "max_lat": 44.10, "state_code": "ID"}]}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
