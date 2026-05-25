-- Migration: 027_add_tomtom_flow_adapter_and_flow_stream
-- Adds the CENTRAL_TRAFFIC_FLOW JetStream stream (telemetry; central.traffic_flow.>,
-- non-overlapping with CENTRAL_TRAFFIC's central.traffic.>) AND the tomtom_flow
-- adapter row. NEW event-bearing stream -> central-archive restart required at deploy
-- (feedback_new_stream_needs_archive_restart). 7-day retention (high-volume telemetry).
-- Ships disabled; operator adds a "tomtom" api_key + enables. Idaho metros at z=10.
-- Additive-only: idempotent via ON CONFLICT DO NOTHING.

INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_TRAFFIC_FLOW', 604800, 1073741824)
ON CONFLICT (name) DO NOTHING;

INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'tomtom_flow',
    false,
    300,
    '{"api_key_alias": "tomtom", "tiles": [{"z":10,"x":181,"y":373},{"z":10,"x":180,"y":374},{"z":10,"x":179,"y":357},{"z":10,"x":193,"y":374},{"z":10,"x":192,"y":376},{"z":10,"x":186,"y":377},{"z":10,"x":179,"y":362},{"z":10,"x":181,"y":374},{"z":10,"x":182,"y":373},{"z":10,"x":182,"y":374}]}'::jsonb
)
ON CONFLICT (name) DO NOTHING;
