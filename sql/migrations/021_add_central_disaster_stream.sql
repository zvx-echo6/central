-- Migration: 021_add_central_disaster_stream
-- Seeds the CENTRAL_DISASTER JetStream stream row for central.disaster.> subjects.
-- 7-day retention, 1 GiB max_bytes -- mirrors CENTRAL_FIRE / CENTRAL_QUAKE / CENTRAL_SPACE.
-- Idempotent: uses ON CONFLICT DO NOTHING.

INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_DISASTER', 604800, 1073741824)
ON CONFLICT (name) DO NOTHING;
