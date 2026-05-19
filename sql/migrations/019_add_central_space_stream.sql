-- Migration: 019_add_central_space_stream
-- Seeds the CENTRAL_SPACE JetStream stream row for central.space.> subjects.
-- 7-day retention, 1 GiB max_bytes (clamped by supervisor recompute) -- mirrors CENTRAL_FIRE / CENTRAL_QUAKE.
-- Idempotent: uses ON CONFLICT DO NOTHING.

INSERT INTO config.streams (name, max_age_s, max_bytes)
VALUES ('CENTRAL_SPACE', 604800, 1073741824)
ON CONFLICT (name) DO NOTHING;
