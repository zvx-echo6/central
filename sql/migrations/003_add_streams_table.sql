-- Migration: 003_add_streams_table
-- Creates the config.streams table for JetStream stream retention configuration.
-- Uses column-filtered NOTIFY to prevent self-loop when supervisor updates max_bytes.

-- Streams configuration table
CREATE TABLE config.streams (
    name              TEXT PRIMARY KEY,
    max_age_s         BIGINT NOT NULL,
    max_bytes         BIGINT NOT NULL DEFAULT 1073741824,  -- 1GB default
    managed_max_bytes BOOLEAN NOT NULL DEFAULT true,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Auto-update trigger for updated_at
CREATE TRIGGER streams_set_updated_at
    BEFORE UPDATE ON config.streams
    FOR EACH ROW EXECUTE FUNCTION config.set_updated_at();

-- Column-filtered NOTIFY trigger for streams.
-- Fires on INSERT/DELETE always.
-- On UPDATE, only fires when max_age_s changes (operator-touchable field),
-- NOT when max_bytes changes (supervisor-managed), to prevent recompute loop.
CREATE OR REPLACE FUNCTION config.notify_streams_change()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'DELETE' THEN
        PERFORM pg_notify('config_changed', 'streams:' ||
            COALESCE(NEW.name, OLD.name));
    ELSIF TG_OP = 'UPDATE' AND
          OLD.max_age_s IS DISTINCT FROM NEW.max_age_s THEN
        PERFORM pg_notify('config_changed', 'streams:' || NEW.name);
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER streams_notify
    AFTER INSERT OR UPDATE OR DELETE ON config.streams
    FOR EACH ROW EXECUTE FUNCTION config.notify_streams_change();

-- Seed with current stream values from investigation
-- CENTRAL_WX: 7d max_age (604800s), 10GB max_bytes (will be clamped to 6GB on first recompute)
-- CENTRAL_META: 1d max_age (86400s), 100MB max_bytes (will be raised to 1GB floor)
INSERT INTO config.streams (name, max_age_s, max_bytes) VALUES
    ('CENTRAL_WX',   604800,  10737418240),
    ('CENTRAL_META', 86400,   104857600);
