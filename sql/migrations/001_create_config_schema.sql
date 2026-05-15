-- Migration: 001_create_config_schema
-- Creates the config schema with adapters and api_keys tables.
-- Also seeds the NWS adapter row from current TOML config.

-- Create config schema
CREATE SCHEMA config;

-- Adapters configuration table
CREATE TABLE config.adapters (
    name           TEXT PRIMARY KEY,
    enabled        BOOLEAN NOT NULL DEFAULT true,
    cadence_s      INTEGER NOT NULL,
    settings       JSONB NOT NULL DEFAULT '{}'::jsonb,
    paused_at      TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- API keys table (encrypted values)
CREATE TABLE config.api_keys (
    alias            TEXT PRIMARY KEY,
    encrypted_value  BYTEA NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    rotated_at       TIMESTAMPTZ,
    last_used_at     TIMESTAMPTZ
);

-- Notify function for config changes
CREATE OR REPLACE FUNCTION config.notify_config_change()
RETURNS trigger AS $$
DECLARE
    key_value TEXT;
BEGIN
    -- Handle different table structures
    IF TG_TABLE_NAME = 'adapters' THEN
        key_value := COALESCE(NEW.name, OLD.name, '');
    ELSIF TG_TABLE_NAME = 'api_keys' THEN
        key_value := COALESCE(NEW.alias, OLD.alias, '');
    ELSE
        key_value := '';
    END IF;

    PERFORM pg_notify('config_changed', TG_TABLE_NAME || ':' || key_value);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Trigger for adapters table
CREATE TRIGGER adapters_notify
    AFTER INSERT OR UPDATE OR DELETE ON config.adapters
    FOR EACH ROW EXECUTE FUNCTION config.notify_config_change();

-- Trigger for api_keys table
CREATE TRIGGER api_keys_notify
    AFTER INSERT OR UPDATE OR DELETE ON config.api_keys
    FOR EACH ROW EXECUTE FUNCTION config.notify_config_change();

-- Seed NWS adapter from current TOML config values
INSERT INTO config.adapters (name, enabled, cadence_s, settings)
VALUES (
    'nws',
    true,
    60,
    '{"states": ["ID", "OR", "WA", "MT", "WY", "UT", "NV"], "contact_email": "mj@k7zvx.com"}'::jsonb
);
