-- Central Data Hub schema
-- PostgreSQL 16 + TimescaleDB + PostGIS

CREATE TABLE IF NOT EXISTS events (
    id              TEXT NOT NULL,                     -- CloudEvent id
    source          TEXT NOT NULL,                     -- adapter identity
    category        TEXT NOT NULL,                     -- "wx.alert.<type>"
    time            TIMESTAMPTZ NOT NULL,              -- event-time UTC
    expires         TIMESTAMPTZ,
    severity        SMALLINT,                          -- 0..4 or NULL
    geom            GEOMETRY(Geometry, 4326),          -- centroid or bbox as Polygon
    regions         TEXT[],                            -- ["US-ID-Ada", ...]
    primary_region  TEXT,
    payload         JSONB NOT NULL,                    -- full Event as JSON
    received        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, time)                             -- composite PK for TimescaleDB
);

SELECT create_hypertable('events', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS events_category_time_idx
    ON events (category, time DESC);

CREATE INDEX IF NOT EXISTS events_geom_gist
    ON events USING GIST (geom);

CREATE INDEX IF NOT EXISTS events_regions_gin
    ON events USING GIN (regions);

-- Dedup on insert via ON CONFLICT (id, time) in the archive consumer.
