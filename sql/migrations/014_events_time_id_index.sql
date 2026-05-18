-- Migration 014: Add composite index for cursor-based pagination
-- Supports ORDER BY (time DESC, id DESC) with efficient range queries

CREATE INDEX IF NOT EXISTS events_time_id_desc_idx
ON public.events (time DESC, id DESC);
