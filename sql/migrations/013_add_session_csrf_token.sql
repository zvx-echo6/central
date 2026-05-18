-- Add CSRF token column to sessions table
-- Session-bound CSRF tokens prevent race conditions from cookie rotation

ALTER TABLE config.sessions
  ADD COLUMN csrf_token TEXT NOT NULL
    DEFAULT encode(gen_random_bytes(32), 'hex');

-- Comment
COMMENT ON COLUMN config.sessions.csrf_token IS 'Session-bound CSRF token for synchronizer token pattern';
