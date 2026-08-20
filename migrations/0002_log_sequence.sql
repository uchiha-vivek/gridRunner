-- Log chunks carry a sequence number so a retried POST (the runner timed out
-- after the server had already applied it) appends the same bytes only once.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS log_seq INT NOT NULL DEFAULT 0;
