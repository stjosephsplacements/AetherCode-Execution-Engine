CREATE INDEX IF NOT EXISTS idx_submissions_queued_updated
    ON submissions (updated_at) WHERE status = 'queued';
