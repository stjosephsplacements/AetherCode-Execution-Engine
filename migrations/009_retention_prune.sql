-- Retention pruning support (application background job; see AC_DB_RETENTION_DAYS).
--
-- Dry-run: rows that would be deleted for N=90 days (read-only):
--   SELECT s.id, s.status, s.updated_at
--   FROM submissions s
--   WHERE s.status IN ('completed', 'failed')
--     AND s.updated_at < now() - make_interval(days => 90)
--   ORDER BY s.updated_at
--   LIMIT 100;
--
-- Count eligible (after this migration):
--   SELECT count_submissions_eligible_for_retention(90);

CREATE INDEX IF NOT EXISTS idx_submissions_terminal_updated
    ON submissions (updated_at)
    WHERE status IN ('completed', 'failed');

CREATE OR REPLACE FUNCTION count_submissions_eligible_for_retention(retention_days integer)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
    SELECT COUNT(*)::bigint
    FROM submissions
    WHERE status IN ('completed', 'failed')
      AND retention_days > 0
      AND updated_at < now() - make_interval(days => retention_days);
$$;