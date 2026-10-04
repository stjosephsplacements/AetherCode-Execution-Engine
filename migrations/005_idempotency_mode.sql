-- The idempotency unique index must include mode so that the same key can be
-- reused across run and submit modes without false conflicts.
-- ON CONFLICT (tenant_id, user_id, idempotency_key, mode) in InsertSubmissionIdempotent
-- requires an index with exactly these four columns.
DROP INDEX IF EXISTS idx_submissions_idempotency;

CREATE UNIQUE INDEX idx_submissions_idempotency
    ON submissions (tenant_id, user_id, idempotency_key, mode)
    WHERE idempotency_key IS NOT NULL;
