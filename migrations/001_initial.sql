CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS submissions (
    id              UUID PRIMARY KEY,
    language        TEXT NOT NULL,
    source_code     TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued',
    verdict         TEXT,
    compile_stderr  TEXT,
    cpu_time_ns     BIGINT,
    memory_bytes    BIGINT,
    test_count      INTEGER,
    tests_passed    INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_submissions_status
    ON submissions (status) WHERE status NOT IN ('completed', 'failed');
CREATE INDEX IF NOT EXISTS idx_submissions_created_at
    ON submissions (created_at DESC);

CREATE TABLE IF NOT EXISTS submission_test_results (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id   UUID NOT NULL REFERENCES submissions(id),
    test_index      INTEGER NOT NULL,
    verdict         TEXT NOT NULL,
    cpu_time_ns     BIGINT,
    memory_bytes    BIGINT,
    stdout_preview  TEXT,
    stderr_preview  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (submission_id, test_index)
);

CREATE INDEX IF NOT EXISTS idx_test_results_submission
    ON submission_test_results (submission_id);
