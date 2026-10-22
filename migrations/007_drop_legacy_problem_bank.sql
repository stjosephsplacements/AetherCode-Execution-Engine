-- Migration 007: Drop legacy problem bank tables, obsolete columns, and legacy indexes.
-- The execution engine is purely an executor and no longer stores problems, problem versions, or test cases.

-- 1. Drop the legacy problem bank tables in reverse dependency order.
DROP TABLE IF EXISTS test_cases CASCADE;
DROP TABLE IF EXISTS problem_versions CASCADE;
DROP TABLE IF EXISTS problems CASCADE;

-- 2. Drop the obsolete columns and indexes from submissions.
DROP INDEX IF EXISTS idx_submissions_problem_version;
DROP INDEX IF EXISTS idx_submissions_idempotency;

ALTER TABLE submissions
    DROP COLUMN IF EXISTS problem_version_id,
    DROP COLUMN IF EXISTS mode;
