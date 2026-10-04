-- 002_step3.sql: Correctness & Security schema
-- Adds tenants, users, problems, problem_versions, test_cases, languages.
-- Extends submissions with tenant/user/problem/idempotency/mode columns.
-- Configures Row-Level Security as defense-in-depth.

-- 1. Tenants
CREATE TABLE IF NOT EXISTS tenants (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO tenants (id, name, slug)
VALUES ('00000000-0000-0000-0000-000000000001', 'Default', 'default')
ON CONFLICT (slug) DO NOTHING;

-- 2. Users (external_subject_id = Zitadel sub claim)
CREATE TABLE IF NOT EXISTS users (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id            UUID NOT NULL REFERENCES tenants(id),
    external_subject_id  TEXT NOT NULL,
    email                TEXT,
    display_name         TEXT,
    role                 TEXT NOT NULL DEFAULT 'student',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, external_subject_id)
);

CREATE INDEX IF NOT EXISTS idx_users_external_sub
    ON users (external_subject_id);

-- 3. Languages (reference table; sandbox/language.go is canonical)
CREATE TABLE IF NOT EXISTS languages (
    slug        TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO languages (slug, name) VALUES
    ('c', 'C'), ('cpp', 'C++'), ('go', 'Go'), ('java', 'Java'),
    ('python', 'Python'), ('javascript', 'JavaScript'), ('sqlite', 'SQLite')
ON CONFLICT (slug) DO NOTHING;

-- 4. Problems
CREATE TABLE IF NOT EXISTS problems (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    title       TEXT NOT NULL,
    slug        TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, slug)
);

-- 5. Problem versions (immutable once created)
CREATE TABLE IF NOT EXISTS problem_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    problem_id      UUID NOT NULL REFERENCES problems(id),
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    version         INTEGER NOT NULL DEFAULT 1,
    description     TEXT,
    cpu_limit_ns    BIGINT,
    memory_limit    BIGINT,
    checker_type    TEXT NOT NULL DEFAULT 'exact',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (problem_id, version)
);

-- 6. Test cases (belong to a problem_version)
CREATE TABLE IF NOT EXISTS test_cases (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    problem_version_id  UUID NOT NULL REFERENCES problem_versions(id),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    ordinal             INTEGER NOT NULL,
    input               TEXT NOT NULL,
    expected_output     TEXT NOT NULL,
    is_sample           BOOLEAN NOT NULL DEFAULT false,
    explanation         TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (problem_version_id, ordinal)
);

-- 7. Extend submissions with tenant/user/problem/idempotency/mode
ALTER TABLE submissions
    ADD COLUMN IF NOT EXISTS tenant_id            UUID REFERENCES tenants(id),
    ADD COLUMN IF NOT EXISTS user_id              UUID REFERENCES users(id),
    ADD COLUMN IF NOT EXISTS problem_version_id   UUID REFERENCES problem_versions(id),
    ADD COLUMN IF NOT EXISTS idempotency_key      TEXT,
    ADD COLUMN IF NOT EXISTS mode                 TEXT NOT NULL DEFAULT 'run';

-- Backfill existing rows to default tenant
UPDATE submissions SET tenant_id = '00000000-0000-0000-0000-000000000001'
WHERE tenant_id IS NULL;

ALTER TABLE submissions ALTER COLUMN tenant_id SET NOT NULL;

-- Idempotency unique constraint (partial: only when key is present)
CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_idempotency
    ON submissions (tenant_id, user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Per-user query index
CREATE INDEX IF NOT EXISTS idx_submissions_tenant_user
    ON submissions (tenant_id, user_id, created_at DESC);

-- Problem-based lookup index
CREATE INDEX IF NOT EXISTS idx_submissions_problem_version
    ON submissions (problem_version_id, created_at DESC)
    WHERE problem_version_id IS NOT NULL;

-- 8. Row-Level Security (defense-in-depth)
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE problems ENABLE ROW LEVEL SECURITY;
ALTER TABLE problem_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE test_cases ENABLE ROW LEVEL SECURITY;
ALTER TABLE submissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE submission_test_results ENABLE ROW LEVEL SECURITY;

-- Superuser (aethercode) bypasses RLS. These policies apply to aethercode_app role
-- if we ever split gateway into a separate connection role.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aethercode_app') THEN
    CREATE ROLE aethercode_app LOGIN;
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'tenants' AND policyname = 'tenant_isolation_tenants'
  ) THEN
    CREATE POLICY tenant_isolation_tenants ON tenants
        FOR ALL TO aethercode_app
        USING (id::text = current_setting('app.tenant_id', true));
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'users' AND policyname = 'tenant_isolation_users'
  ) THEN
    CREATE POLICY tenant_isolation_users ON users
        FOR ALL TO aethercode_app
        USING (tenant_id::text = current_setting('app.tenant_id', true));
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'problems' AND policyname = 'tenant_isolation_problems'
  ) THEN
    CREATE POLICY tenant_isolation_problems ON problems
        FOR ALL TO aethercode_app
        USING (tenant_id::text = current_setting('app.tenant_id', true));
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'problem_versions' AND policyname = 'tenant_isolation_problem_versions'
  ) THEN
    CREATE POLICY tenant_isolation_problem_versions ON problem_versions
        FOR ALL TO aethercode_app
        USING (tenant_id::text = current_setting('app.tenant_id', true));
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'test_cases' AND policyname = 'tenant_isolation_test_cases'
  ) THEN
    CREATE POLICY tenant_isolation_test_cases ON test_cases
        FOR ALL TO aethercode_app
        USING (tenant_id::text = current_setting('app.tenant_id', true));
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'submissions' AND policyname = 'tenant_isolation_submissions'
  ) THEN
    CREATE POLICY tenant_isolation_submissions ON submissions
        FOR ALL TO aethercode_app
        USING (tenant_id::text = current_setting('app.tenant_id', true));
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'submission_test_results' AND policyname = 'tenant_isolation_test_results'
  ) THEN
    CREATE POLICY tenant_isolation_test_results ON submission_test_results
        FOR ALL TO aethercode_app
        USING (submission_id IN (
            SELECT id FROM submissions
            WHERE tenant_id::text = current_setting('app.tenant_id', true)
        ));
  END IF;
END $$;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO aethercode_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO aethercode_app;
