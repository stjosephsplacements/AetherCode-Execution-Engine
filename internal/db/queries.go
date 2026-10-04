package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

func InsertSubmission(ctx context.Context, pool *pgxpool.Pool, sub *model.Submission) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO submissions (id, tenant_id, user_id, problem_version_id, idempotency_key, mode,
		                         language, source_code, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())`,
		sub.ID, sub.TenantID, sub.UserID, sub.ProblemVersionID, sub.IdempotencyKey, sub.Mode,
		sub.Language, sub.SourceCode, sub.Status)
	if err != nil {
		return fmt.Errorf("db: insert submission: %w", err)
	}
	return nil
}

// InsertSubmissionIdempotent attempts an insert; if the idempotency key already exists,
// it returns the existing submission's ID and isNew=false.
func InsertSubmissionIdempotent(ctx context.Context, pool *pgxpool.Pool, sub *model.Submission) (uuid.UUID, bool, error) {
	var insertedID *uuid.UUID
	err := pool.QueryRow(ctx, `
		INSERT INTO submissions (id, tenant_id, user_id, problem_version_id, idempotency_key, mode,
		                         language, source_code, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())
		ON CONFLICT (tenant_id, user_id, idempotency_key, mode) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING id`,
		sub.ID, sub.TenantID, sub.UserID, sub.ProblemVersionID, sub.IdempotencyKey, sub.Mode,
		sub.Language, sub.SourceCode, sub.Status).Scan(&insertedID)

	if err == pgx.ErrNoRows {
		// Conflict: fetch the existing submission's ID
		var existingID uuid.UUID
		err2 := pool.QueryRow(ctx, `
			SELECT id FROM submissions
			WHERE tenant_id = $1 AND user_id = $2 AND idempotency_key = $3 AND mode = $4`,
			sub.TenantID, sub.UserID, sub.IdempotencyKey, sub.Mode).Scan(&existingID)
		if err2 != nil {
			return uuid.Nil, false, fmt.Errorf("db: idempotent lookup: %w", err2)
		}
		return existingID, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("db: idempotent insert: %w", err)
	}
	return *insertedID, true, nil
}

func GetSubmission(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (*model.Submission, error) {
	var sub model.Submission
	var verdict, compileStderr *string
	var cpuTimeNs, memoryBytes *int64
	var testCount, testsPassed *int
	err := pool.QueryRow(ctx, `
		SELECT id, tenant_id, user_id, problem_version_id, idempotency_key, mode,
		       language, source_code, status, verdict,
		       compile_stderr, cpu_time_ns, memory_bytes,
		       test_count, tests_passed, created_at, updated_at
		FROM submissions WHERE id = $1`, id).Scan(
		&sub.ID, &sub.TenantID, &sub.UserID, &sub.ProblemVersionID, &sub.IdempotencyKey, &sub.Mode,
		&sub.Language, &sub.SourceCode, &sub.Status, &verdict,
		&compileStderr, &cpuTimeNs, &memoryBytes,
		&testCount, &testsPassed, &sub.CreatedAt, &sub.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("db: get submission: %w", err)
	}
	if verdict != nil {
		sub.Verdict = model.Verdict(*verdict)
	}
	if compileStderr != nil {
		sub.CompileStderr = *compileStderr
	}
	if cpuTimeNs != nil {
		sub.CPUTimeNs = *cpuTimeNs
	}
	if memoryBytes != nil {
		sub.MemoryBytes = *memoryBytes
	}
	if testCount != nil {
		sub.TestCount = *testCount
	}
	if testsPassed != nil {
		sub.TestsPassed = *testsPassed
	}
	return &sub, nil
}

func UpdateSubmissionStatus(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, status model.Status) error {
	tag, err := pool.Exec(ctx, `
		UPDATE submissions SET status = $2, updated_at = now()
		WHERE id = $1 AND status NOT IN ('completed', 'failed')`,
		id, status)
	if err != nil {
		return fmt.Errorf("db: update status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("db: status update skipped for %s (already terminal)", id)
	}
	return nil
}

// TouchSubmissionUpdatedAt bumps updated_at without changing status.
// Used by the orphan reaper after re-enqueue to prevent tight re-enqueue loops.
func TouchSubmissionUpdatedAt(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) error {
	_, err := pool.Exec(ctx, `UPDATE submissions SET updated_at = now() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("db: touch updated_at: %w", err)
	}
	return nil
}

func UpdateSubmissionVerdict(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID,
	status model.Status, verdict model.Verdict, compileStderr string,
	cpuTimeNs, memoryBytes int64, testCount, testsPassed int) error {
	tag, err := pool.Exec(ctx, `
		UPDATE submissions
		SET status = $2, verdict = $3, compile_stderr = $4,
		    cpu_time_ns = $5, memory_bytes = $6,
		    test_count = $7, tests_passed = $8, updated_at = now()
		WHERE id = $1 AND status NOT IN ('completed', 'failed')`,
		id, status, verdict, compileStderr, cpuTimeNs, memoryBytes, testCount, testsPassed)
	if err != nil {
		return fmt.Errorf("db: update verdict: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("db: verdict already written for %s", id)
	}
	return nil
}

func InsertTestResults(ctx context.Context, pool *pgxpool.Pool, submissionID uuid.UUID, results []model.TestResult) error {
	if len(results) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, r := range results {
		batch.Queue(`
			INSERT INTO submission_test_results
				(submission_id, test_index, verdict, cpu_time_ns, memory_bytes, stdout_preview, stderr_preview)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (submission_id, test_index) DO UPDATE SET
				verdict = EXCLUDED.verdict,
				cpu_time_ns = EXCLUDED.cpu_time_ns,
				memory_bytes = EXCLUDED.memory_bytes,
				stdout_preview = EXCLUDED.stdout_preview,
				stderr_preview = EXCLUDED.stderr_preview`,
			submissionID, r.TestIndex, r.Verdict, r.CPUTimeNs, r.MemoryBytes,
			Truncate(r.StdoutPreview, 256), Truncate(r.StderrPreview, 256))
	}

	br := pool.SendBatch(ctx, batch)
	defer br.Close() //nolint:errcheck // pgx BatchResults.Close() is a no-op that always returns nil

	var firstErr error
	for i := range results {
		if _, err := br.Exec(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("db: insert test result[%d]: %w", i, err)
		}
	}
	return firstErr
}

type OrphanedSubmission struct {
	ID   uuid.UUID
	Mode string
}

// OrphanedSubmissions returns submissions stuck in queued state for re-enqueuing.
func OrphanedSubmissions(ctx context.Context, pool *pgxpool.Pool) ([]OrphanedSubmission, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, mode FROM submissions
		WHERE status = 'queued' AND updated_at < now() - interval '2 minutes'
		ORDER BY updated_at
		LIMIT 100`)
	if err != nil {
		return nil, fmt.Errorf("db: orphaned submissions: %w", err)
	}
	defer rows.Close()

	var subs []OrphanedSubmission
	for rows.Next() {
		var s OrphanedSubmission
		if err := rows.Scan(&s.ID, &s.Mode); err != nil {
			return nil, err
		}
		subs = append(subs, s)
	}
	return subs, rows.Err()
}

func Truncate(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	// Walk back to avoid splitting a multi-byte rune
	cut := maxBytes
	for cut > 0 && cut < len(s) && s[cut]>>6 == 0b10 {
		cut--
	}
	return s[:cut]
}
