package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

type ProblemVersion struct {
	ID          uuid.UUID
	ProblemID   uuid.UUID
	TenantID    uuid.UUID
	Version     int
	CPULimitNs  *int64
	MemoryLimit *int64
	CheckerType string
}

func GetProblemVersion(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (*ProblemVersion, error) {
	var pv ProblemVersion
	err := pool.QueryRow(ctx, `
		SELECT id, problem_id, tenant_id, version, cpu_limit_ns, memory_limit, checker_type
		FROM problem_versions WHERE id = $1`, id).Scan(
		&pv.ID, &pv.ProblemID, &pv.TenantID, &pv.Version,
		&pv.CPULimitNs, &pv.MemoryLimit, &pv.CheckerType)
	if err != nil {
		return nil, fmt.Errorf("db: get problem version: %w", err)
	}
	return &pv, nil
}

func GetTestCases(ctx context.Context, pool *pgxpool.Pool, problemVersionID uuid.UUID) ([]model.TestCase, error) {
	rows, err := pool.Query(ctx, `
		SELECT ordinal, input, expected_output, is_sample
		FROM test_cases
		WHERE problem_version_id = $1
		ORDER BY ordinal`, problemVersionID)
	if err != nil {
		return nil, fmt.Errorf("db: get test cases: %w", err)
	}
	defer rows.Close()

	var tests []model.TestCase
	for rows.Next() {
		var tc model.TestCase
		if err := rows.Scan(&tc.Index, &tc.Input, &tc.ExpectedOutput, &tc.IsSample); err != nil {
			return nil, fmt.Errorf("db: scan test case: %w", err)
		}
		tests = append(tests, tc)
	}
	return tests, rows.Err()
}
