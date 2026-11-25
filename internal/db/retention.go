package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const retentionBatchSize = 500

// CountRetentionEligible returns how many terminal submissions are older than retentionDays.
// retentionDays <= 0 always yields 0.
func CountRetentionEligible(ctx context.Context, pool *pgxpool.Pool, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	var n int64
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*)::bigint FROM submissions
		WHERE status IN ('completed', 'failed')
		  AND updated_at < now() - make_interval(days => $1)`, retentionDays).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("db: count retention eligible: %w", err)
	}
	return n, nil
}

// PruneRetentionBatch deletes up to batchSize terminal submissions older than retentionDays,
// removing submission_test_results first. Returns how many submissions were deleted.
func PruneRetentionBatch(ctx context.Context, pool *pgxpool.Pool, retentionDays, batchSize int) (int, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	if batchSize <= 0 {
		batchSize = retentionBatchSize
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("db: retention prune begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // committed batches roll back only on failure

	rows, err := tx.Query(ctx, `
		SELECT id FROM submissions
		WHERE status IN ('completed', 'failed')
		  AND updated_at < now() - make_interval(days => $1)
		ORDER BY updated_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, retentionDays, batchSize)
	if err != nil {
		return 0, fmt.Errorf("db: retention prune select: %w", err)
	}

	var ids []uuid.UUID
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			rows.Close()
			return 0, err
		}
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("db: retention prune scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("db: retention prune iterate: %w", err)
	}
	rows.Close()

	if len(ids) == 0 {
		return 0, nil
	}

	if _, err := tx.Exec(ctx, `DELETE FROM submission_test_results WHERE submission_id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("db: retention prune test results: %w", err)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM submissions WHERE id = ANY($1)`, ids)
	if err != nil {
		return 0, fmt.Errorf("db: retention prune submissions: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("db: retention prune commit: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// PruneRetentionEligible drains batches until none remain or ctx is cancelled.
func PruneRetentionEligible(ctx context.Context, pool *pgxpool.Pool, retentionDays int) (int, error) {
	var total int
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, err := PruneRetentionBatch(ctx, pool, retentionDays, retentionBatchSize)
		if err != nil {
			return total, err
		}
		total += n
		if n == 0 {
			return total, nil
		}
	}
}