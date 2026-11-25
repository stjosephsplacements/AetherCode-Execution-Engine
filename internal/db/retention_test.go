package db_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/db"
	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/model"
)

func markTerminal(t *testing.T, id uuid.UUID, status model.Status) {
	t.Helper()
	ctx := context.Background()
	_, err := sharedPool.Exec(ctx, `
		UPDATE submissions SET status = $2, verdict = 'accepted', updated_at = now() WHERE id = $1`,
		id, status)
	if err != nil {
		t.Fatalf("mark terminal: %v", err)
	}
}

func backdateUpdatedAt(t *testing.T, id uuid.UUID, days int) {
	t.Helper()
	ctx := context.Background()
	_, err := sharedPool.Exec(ctx,
		`UPDATE submissions SET updated_at = now() - make_interval(days => $2) WHERE id = $1`,
		id, days)
	if err != nil {
		t.Fatalf("backdate updated_at: %v", err)
	}
}

func submissionExists(t *testing.T, id uuid.UUID) bool {
	t.Helper()
	var ok bool
	err := sharedPool.QueryRow(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM submissions WHERE id = $1)`, id).Scan(&ok)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	return ok
}

func TestCountRetentionEligible(t *testing.T) {
	ctx := context.Background()

	old := newSub()
	mustInsert(t, old)
	markTerminal(t, old.ID, model.StatusCompleted)
	backdateUpdatedAt(t, old.ID, 40)

	fresh := newSub()
	mustInsert(t, fresh)
	markTerminal(t, fresh.ID, model.StatusFailed)

	stuck := newSub()
	mustInsert(t, stuck)
	backdateUpdatedAt(t, stuck.ID, 60)

	n, err := db.CountRetentionEligible(ctx, sharedPool, 30)
	if err != nil {
		t.Fatalf("CountRetentionEligible: %v", err)
	}
	if n < 1 {
		t.Fatalf("eligible count = %d, want at least 1 (old terminal row)", n)
	}

	n0, err := db.CountRetentionEligible(ctx, sharedPool, 0)
	if err != nil {
		t.Fatalf("CountRetentionEligible(0): %v", err)
	}
	if n0 != 0 {
		t.Fatalf("eligible count with retention 0 = %d, want 0", n0)
	}
}

func TestPruneRetentionBatch(t *testing.T) {
	ctx := context.Background()

	old := newSub()
	mustInsert(t, old)
	markTerminal(t, old.ID, model.StatusCompleted)
	backdateUpdatedAt(t, old.ID, 45)
	if _, err := sharedPool.Exec(ctx, `
		INSERT INTO submission_test_results (submission_id, test_index, verdict)
		VALUES ($1, 0, 'accepted')`, old.ID); err != nil {
		t.Fatalf("insert test result: %v", err)
	}

	keep := newSub()
	mustInsert(t, keep)
	markTerminal(t, keep.ID, model.StatusCompleted)
	backdateUpdatedAt(t, keep.ID, 5)

	queued := newSub()
	mustInsert(t, queued)
	backdateUpdatedAt(t, queued.ID, 90)

	deleted, err := db.PruneRetentionBatch(ctx, sharedPool, 30, 100)
	if err != nil {
		t.Fatalf("PruneRetentionBatch: %v", err)
	}
	if deleted < 1 {
		t.Fatalf("deleted = %d, want at least 1", deleted)
	}
	if submissionExists(t, old.ID) {
		t.Errorf("old terminal submission should have been pruned")
	}
	if !submissionExists(t, keep.ID) {
		t.Errorf("recent terminal submission should remain")
	}
	if !submissionExists(t, queued.ID) {
		t.Errorf("non-terminal submission should remain")
	}

	var trCount int
	err = sharedPool.QueryRow(ctx,
		`SELECT COUNT(*) FROM submission_test_results WHERE submission_id = $1`, old.ID).Scan(&trCount)
	if err != nil {
		t.Fatalf("count test results: %v", err)
	}
	if trCount != 0 {
		t.Errorf("test results for pruned submission = %d, want 0", trCount)
	}
}