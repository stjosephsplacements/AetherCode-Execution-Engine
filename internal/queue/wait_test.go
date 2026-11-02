package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/metrics"
)

func TestObserveQueueWait(t *testing.T) {
	before := testutil.CollectAndCount(metrics.QueueWaitSeconds)
	added := time.Now().Add(-3 * time.Second).UnixMilli()
	observeQueueWait("ac:test-wait", fmt.Sprintf("%d-0", added))
	if got := testutil.CollectAndCount(metrics.QueueWaitSeconds); got != before+1 {
		t.Fatalf("series after a valid id = %d, want %d", got, before+1)
	}
	// Ids that are not stream ids add nothing and do not panic.
	for _, id := range []string{"", "nope", "abc-0", "-"} {
		observeQueueWait("ac:test-wait", id)
	}
	if got := testutil.CollectAndCount(metrics.QueueWaitSeconds); got != before+1 {
		t.Fatalf("series after invalid ids = %d, want unchanged %d", got, before+1)
	}
}
