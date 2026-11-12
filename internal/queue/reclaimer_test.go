package queue

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	goredis "github.com/redis/go-redis/v9"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/metrics"
)

func TestFullJitter_StaysBelowBase(t *testing.T) {
	base := 30 * time.Second
	cases := []struct {
		r    float64
		want time.Duration
	}{
		{0, 0},
		{0.5, 15 * time.Second},
		{0.25, 7500 * time.Millisecond},
	}
	for _, c := range cases {
		if got := fullJitter(base, func() float64 { return c.r }); got != c.want {
			t.Errorf("fullJitter(30s, %v) = %v, want %v", c.r, got, c.want)
		}
	}

	r := rand.New(rand.NewPCG(1, 2)).Float64 //nolint:gosec // G404: seeded so the test is repeatable
	for i := 0; i < 10_000; i++ {
		if d := fullJitter(base, r); d < 0 || d >= base {
			t.Fatalf("draw %d = %v, want within [0, %v)", i, d, base)
		}
	}
}

// Ten equal-width buckets over [0, 30s) should each get about a tenth of the
// draws. A fixed seed keeps the test deterministic.
func TestFullJitter_IsUniform(t *testing.T) {
	const draws, buckets = 10_000, 10
	base := 30 * time.Second
	r := rand.New(rand.NewPCG(7, 11)).Float64 //nolint:gosec // G404: seeded so the test is repeatable

	var counts [buckets]int
	for i := 0; i < draws; i++ {
		counts[int(fullJitter(base, r)*buckets/base)]++
	}
	for i, n := range counts {
		// Expected 1000 per bucket; ±20% is far outside normal variation.
		if n < 800 || n > 1200 {
			t.Errorf("bucket %d holds %d draws, want 800..1200 (counts %v)", i, n, counts)
		}
	}
}

// Twenty reclaimers that start in the same instant must not fire their
// first cycle together: no one-second window may hold more than a handful.
func TestFullJitter_SpreadsSimultaneousStarts(t *testing.T) {
	const nodes = 20
	base := 30 * time.Second

	perSecond := map[int64]int{}
	for node := 0; node < nodes; node++ {
		r := rand.New(rand.NewPCG(uint64(node), 99)).Float64 //nolint:gosec // G404: seeded so the test is repeatable
		perSecond[int64(fullJitter(base, r)/time.Second)]++
	}
	for sec, n := range perSecond {
		if n > 4 {
			t.Errorf("%d of %d reclaimers fire in second %d, want at most 4", n, nodes, sec)
		}
	}
	if len(perSecond) < nodes/2 {
		t.Errorf("first cycles fall into only %d distinct seconds, want at least %d", len(perSecond), nodes/2)
	}
}

// The loop must draw a new wait after every cycle (a Ticker would keep one
// fixed phase). Two zero waits make two cycles run at once; the third draw
// parks the loop for most of an hour.
func TestReclaimer_RedrawsJitterEveryCycle(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dc := NewDualConsumer(rdb, "ac:sub-jit", "ac:run-jit", "workers", 3, 300*time.Millisecond)
	dc.reclaimMinIdle = 0
	dc.reclaimInterval = time.Hour
	var draws atomic.Int32
	dc.jitter = func() float64 {
		if draws.Add(1) <= 2 {
			return 0
		}
		return 0.99
	}
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}
	addStuckMessage(ctx, t, rdb, "ac:sub-jit", "stuck-jit")

	var handled atomic.Int32
	var wg sync.WaitGroup
	dc.StartReclaimer(ctx, &wg, func(_ context.Context, id string) error {
		if id == "stuck-jit" {
			handled.Add(1)
		}
		return nil
	})

	deadline := time.Now().Add(5 * time.Second)
	for draws.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if got := draws.Load(); got != 3 {
		t.Errorf("jitter drawn %d times, want 3 (initial wait + one per finished cycle)", got)
	}
	if got := handled.Load(); got != 1 {
		t.Errorf("stuck message handled %d times, want 1", got)
	}
}

func TestReclaimer_CountsRecoveredClaims(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	dc := NewDualConsumer(rdb, "ac:sub-rec", "ac:run-rec", "workers", 3, 300*time.Millisecond)
	dc.reclaimMinIdle = 0
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}
	addStuckMessage(ctx, t, rdb, "ac:sub-rec", "stuck-rec")

	recovered := metrics.ReclaimerClaimsTotal.WithLabelValues("ac:sub-rec", "recovered")
	poison := metrics.ReclaimerClaimsTotal.WithLabelValues("ac:sub-rec", "poison")
	before, beforePoison := testutil.ToFloat64(recovered), testutil.ToFloat64(poison)

	dc.reclaimStream(ctx, "ac:sub-rec", func(context.Context, string) error { return nil })

	if got := testutil.ToFloat64(recovered) - before; got != 1 {
		t.Errorf("recovered claims grew by %v, want 1", got)
	}
	if got := testutil.ToFloat64(poison) - beforePoison; got != 0 {
		t.Errorf("poison claims grew by %v, want 0", got)
	}
}

// A message whose handler fails is left pending for the next cycle, so it
// must not count as recovered.
func TestReclaimer_FailedHandlerIsNotCounted(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	dc := NewDualConsumer(rdb, "ac:sub-fail", "ac:run-fail", "workers", 3, 300*time.Millisecond)
	dc.reclaimMinIdle = 0
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}
	addStuckMessage(ctx, t, rdb, "ac:sub-fail", "stuck-fail")

	recovered := metrics.ReclaimerClaimsTotal.WithLabelValues("ac:sub-fail", "recovered")
	before := testutil.ToFloat64(recovered)

	dc.reclaimStream(ctx, "ac:sub-fail", func(context.Context, string) error {
		return context.DeadlineExceeded
	})

	if got := testutil.ToFloat64(recovered) - before; got != 0 {
		t.Errorf("recovered claims grew by %v after a handler error, want 0", got)
	}
}

func TestReclaimer_CountsPoisonClaims(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	dc := NewDualConsumer(rdb, "ac:sub-poi", "ac:run-poi", "workers", 3, 300*time.Millisecond)
	dc.reclaimMinIdle = 0
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}
	id := addStuckMessage(ctx, t, rdb, "ac:sub-poi", "poison-job")
	// The read above was delivery 1; each XCLAIM adds one more.
	for i := 1; i < maxDeliveryCount; i++ {
		if err := rdb.XClaim(ctx, &goredis.XClaimArgs{
			Stream:   "ac:sub-poi",
			Group:    "workers",
			Consumer: "crashing-worker",
			Messages: []string{id},
		}).Err(); err != nil {
			t.Fatalf("XClaim: %v", err)
		}
	}

	poison := metrics.ReclaimerClaimsTotal.WithLabelValues("ac:sub-poi", "poison")
	recovered := metrics.ReclaimerClaimsTotal.WithLabelValues("ac:sub-poi", "recovered")
	before, beforeRecovered := testutil.ToFloat64(poison), testutil.ToFloat64(recovered)

	var handled atomic.Int32
	dc.reclaimStream(ctx, "ac:sub-poi", func(context.Context, string) error {
		handled.Add(1)
		return nil
	})

	if n := handled.Load(); n != 0 {
		t.Errorf("handler ran %d times for a poison message, want 0", n)
	}
	if got := testutil.ToFloat64(poison) - before; got != 1 {
		t.Errorf("poison claims grew by %v, want 1", got)
	}
	if got := testutil.ToFloat64(recovered) - beforeRecovered; got != 0 {
		t.Errorf("recovered claims grew by %v, want 0", got)
	}
	if n, err := rdb.XLen(ctx, "ac:dlq:poison").Result(); err != nil || n != 1 {
		t.Errorf("DLQ length = %d (err %v), want 1", n, err)
	}
}

// addStuckMessage enqueues a message and reads it as a worker that never
// acks, leaving it in the group's pending list. It returns the message ID.
func addStuckMessage(ctx context.Context, t *testing.T, rdb *goredis.Client, stream, submissionID string) string {
	t.Helper()
	id, err := rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"submission_id": submissionID},
	}).Result()
	if err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	if err := rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    "workers",
		Consumer: "crashed-worker",
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Err(); err != nil {
		t.Fatalf("XReadGroup: %v", err)
	}
	return id
}
