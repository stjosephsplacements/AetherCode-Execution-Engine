package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *goredis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func TestStream_EnqueueAndCreateGroup(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	s := NewStream(rdb, "ac:test:stream", "workers")
	if err := s.CreateGroup(ctx); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	// Idempotent
	if err := s.CreateGroup(ctx); err != nil {
		t.Fatalf("CreateGroup second call: %v", err)
	}

	if err := s.Enqueue(ctx, "sub-abc-123"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	n, err := rdb.XLen(ctx, "ac:test:stream").Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 message, got %d", n)
	}
}

func TestStream_StreamName(t *testing.T) {
	_, rdb := newTestRedis(t)
	s := NewStream(rdb, "ac:my:stream", "workers")
	if s.StreamName() != "ac:my:stream" {
		t.Fatalf("StreamName() = %q, want %q", s.StreamName(), "ac:my:stream")
	}
}

func TestDualConsumer_WeightedFairQueuing(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const submitStream = "ac:sub"
	const runStream = "ac:run"
	const group = "workers"
	const submitWeight = 3

	dc := NewDualConsumer(rdb, submitStream, runStream, group, submitWeight, 200*time.Millisecond)
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}

	// Enqueue 3 submit + 1 run messages
	for i := 0; i < 3; i++ {
		rdb.XAdd(ctx, &goredis.XAddArgs{Stream: submitStream, Values: map[string]any{"submission_id": "s" + string(rune('0'+i))}})
	}
	rdb.XAdd(ctx, &goredis.XAddArgs{Stream: runStream, Values: map[string]any{"submission_id": "r0"}})

	var submitCount, runCount atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dc.Consume(ctx, "w0", func(_ context.Context, id string) error {
			if len(id) > 0 && id[0] == 's' {
				submitCount.Add(1)
			} else {
				runCount.Add(1)
			}
			// Cancel once all 4 messages are processed
			if submitCount.Load()+runCount.Load() >= 4 {
				cancel()
			}
			return nil
		})
	}()
	wg.Wait()

	if submitCount.Load() != 3 {
		t.Errorf("submitCount = %d, want 3", submitCount.Load())
	}
	if runCount.Load() != 1 {
		t.Errorf("runCount = %d, want 1", runCount.Load())
	}
}

func TestTieredConsumer_ThreePriorityStreams(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const submitStream = "ac:sub:tiered"
	const runStream = "ac:run:tiered"
	const bulkStream = "ac:bulk:tiered"
	const group = "workers"

	tc := NewTieredConsumer(rdb, submitStream, runStream, bulkStream, group, 2, 1, 200*time.Millisecond)
	if err := tc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}

	// Enqueue 2 submit, 1 run, 1 bulk
	rdb.XAdd(ctx, &goredis.XAddArgs{Stream: submitStream, Values: map[string]any{"submission_id": "s0"}})
	rdb.XAdd(ctx, &goredis.XAddArgs{Stream: submitStream, Values: map[string]any{"submission_id": "s1"}})
	rdb.XAdd(ctx, &goredis.XAddArgs{Stream: runStream, Values: map[string]any{"submission_id": "r0"}})
	rdb.XAdd(ctx, &goredis.XAddArgs{Stream: bulkStream, Values: map[string]any{"submission_id": "b0"}})

	var subCnt, runCnt, bulkCnt atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tc.Consume(ctx, "w0", func(_ context.Context, id string) error {
			switch id[0] {
			case 's':
				subCnt.Add(1)
			case 'r':
				runCnt.Add(1)
			case 'b':
				bulkCnt.Add(1)
			}
			if subCnt.Load()+runCnt.Load()+bulkCnt.Load() >= 4 {
				cancel()
			}
			return nil
		})
	}()
	wg.Wait()

	if subCnt.Load() != 2 {
		t.Errorf("subCnt = %d, want 2", subCnt.Load())
	}
	if runCnt.Load() != 1 {
		t.Errorf("runCnt = %d, want 1", runCnt.Load())
	}
	if bulkCnt.Load() != 1 {
		t.Errorf("bulkCnt = %d, want 1", bulkCnt.Load())
	}
}

func TestDualConsumer_CreateGroups_Idempotent(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	dc := NewDualConsumer(rdb, "ac:s", "ac:r", "grp", 3, 200*time.Millisecond)
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("first CreateGroups: %v", err)
	}
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("second CreateGroups: %v", err)
	}
}

func TestDualConsumer_BlockRead_UnblocksOnNewMessage(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dc := NewDualConsumer(rdb, "ac:sub2", "ac:run2", "workers", 3, 300*time.Millisecond)
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}

	received := make(chan string, 1)
	go func() {
		dc.Consume(ctx, "w0", func(_ context.Context, id string) error {
			received <- id
			cancel()
			return nil
		})
	}()

	// Give the consumer time to block, then add a message
	time.Sleep(100 * time.Millisecond)
	rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: "ac:sub2",
		Values: map[string]any{"submission_id": "unblock-me"},
	})

	select {
	case id := <-received:
		if id != "unblock-me" {
			t.Errorf("got id=%q, want %q", id, "unblock-me")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for consumer to unblock")
	}
}

func TestDualConsumer_Reclaimer_ReprocessesIdleMessages(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx := context.Background()

	dc := NewDualConsumer(rdb, "ac:sub3", "ac:run3", "workers", 3, 300*time.Millisecond)
	// Set MinIdle to 0 so any un-ACKed message is immediately reclaimable, avoiding
	// the need to fast-forward wall-clock time (miniredis FastForward doesn't
	// affect XAutoClaim's idle calculation against the message's delivery timestamp).
	dc.reclaimMinIdle = 0
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}

	// Add a message and claim it without ACKing (simulating a stuck worker).
	rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: "ac:sub3",
		Values: map[string]any{"submission_id": "stuck-job"},
	})
	rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    "workers",
		Consumer: "orphan-worker",
		Streams:  []string{"ac:sub3", ">"},
		Count:    1,
	})

	// Call reclaimStream directly — avoids the 30s ticker in StartReclaimer.
	reclaimed := make([]string, 0, 1)
	dc.reclaimStream(ctx, "ac:sub3", func(_ context.Context, id string) error {
		reclaimed = append(reclaimed, id)
		return nil
	})

	if len(reclaimed) != 1 || reclaimed[0] != "stuck-job" {
		t.Errorf("reclaimed = %v, want [stuck-job]", reclaimed)
	}
}

func TestDualConsumer_PoisonMessage_DeadLettered(t *testing.T) {
	mr, rdb := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dc := NewDualConsumer(rdb, "ac:sub4", "ac:run4", "workers", 3, 300*time.Millisecond)
	if err := dc.CreateGroups(ctx); err != nil {
		t.Fatalf("CreateGroups: %v", err)
	}

	// Add the message
	rdb.XAdd(ctx, &goredis.XAddArgs{
		Stream: "ac:sub4",
		Values: map[string]any{"submission_id": "poison"},
	})

	// Simulate maxDeliveryCount delivery cycles by claiming and NOT acking 5 times
	for i := 0; i < maxDeliveryCount; i++ {
		mr.FastForward(31 * time.Second)
		rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
			Stream:   "ac:sub4",
			Group:    "workers",
			Consumer: "reclaimer",
			MinIdle:  30 * time.Second,
			Start:    "0-0",
			Count:    50,
		})
	}

	// Now run the reclaimer — it should dead-letter (ACK without processing) the message
	handlerCallCount := atomic.Int32{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dc.StartReclaimer(ctx, &wg, func(_ context.Context, id string) error {
			handlerCallCount.Add(1)
			return nil
		})
	}()

	// Give it time to run one cycle
	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()

	// The poison message handler should NOT have been called (it was dead-lettered)
	if handlerCallCount.Load() != 0 {
		t.Errorf("handler called %d times for poison message, want 0", handlerCallCount.Load())
	}
}

func TestDrainLegacyStream_DrainsThenExits(t *testing.T) {
	_, rdb := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create the consumer group BEFORE enqueuing so XReadGroup finds it.
	// DrainLegacyStream calls XGroupCreateMkStream internally, but miniredis
	// requires the group to exist before messages are added in some edge cases;
	// creating it up front is the safe approach.
	rdb.XGroupCreateMkStream(ctx, "ac:legacy", "workers", "0")

	for i := 0; i < 5; i++ {
		rdb.XAdd(ctx, &goredis.XAddArgs{
			Stream: "ac:legacy",
			Values: map[string]any{"submission_id": "legacy-" + string(rune('0'+i))},
		})
	}

	processed := make([]string, 0, 5)
	var mu sync.Mutex
	var wg sync.WaitGroup

	DrainLegacyStream(ctx, &wg, rdb, "ac:legacy", "workers", func(_ context.Context, id string) error {
		mu.Lock()
		processed = append(processed, id)
		mu.Unlock()
		return nil
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("DrainLegacyStream did not finish within 10s")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 5 {
		t.Errorf("processed %d messages, want 5", len(processed))
	}
}
