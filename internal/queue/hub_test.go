package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stjosephsplacements/AetherCode-Execution-Engine/internal/eventlog"
)

func recv(t *testing.T, ch <-chan eventlog.Event) eventlog.Event {
	t.Helper()
	select {
	case evt, ok := <-ch:
		if !ok {
			t.Fatal("channel closed, want an event")
		}
		return evt
	case <-time.After(3 * time.Second):
		t.Fatal("no event within 3s")
	}
	return eventlog.Event{}
}

func TestHub_OneRedisSubscriptionServesEveryClient(t *testing.T) {
	mr, rdb := newTestRedis(t)
	ps := NewPubSub(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a1 := ps.Subscribe(ctx, "job-a")
	a2 := ps.Subscribe(ctx, "job-a")
	b := ps.Subscribe(ctx, "job-b")
	// All three clients share one pattern subscription.
	if n := mr.PubSubNumPat(); n != 1 {
		t.Fatalf("pattern subscriptions = %d, want exactly 1", n)
	}

	if err := ps.PublishEvent(ctx, eventlog.New("job-a", eventlog.EventRunning, nil)); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, a1); got.JobID != "job-a" {
		t.Errorf("a1 got %q", got.JobID)
	}
	if got := recv(t, a2); got.JobID != "job-a" {
		t.Errorf("a2 got %q", got.JobID)
	}
	select {
	case evt := <-b:
		t.Fatalf("job-b received job-a's event: %+v", evt)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestHub_CancelledClientIsRemovedAndItsChannelClosed(t *testing.T) {
	_, rdb := newTestRedis(t)
	ps := NewPubSub(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	ch := ps.Subscribe(ctx, "job-c")
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("got an event after cancel, want a closed channel")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("channel not closed after cancel")
	}
	ps.hubMu.Lock()
	defer ps.hubMu.Unlock()
	ps.hub.mu.Lock()
	defer ps.hub.mu.Unlock()
	if len(ps.hub.subs["job-c"]) != 0 {
		t.Fatal("cancelled client still registered")
	}
}

func TestHub_AClientThatFallsBehindIsClosedSoItReplays(t *testing.T) {
	_, rdb := newTestRedis(t)
	ps := NewPubSub(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow := ps.Subscribe(ctx, "job-d")
	fast := ps.Subscribe(ctx, "job-e")

	for i := 0; i < subscriberBuffer+5; i++ {
		if err := ps.PublishEvent(ctx, eventlog.New("job-d", eventlog.EventRunning, nil)); err != nil {
			t.Fatal(err)
		}
	}
	// Drain the buffered events; the channel must then be closed, not stalled.
	deadline := time.After(3 * time.Second)
	for open := true; open; {
		select {
		case _, ok := <-slow:
			open = ok
		case <-deadline:
			t.Fatal("slow client's channel was never closed")
		}
	}
	// Other jobs are unaffected.
	if err := ps.PublishEvent(ctx, eventlog.New("job-e", eventlog.EventRunning, nil)); err != nil {
		t.Fatal(err)
	}
	recv(t, fast)
}

func TestHub_WaitStopsTheSharedSubscription(t *testing.T) {
	_, rdb := newTestRedis(t)
	ps := NewPubSub(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	ch := ps.Subscribe(ctx, "job-f")
	done := make(chan struct{})
	go func() { ps.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Wait did not return")
	}
	cancel()
	if _, ok := <-ch; ok {
		t.Fatal("channel delivered an event after the hub stopped")
	}
}
