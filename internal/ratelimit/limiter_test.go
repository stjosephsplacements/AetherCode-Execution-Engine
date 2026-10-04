package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func newMiniredis(t *testing.T) *goredis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// TestAdmissionCheck covers the queue-depth admission gate: a stream within the
// threshold is admitted, one past it is not.
func TestAdmissionCheck(t *testing.T) {
	rdb := newMiniredis(t)
	ctx := context.Background()
	stream := "ac:submissions:run"

	if ok, err := AdmissionCheck(ctx, rdb, stream, 100); err != nil || !ok {
		t.Fatalf("empty stream should be admitted, ok=%v err=%v", ok, err)
	}

	for i := 0; i < 150; i++ {
		if _, err := rdb.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"i": i}}).Result(); err != nil {
			t.Fatalf("XAdd failed: %v", err)
		}
	}
	if ok, err := AdmissionCheck(ctx, rdb, stream, 100); err != nil || ok {
		t.Fatalf("over-depth stream should be rejected, ok=%v err=%v", ok, err)
	}
}

// TestCheckFirstRequestAllowed is a smoke test that the limiter constructs and
// the sliding-window Lua script runs without error.
func TestCheckFirstRequestAllowed(t *testing.T) {
	rdb := newMiniredis(t)
	l := New(rdb, 10, 5)

	if res := l.Check(context.Background(), uuid.New()); !res.Allowed {
		t.Fatalf("first request should be allowed, got %+v", res)
	}
}

// TestCheckBurstLimit drives the burst window: with burst=2 the third request
// inside the window is denied with a RetryAfter. A short sleep between requests
// guarantees each uses a distinct millisecond, so the per-request members in the
// sorted set stay unique (the member is derived from the current millisecond).
func TestCheckBurstLimit(t *testing.T) {
	rdb := newMiniredis(t)
	l := New(rdb, 100, 2)
	ctx := context.Background()
	user := uuid.New()

	for i := 0; i < 2; i++ {
		if res := l.Check(ctx, user); !res.Allowed {
			t.Fatalf("request %d should be allowed", i)
		}
		time.Sleep(5 * time.Millisecond)
	}

	res := l.Check(ctx, user)
	if res.Allowed {
		t.Fatalf("third request should be burst-limited, got %+v", res)
	}
	if res.RetryAfter <= 0 {
		t.Fatalf("denied request should carry a RetryAfter, got %v", res.RetryAfter)
	}
}
