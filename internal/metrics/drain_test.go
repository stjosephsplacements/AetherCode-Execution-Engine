package metrics

import (
	"testing"
	"time"
)

func TestDrainRateAndRetryAfter(t *testing.T) {
	now := time.Unix(1000, 0)
	d := NewDrain(func() time.Time { return now })

	if got := d.RetryAfter(500, 1, 60); got != 1 {
		t.Fatalf("RetryAfter with no measurements = %d, want the lower bound", got)
	}
	d.Complete() // starts the first window
	for i := 0; i < 100; i++ {
		d.Complete()
	}
	now = now.Add(time.Second)
	rate := d.Rate()
	if rate < 25 || rate > 35 {
		t.Fatalf("rate after one 101-job second = %.1f, want about 30 (alpha 0.3)", rate)
	}
	if got := d.RetryAfter(600, 1, 60); got < 15 || got > 25 {
		t.Fatalf("RetryAfter(600) = %d, want about 20 at 30 jobs/s", got)
	}
	if got := d.RetryAfter(1_000_000, 1, 60); got != 60 {
		t.Fatalf("RetryAfter above the cap = %d, want 60", got)
	}
	if got := d.RetryAfter(0, 2, 60); got != 2 {
		t.Fatalf("RetryAfter on an empty queue = %d, want the lower bound", got)
	}
	now = now.Add(30 * time.Second)
	if d.Rate() > 0.1 {
		t.Fatalf("rate after 30 idle seconds = %.3f, want it to have decayed", d.Rate())
	}
}
