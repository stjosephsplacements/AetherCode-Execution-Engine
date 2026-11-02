package metrics

import (
	"math"
	"sync"
	"time"
)

// Drain measures how fast jobs finish, as a smoothed rate per second. The
// gateway turns it into an honest Retry-After when it sheds load.
type Drain struct {
	mu      sync.Mutex
	rate    float64 // smoothed completions per second
	window  time.Time
	count   int
	now     func() time.Time
	started bool
}

const (
	drainWindow = time.Second
	// drainAlpha weights the newest second against the history.
	drainAlpha = 0.3
)

// NewDrain returns a tracker that reads time from now.
func NewDrain(now func() time.Time) *Drain {
	return &Drain{now: now}
}

// Complete records one finished job.
func (d *Drain) Complete() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.advance()
	d.count++
}

// Rate is the smoothed completions per second; zero until a full second passed.
func (d *Drain) Rate() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.advance()
	return d.rate
}

// RetryAfter is how many seconds a caller should wait for depth queued jobs to
// drain, between lo and hi. With no measured rate it returns lo.
func (d *Drain) RetryAfter(depth int64, lo, hi int) int {
	rate := d.Rate()
	if rate <= 0 || depth <= 0 {
		return lo
	}
	seconds := int(math.Ceil(float64(depth) / rate))
	return max(lo, min(hi, seconds))
}

// advance folds every full elapsed window into the smoothed rate. Idle windows
// decay it toward zero.
func (d *Drain) advance() {
	now := d.now()
	if !d.started {
		d.started, d.window = true, now
		return
	}
	for now.Sub(d.window) >= drainWindow {
		d.rate = drainAlpha*float64(d.count) + (1-drainAlpha)*d.rate
		d.count = 0
		d.window = d.window.Add(drainWindow)
	}
}

// JobsDrain is the process-wide tracker: workers feed it, the gateway reads it.
var JobsDrain = NewDrain(time.Now)
