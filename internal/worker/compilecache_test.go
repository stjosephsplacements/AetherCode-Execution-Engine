package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompileCacheHitAndMiss(t *testing.T) {
	c := newCompileCache(time.Minute)
	key := compileCacheKey("cpp", "int main(){}")

	if _, ok := c.get(key); ok {
		t.Fatal("expected miss on empty cache")
	}

	c.put(key, "file-abc")
	fid, ok := c.get(key)
	if !ok || fid != "file-abc" {
		t.Fatalf("expected hit with file-abc, got %q ok=%v", fid, ok)
	}
}

func TestCompileCacheExpiry(t *testing.T) {
	c := newCompileCache(10 * time.Millisecond)
	key := compileCacheKey("python", "print(1)")

	c.put(key, "file-xyz")
	time.Sleep(20 * time.Millisecond)

	if _, ok := c.get(key); ok {
		t.Fatal("expected miss after TTL expiry")
	}
}

func TestCompileCacheDisabledWhenZeroTTL(t *testing.T) {
	c := newCompileCache(0)
	key := compileCacheKey("c", "int main(){}")

	c.put(key, "file-999")
	if _, ok := c.get(key); ok {
		t.Fatal("expected miss when TTL is zero (cache disabled)")
	}
}

func TestCompileCacheEvictExpired(t *testing.T) {
	c := newCompileCache(10 * time.Millisecond)
	c.put(compileCacheKey("a", "1"), "f1")
	c.put(compileCacheKey("b", "2"), "f2")

	time.Sleep(20 * time.Millisecond)
	c.evictExpired()

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) != 0 {
		t.Fatalf("expected empty map after evict, got %d entries", len(c.m))
	}
}

func TestCompileCacheKeyDeterministic(t *testing.T) {
	a := compileCacheKey("cpp", "int main(){}")
	b := compileCacheKey("cpp", "int main(){}")
	if a != b {
		t.Fatalf("same input produced different keys: %s vs %s", a, b)
	}

	c := compileCacheKey("c", "int main(){}")
	if a == c {
		t.Fatal("different languages should produce different keys")
	}
}

func BenchmarkCompileCacheGet_Hit(b *testing.B) {
	c := newCompileCache(time.Hour)
	key := compileCacheKey("cpp", "int main(){ return 0; }")
	c.put(key, "file-bench-hit")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fid, ok := c.get(key)
		if !ok || fid != "file-bench-hit" {
			b.Fatalf("expected hit, got %q ok=%v", fid, ok)
		}
	}
}

func BenchmarkCompileCacheGet_Miss(b *testing.B) {
	c := newCompileCache(time.Hour)
	key := "non-existent-key"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := c.get(key); ok {
			b.Fatal("expected miss")
		}
	}
}

func BenchmarkCompileCachePut(b *testing.B) {
	c := newCompileCache(time.Hour)
	// Pre-generate keys so we only benchmark put
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = compileCacheKey("cpp", string(rune(i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.put(keys[i], "file-benchmark-put")
	}
}

func BenchmarkCompileCacheEvict_1000Entries(b *testing.B) {
	b.StopTimer()
	// Pre-generate keys
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = compileCacheKey("lang", string(rune(i)))
	}

	for i := 0; i < b.N; i++ {
		c := newCompileCache(time.Nanosecond)
		for j := 0; j < 1000; j++ {
			c.m[keys[j]] = cacheEntry{
				fileID:  "expired-file",
				created: time.Now().Add(-time.Hour),
			}
		}

		b.StartTimer()
		c.evictExpired()
		b.StopTimer()
	}
}

func TestFirstFileID(t *testing.T) {
	cases := []struct {
		m    map[string]string
		want string
	}{
		{nil, ""},
		{map[string]string{}, ""},
		{map[string]string{"/w/a.out": "abc123"}, "abc123"},
	}
	for _, tc := range cases {
		if got := firstFileID(tc.m); got != tc.want {
			t.Errorf("firstFileID(%v) = %q; want %q", tc.m, got, tc.want)
		}
	}
}

func BenchmarkCompileCacheKey(b *testing.B) {
	largeSourceBytes := make([]byte, 64*1024)
	for i := range largeSourceBytes {
		largeSourceBytes[i] = byte('a' + (i % 26))
	}
	largeSource := string(largeSourceBytes)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = compileCacheKey("python", largeSource)
	}
}

func TestCompileCache_ConcurrentJobsShareOneCompile(t *testing.T) {
	t.Parallel()
	c := newCompileCache(time.Minute)
	var runs atomic.Int32
	release := make(chan struct{})
	run := func() (compileOutcome, error) {
		runs.Add(1)
		<-release
		return compileOutcome{fileID: "bin-1"}, nil
	}

	const callers = 20
	var wg sync.WaitGroup
	shared := make([]bool, callers)
	ids := make([]string, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, sh, err := c.compile(context.Background(), "k", run)
			if err != nil {
				t.Errorf("compile: %v", err)
			}
			shared[i], ids[i] = sh, out.fileID
		}()
	}
	time.Sleep(100 * time.Millisecond) // let every caller reach the wait
	close(release)
	wg.Wait()

	if runs.Load() != 1 {
		t.Fatalf("compile ran %d times for %d concurrent jobs, want once", runs.Load(), callers)
	}
	leaders := 0
	for i := range callers {
		if ids[i] != "bin-1" {
			t.Errorf("caller %d got %q", i, ids[i])
		}
		if !shared[i] {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("%d callers report having compiled, want exactly 1", leaders)
	}
	// Later jobs hit the cache without compiling.
	if out, sh, _ := c.compile(context.Background(), "k", run); !sh || out.fileID != "bin-1" || runs.Load() != 1 {
		t.Errorf("a later job should hit the cache: %+v shared=%v runs=%d", out, sh, runs.Load())
	}
}

func TestCompileCache_ASourceThatDoesNotCompileIsSharedButNotCached(t *testing.T) {
	t.Parallel()
	c := newCompileCache(time.Minute)
	var runs atomic.Int32
	release := make(chan struct{})
	run := func() (compileOutcome, error) {
		runs.Add(1)
		<-release
		return compileOutcome{failed: true, stderr: "syntax error"}, nil
	}
	results := make(chan compileOutcome, 2)
	for range 2 {
		go func() {
			out, _, _ := c.compile(context.Background(), "bad", run)
			results <- out
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	for range 2 {
		if out := <-results; !out.failed || out.stderr != "syntax error" {
			t.Fatalf("outcome = %+v, want the shared compile failure", out)
		}
	}
	if runs.Load() != 1 {
		t.Fatalf("ran %d times, want once", runs.Load())
	}
	// A failure is not cached: the next job compiles again.
	if _, shared, _ := c.compile(context.Background(), "bad", func() (compileOutcome, error) { runs.Add(1); return compileOutcome{failed: true}, nil }); shared || runs.Load() != 2 {
		t.Fatalf("the failed source was cached: shared=%v runs=%d", shared, runs.Load())
	}
}

func TestCompileCache_WaitersRetryWhenTheLeaderHitsATransientError(t *testing.T) {
	t.Parallel()
	c := newCompileCache(time.Minute)
	var runs atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	leader := func() (compileOutcome, error) {
		runs.Add(1)
		close(started)
		<-release
		return compileOutcome{}, errors.New("sandbox unreachable")
	}
	errCh := make(chan error, 1)
	go func() { _, _, err := c.compile(context.Background(), "k", leader); errCh <- err }()
	<-started
	waiter := make(chan compileOutcome, 1)
	go func() {
		out, _, _ := c.compile(context.Background(), "k", func() (compileOutcome, error) {
			runs.Add(1)
			return compileOutcome{fileID: "bin-2"}, nil
		})
		waiter <- out
	}()
	time.Sleep(100 * time.Millisecond)
	close(release)
	if err := <-errCh; err == nil {
		t.Fatal("the leader's error was swallowed")
	}
	if out := <-waiter; out.fileID != "bin-2" {
		t.Fatalf("waiter got %+v, want its own successful compile after the leader failed", out)
	}
	if runs.Load() != 2 {
		t.Fatalf("ran %d times, want the failed leader plus one retry", runs.Load())
	}
}

func TestCompileCache_AWaiterGivesUpWhenItsContextEnds(t *testing.T) {
	t.Parallel()
	c := newCompileCache(time.Minute)
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _, _ = c.compile(context.Background(), "k", func() (compileOutcome, error) {
			close(started)
			<-release
			return compileOutcome{fileID: "x"}, nil
		})
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := c.compile(ctx, "k", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the waiter's deadline", err)
	}
	close(release)
}

func TestCompileCache_ZeroTTLCompilesEveryTime(t *testing.T) {
	t.Parallel()
	c := newCompileCache(0)
	var runs int
	for range 3 {
		_, shared, _ := c.compile(context.Background(), "k", func() (compileOutcome, error) { runs++; return compileOutcome{fileID: "x"}, nil })
		if shared {
			t.Fatal("a disabled cache reported a shared compile")
		}
	}
	if runs != 3 {
		t.Fatalf("ran %d times, want 3", runs)
	}
}
