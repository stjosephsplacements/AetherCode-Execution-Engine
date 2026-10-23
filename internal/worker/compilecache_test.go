package worker

import (
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
