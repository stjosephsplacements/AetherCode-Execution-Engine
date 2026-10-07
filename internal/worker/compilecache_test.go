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
