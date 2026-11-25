package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testCompileCacheRedis(t *testing.T) (*miniredis.Miniredis, *compileCache) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return mr, newCompileCache(time.Minute, rdb)
}

func TestCompileCache_L1Hit(t *testing.T) {
	t.Parallel()
	_, c := testCompileCacheRedis(t)
	key := compileCacheKey("cpp", "int main(){}")
	c.put(key, "file-l1")

	var runs atomic.Int32
	out, tier, err := c.compile(context.Background(), key, func() (compileOutcome, error) {
		runs.Add(1)
		return compileOutcome{fileID: "never"}, nil
	})
	if err != nil || tier != cacheTierL1 || out.fileID != "file-l1" || runs.Load() != 0 {
		t.Fatalf("out=%+v tier=%v err=%v runs=%d", out, tier, err, runs.Load())
	}
}

func TestCompileCache_L2Hit(t *testing.T) {
	t.Parallel()
	mr, c := testCompileCacheRedis(t)
	key := compileCacheKey("go", "package main\nfunc main(){}")
	mr.Set(compileCacheRedisPrefix+key, "file-l2")

	var runs atomic.Int32
	out, tier, err := c.compile(context.Background(), key, func() (compileOutcome, error) {
		runs.Add(1)
		return compileOutcome{fileID: "never"}, nil
	})
	if err != nil || tier != cacheTierL2 || out.fileID != "file-l2" || runs.Load() != 0 {
		t.Fatalf("out=%+v tier=%v err=%v runs=%d", out, tier, err, runs.Load())
	}
	fid, ok := c.get(key)
	if !ok || fid != "file-l2" {
		t.Fatalf("L2 hit should warm L1: got %q ok=%v", fid, ok)
	}
}

func TestCompileCache_L2MissCompilesAndSetsRedis(t *testing.T) {
	t.Parallel()
	mr, c := testCompileCacheRedis(t)
	key := compileCacheKey("rust", "fn main(){}")
	if mr.Exists(compileCacheRedisPrefix + key) {
		t.Fatal("expected L2 miss before compile")
	}

	out, tier, err := c.compile(context.Background(), key, func() (compileOutcome, error) {
		return compileOutcome{fileID: "file-new"}, nil
	})
	if err != nil || tier != cacheTierMiss || out.fileID != "file-new" {
		t.Fatalf("out=%+v tier=%v err=%v", out, tier, err)
	}
	mr.CheckGet(t, compileCacheRedisPrefix+key, "file-new")
}

func TestCompileCache_L2UnavailableFallsBackToL1Only(t *testing.T) {
	t.Parallel()
	rdb := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:1",
		DialTimeout:  10 * time.Millisecond,
		ReadTimeout:  10 * time.Millisecond,
		WriteTimeout: 10 * time.Millisecond,
		MaxRetries:   0,
	})
	c := newCompileCache(time.Minute, rdb)
	key := compileCacheKey("c", "int main(){return 0;}")

	out, tier, err := c.compile(context.Background(), key, func() (compileOutcome, error) {
		return compileOutcome{fileID: "file-only"}, nil
	})
	if err != nil || tier != cacheTierMiss || out.fileID != "file-only" {
		t.Fatalf("out=%+v tier=%v err=%v", out, tier, err)
	}
	var runs atomic.Int32
	out, tier, _ = c.compile(context.Background(), key, func() (compileOutcome, error) {
		runs.Add(1)
		return compileOutcome{fileID: "never"}, nil
	})
	if tier != cacheTierL1 || out.fileID != "file-only" || runs.Load() != 0 {
		t.Fatalf("second call should hit L1: %+v tier=%v runs=%d", out, tier, runs.Load())
	}
}