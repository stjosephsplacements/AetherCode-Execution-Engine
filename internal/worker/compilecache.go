package worker

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const compileCacheRedisPrefix = "ac:compile:"

type cacheTier int

const (
	cacheTierMiss cacheTier = iota
	cacheTierL1
	cacheTierL2
	// cacheTierShared is an inflight wait that reused another job's compile.
	cacheTierShared
)

type cacheEntry struct {
	fileID  string
	created time.Time
}

type compileCache struct {
	mu       sync.Mutex
	m        map[string]cacheEntry
	inflight map[string]*compileFlight
	ttl      time.Duration
	rdb      *redis.Client
}

func newCompileCache(ttl time.Duration, rdb *redis.Client) *compileCache {
	return &compileCache{
		m:        make(map[string]cacheEntry),
		inflight: make(map[string]*compileFlight),
		ttl:      ttl,
		rdb:      rdb,
	}
}

// compileOutcome is what one compile produced: the cached binary's file id, or
// the compiler's diagnostics when the source does not compile.
type compileOutcome struct {
	fileID string
	failed bool
	stderr string
}

// compileFlight is one compile in progress, shared by every job that asked for
// the same source while it ran.
type compileFlight struct {
	done    chan struct{}
	outcome compileOutcome
	err     error
}

// compile returns the outcome for key, running run at most once for jobs that
// arrive together. Jobs that arrive while a compile is running wait for it
// instead of repeating it (shared is true for them), and jobs that arrive after
// it succeeds hit the cache. A source that does not compile is shared with the
// jobs that waited, because the same source fails the same way; it is not
// cached. If the leader's run fails for a reason of its own (the sandbox was
// unreachable, say), each waiter tries again for itself. With a zero ttl the
// cache is off and every call runs run.
func (c *compileCache) compile(ctx context.Context, key string, run func() (compileOutcome, error)) (outcome compileOutcome, tier cacheTier, err error) {
	if c.ttl == 0 {
		outcome, err = run()
		return outcome, cacheTierMiss, err
	}
	for {
		c.mu.Lock()
		if e, ok := c.m[key]; ok && time.Since(e.created) <= c.ttl {
			c.mu.Unlock()
			return compileOutcome{fileID: e.fileID}, cacheTierL1, nil
		}
		c.mu.Unlock()

		if fileID, ok := c.l2Get(ctx, key); ok {
			c.put(key, fileID)
			return compileOutcome{fileID: fileID}, cacheTierL2, nil
		}

		c.mu.Lock()
		flight, running := c.inflight[key]
		if !running {
			flight = &compileFlight{done: make(chan struct{})}
			c.inflight[key] = flight
			c.mu.Unlock()
			return c.lead(ctx, key, flight, run)
		}
		c.mu.Unlock()

		select {
		case <-flight.done:
			if flight.err == nil {
				return flight.outcome, cacheTierShared, nil
			}
			// The leader failed for its own reasons: try again, as the leader if no one else has.
		case <-ctx.Done():
			return compileOutcome{}, cacheTierMiss, ctx.Err()
		}
	}
}

func (c *compileCache) lead(ctx context.Context, key string, flight *compileFlight, run func() (compileOutcome, error)) (compileOutcome, cacheTier, error) {
	defer func() {
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		close(flight.done)
	}()
	outcome, err := run()
	flight.outcome, flight.err = outcome, err
	if err == nil && !outcome.failed {
		c.put(key, outcome.fileID)
		c.l2Put(ctx, key, outcome.fileID)
	}
	return outcome, cacheTierMiss, err
}

func (c *compileCache) l2Get(ctx context.Context, key string) (string, bool) {
	if c.rdb == nil || c.ttl == 0 {
		return "", false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	val, err := c.rdb.Get(ctx, compileCacheRedisPrefix+key).Result()
	if err != nil || val == "" {
		return "", false
	}
	return val, true
}

func (c *compileCache) l2Put(ctx context.Context, key, fileID string) {
	if c.rdb == nil || c.ttl == 0 || fileID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = c.rdb.Set(ctx, compileCacheRedisPrefix+key, fileID, c.ttl).Err()
}

func compileCacheKey(language, source string) string {
	return compileCacheKeyWithOptions(language, source, nil, nil)
}

func compileCacheKeyWithOptions(language, source string, files map[string]string, compilerFlags []string) string {
	h := sha256.New()
	writeLen := func(s string) {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(len(s)))
		h.Write(buf[:])
		h.Write([]byte(s))
	}
	writeLen(language)
	writeLen(source)
	if len(files) > 0 {
		names := make([]string, 0, len(files))
		for name := range files {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			writeLen(name)
			writeLen(files[name])
		}
	}
	if len(compilerFlags) > 0 {
		for _, flag := range compilerFlags {
			writeLen(flag)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (c *compileCache) get(key string) (string, bool) {
	if c.ttl == 0 {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok {
		return "", false
	}
	if time.Since(e.created) > c.ttl {
		delete(c.m, key)
		return "", false
	}
	return e.fileID, true
}

func (c *compileCache) put(key, fileID string) {
	if c.ttl == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[key] = cacheEntry{fileID: fileID, created: time.Now()}
}

// firstFileID returns the first value from a string map, or "" if the map is empty.
// go-judge returns exactly one file ID per cached output; this extracts it without
// relying on map iteration order being deterministic (it is not, but there is only one entry).
func firstFileID(m map[string]string) string {
	for _, id := range m {
		return id
	}
	return ""
}

func (c *compileCache) evictExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for k, e := range c.m {
		if now.Sub(e.created) > c.ttl {
			delete(c.m, k)
		}
	}
}
