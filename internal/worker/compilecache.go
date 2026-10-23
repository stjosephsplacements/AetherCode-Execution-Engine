package worker

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"sync"
	"time"
)

type cacheEntry struct {
	fileID  string
	created time.Time
}

type compileCache struct {
	mu  sync.Mutex
	m   map[string]cacheEntry
	ttl time.Duration
}

func newCompileCache(ttl time.Duration) *compileCache {
	return &compileCache{
		m:   make(map[string]cacheEntry),
		ttl: ttl,
	}
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
