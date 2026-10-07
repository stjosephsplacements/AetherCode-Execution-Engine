package worker

import (
	"crypto/sha256"
	"encoding/hex"
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
	h := sha256.New()
	h.Write([]byte(language))
	h.Write([]byte{0})
	h.Write([]byte(source))
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
