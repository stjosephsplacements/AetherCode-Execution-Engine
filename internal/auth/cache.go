package auth

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

type cacheEntry[V any] struct {
	value     V
	expiresAt time.Time
}

type ttlCache[V any] struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry[V]
	ttl     time.Duration
}

func newTTLCache[V any](ctx context.Context, ttl time.Duration) *ttlCache[V] {
	c := &ttlCache[V]{
		entries: make(map[string]cacheEntry[V]),
		ttl:     ttl,
	}
	go c.reapLoop(ctx)
	return c
}

func (c *ttlCache[V]) get(key string) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		var zero V
		return zero, false
	}
	return e.value, true
}

func (c *ttlCache[V]) set(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cacheEntry[V]{value: value, expiresAt: time.Now().Add(c.ttl)}
}

func (c *ttlCache[V]) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(c.ttl)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for k, e := range c.entries {
				if now.After(e.expiresAt) {
					delete(c.entries, k)
				}
			}
			c.mu.Unlock()
		}
	}
}

// initCaches creates the package-level caches tied to the middleware's lifecycle.
func initCaches(ctx context.Context) (tenant *ttlCache[uuid.UUID], user *ttlCache[uuid.UUID]) {
	return newTTLCache[uuid.UUID](ctx, 60*time.Second),
		newTTLCache[uuid.UUID](ctx, 60*time.Second)
}
