package auth

import (
	"sync"
	"time"
)

type ReplayCache struct {
	mu      sync.Mutex
	entries map[string]time.Time
	max     int
	clock   Clock
}

func NewReplayCache(max int, clock Clock) *ReplayCache {
	return &ReplayCache{entries: make(map[string]time.Time), max: max, clock: clock}
}

func (c *ReplayCache) Consume(jti string, expires time.Time) bool {
	if c == nil || jti == "" || !expires.After(c.clock.Now()) || c.max < 1 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	for key, expiry := range c.entries {
		if !expiry.After(now) {
			delete(c.entries, key)
		}
	}
	if _, exists := c.entries[jti]; exists || len(c.entries) >= c.max {
		return false
	}
	c.entries[jti] = expires
	return true
}
