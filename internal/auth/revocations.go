package auth

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrRevocationsInvalid = errors.New("invalid revocation document")

const MaxRevokedJTIs = 10000

// RevocationCache keeps the current server snapshot and notifies active credentials.
// Terminal data frames read only the returned atomic flag.
type RevocationCache struct {
	mu       sync.RWMutex
	revoked  map[string]struct{}
	watchers map[string]map[*revocationWatcher]struct{}
}

type revocationWatcher struct {
	flag   atomic.Bool
	signal chan struct{}
	once   sync.Once
}

func NewRevocationCache() *RevocationCache {
	return &RevocationCache{revoked: make(map[string]struct{}), watchers: make(map[string]map[*revocationWatcher]struct{})}
}

func (c *RevocationCache) Revoked(claims Claims) bool {
	if c == nil || claims.JTI == "" {
		return false
	}
	c.mu.RLock()
	_, revoked := c.revoked[claims.JTI]
	c.mu.RUnlock()
	return revoked
}

func (c *RevocationCache) Watch(jti string) (*atomic.Bool, <-chan struct{}, func(), error) {
	if c == nil || jti == "" {
		return nil, nil, nil, ErrRevocationsInvalid
	}
	watcher := &revocationWatcher{signal: make(chan struct{})}
	c.mu.Lock()
	if _, revoked := c.revoked[jti]; revoked {
		watcher.flag.Store(true)
		watcher.once.Do(func() { close(watcher.signal) })
	}
	if c.watchers[jti] == nil {
		c.watchers[jti] = make(map[*revocationWatcher]struct{})
	}
	c.watchers[jti][watcher] = struct{}{}
	c.mu.Unlock()
	var once sync.Once
	return &watcher.flag, watcher.signal, func() {
		once.Do(func() {
			c.mu.Lock()
			delete(c.watchers[jti], watcher)
			if len(c.watchers[jti]) == 0 {
				delete(c.watchers, jti)
			}
			c.mu.Unlock()
		})
	}, nil
}

func (c *RevocationCache) Replace(jtis []string) error {
	if c == nil || len(jtis) > MaxRevokedJTIs {
		return ErrRevocationsInvalid
	}
	next := make(map[string]struct{}, len(jtis))
	for _, jti := range jtis {
		if jti == "" {
			return ErrRevocationsInvalid
		}
		next[jti] = struct{}{}
	}
	c.mu.Lock()
	c.revoked = next
	for jti := range next {
		for watcher := range c.watchers[jti] {
			watcher.flag.Store(true)
			watcher.once.Do(func() { close(watcher.signal) })
		}
	}
	c.mu.Unlock()
	return nil
}
