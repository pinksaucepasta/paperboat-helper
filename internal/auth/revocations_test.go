package auth

import (
	"errors"
	"strings"
	"testing"
)

func TestRevocationCacheUpdatesActiveWatchers(t *testing.T) {
	cache := NewRevocationCache()
	flag, signal, release, err := cache.Watch("jti_active")
	if err != nil || flag.Load() {
		t.Fatalf("initial watch flag=%v err=%v", flag.Load(), err)
	}
	if err := cache.Replace([]string{"jti_active"}); err != nil {
		t.Fatal(err)
	}
	if !flag.Load() || !cache.Revoked(Claims{JTI: "jti_active"}) {
		t.Fatal("active credential was not revoked")
	}
	select {
	case <-signal:
	default:
		t.Fatal("active credential did not receive immediate revocation signal")
	}
	release()
	release()
	cache.mu.RLock()
	watchers := len(cache.watchers)
	cache.mu.RUnlock()
	if watchers != 0 {
		t.Fatalf("watchers=%d, want 0", watchers)
	}
}

func TestRevocationCacheRejectsInvalidSnapshots(t *testing.T) {
	cache := NewRevocationCache()
	if err := cache.Replace([]string{""}); !errors.Is(err, ErrRevocationsInvalid) {
		t.Fatalf("empty JTI error=%v", err)
	}
	tooMany := make([]string, MaxRevokedJTIs+1)
	for index := range tooMany {
		tooMany[index] = strings.Repeat("x", 8)
	}
	if err := cache.Replace(tooMany); !errors.Is(err, ErrRevocationsInvalid) {
		t.Fatalf("oversized snapshot error=%v", err)
	}
}
