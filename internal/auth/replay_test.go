package auth

import (
	"testing"
	"time"
)

type replayClock struct{ now time.Time }

func (c *replayClock) Now() time.Time { return c.now }

func TestReplayCacheConsumesOnceExpiresAndBounds(t *testing.T) {
	clock := &replayClock{now: time.Unix(100, 0)}
	cache := NewReplayCache(1, clock)
	if !cache.Consume("jti_1", clock.now.Add(time.Minute)) || cache.Consume("jti_1", clock.now.Add(time.Minute)) {
		t.Fatal("replay was not rejected")
	}
	if cache.Consume("jti_2", clock.now.Add(time.Minute)) {
		t.Fatal("capacity was not enforced")
	}
	clock.now = clock.now.Add(2 * time.Minute)
	if !cache.Consume("jti_2", clock.now.Add(time.Minute)) {
		t.Fatal("expired replay did not release capacity")
	}
}
