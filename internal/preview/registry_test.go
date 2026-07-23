package preview

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

type fakeProber struct {
	mu      sync.Mutex
	err     error
	started chan struct{}
	release chan struct{}
}

func (p *fakeProber) Probe(ctx context.Context, _ Target) error {
	if p.started != nil {
		p.mu.Lock()
		started := p.started
		p.started = nil
		p.mu.Unlock()
		close(started)
	}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.err
}
func newRegistry(t *testing.T, prober Prober, max int) *Registry {
	t.Helper()
	r, err := New(Config{Clock: &fixedClock{time.Now()}, Prober: prober, MaxConcurrentProbes: max})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRegisterRequiresPublicAcknowledgementAndLoopback(t *testing.T) {
	r := newRegistry(t, &fakeProber{}, 1)
	for _, tc := range []struct {
		target Target
		ack    bool
	}{{Target{"0.0.0.0", 3000}, true}, {Target{"127.0.0.1", 3000}, false}} {
		if _, err := r.Register("prv", "env", "web", tc.target, tc.ack); !errors.Is(err, ErrInvalidTarget) {
			t.Fatalf("err=%v", err)
		}
	}
}

func TestListEnvironmentDoesNotDiscloseOtherEnvironment(t *testing.T) {
	r := newRegistry(t, &fakeProber{}, 1)
	if _, err := r.Register("prv-a", "env-a", "web", Target{"127.0.0.1", 3000}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register("prv-b", "env-b", "web", Target{"127.0.0.1", 3001}, true); err != nil {
		t.Fatal(err)
	}
	items := r.ListEnvironment("env-a")
	if len(items) != 1 || items[0].Identity != "prv-a" {
		t.Fatalf("items=%#v", items)
	}
}

func TestTargetChangePreservesIdentityAndRequiresNewProbe(t *testing.T) {
	r := newRegistry(t, &fakeProber{}, 1)
	record, err := r.Register("prv", "env", "web", Target{"127.0.0.1", 3000}, true)
	if err != nil {
		t.Fatal(err)
	}
	record, err = r.Probe(context.Background(), record.Identity)
	if err != nil || record.State != Ready {
		t.Fatalf("record=%#v err=%v", record, err)
	}
	changed, err := r.Register("prv", "env", "web", Target{"127.0.0.1", 4000}, true)
	if err != nil || changed.Identity != "prv" || changed.State != Degraded || changed.Reason != "target_changed" {
		t.Fatalf("changed=%#v err=%v", changed, err)
	}
}

func TestProbeFailureAndRemovalAreDistinct(t *testing.T) {
	prober := &fakeProber{err: errors.New("down")}
	r := newRegistry(t, prober, 1)
	r.Register("prv", "env", "web", Target{"::1", 3000}, true)
	if got := r.ResourceCounts()["previews"]; got != 1 {
		t.Fatalf("active previews = %d", got)
	}
	record, err := r.Probe(context.Background(), "prv")
	if err != nil || record.State != Degraded || record.Reason != "target_unhealthy" {
		t.Fatalf("record=%#v err=%v", record, err)
	}
	removed, err := r.Remove("prv")
	if err != nil || removed.State != Removed {
		t.Fatalf("removed=%#v err=%v", removed, err)
	}
	if _, err := r.Get("prv"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get err=%v", err)
	}
	if got := r.ResourceCounts()["previews"]; got != 0 {
		t.Fatalf("active previews after removal = %d", got)
	}
}

func TestProbeAdmissionAndStaleResultDoNotOverwriteTargetChange(t *testing.T) {
	started := make(chan struct{})
	prober := &fakeProber{started: started, release: make(chan struct{})}
	r := newRegistry(t, prober, 1)
	r.Register("prv", "env", "web", Target{"127.0.0.1", 3000}, true)
	done := make(chan Record)
	go func() { record, _ := r.Probe(context.Background(), "prv"); done <- record }()
	<-started
	if _, err := r.Probe(context.Background(), "prv"); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("admission err=%v", err)
	}
	changed, err := r.Register("prv", "env", "web", Target{"127.0.0.1", 4000}, true)
	if err != nil {
		t.Fatal(err)
	}
	close(prober.release)
	result := <-done
	if result.Target.Port != changed.Target.Port || result.State != changed.State {
		t.Fatalf("result=%#v changed=%#v", result, changed)
	}
}
