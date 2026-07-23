package preview

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type changingProber struct {
	mu      sync.Mutex
	err     error
	entered chan struct{}
}

func (p *changingProber) Probe(ctx context.Context, _ Target) error {
	if p.entered != nil {
		select {
		case p.entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func TestMonitorProducesMonotonicReadinessObservations(t *testing.T) {
	prober := &changingProber{err: errors.New("down")}
	registry, err := New(Config{Prober: prober})
	if err != nil {
		t.Fatal(err)
	}
	registered, _ := registry.Register("p-abcdefghijklmnopqrstuvwxyz", "env", "web", Target{"127.0.0.1", 3000}, true)
	monitor, _ := NewMonitor(MonitorConfig{Registry: registry})
	if err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	degraded, _ := registry.Get(registered.Identity)
	if degraded.State != Degraded || degraded.Revision <= registered.Revision {
		t.Fatalf("degraded=%#v", degraded)
	}
	prober.mu.Lock()
	prober.err = nil
	prober.mu.Unlock()
	if err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	ready, _ := registry.Get(registered.Identity)
	if ready.State != Ready || ready.Revision <= degraded.Revision {
		t.Fatalf("ready=%#v", ready)
	}
	if err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	heartbeat, _ := registry.Get(registered.Identity)
	if heartbeat.State != Ready || heartbeat.Revision <= ready.Revision {
		t.Fatalf("readiness heartbeat=%#v", heartbeat)
	}
}

func TestMonitorShutdownCancelsProbe(t *testing.T) {
	prober := &changingProber{entered: make(chan struct{}, 1)}
	registry, _ := New(Config{Prober: prober, ProbeTimeout: time.Hour})
	_, _ = registry.Register("p-abcdefghijklmnopqrstuvwxyz", "env", "web", Target{"127.0.0.1", 3000}, true)
	monitor, _ := NewMonitor(MonitorConfig{Registry: registry, Interval: time.Hour})
	if err := monitor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-prober.entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := monitor.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
