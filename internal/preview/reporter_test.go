package preview

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type observationSenderFunc func(context.Context, Observation) error

func (f observationSenderFunc) Send(ctx context.Context, observation Observation) error {
	return f(ctx, observation)
}

func TestReporterRetriesStableObservationBeforeNewRevision(t *testing.T) {
	clock := &fixedClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	registry, err := New(Config{Clock: clock, Prober: &fakeProber{}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := registry.Register("p-abcdefghijklmnopqrstuvwxyz", "env", "web", Target{"127.0.0.1", 3000}, true)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("control plane unavailable")
	var mu sync.Mutex
	var sent []Observation
	reporter, err := NewReporter(ReporterConfig{Registry: registry, Sender: observationSenderFunc(func(_ context.Context, observation Observation) error {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, observation)
		if len(sent) == 1 {
			return failure
		}
		return nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if available, err := reporter.DeliverOnce(context.Background()); !available || !errors.Is(err, failure) {
		t.Fatalf("available=%v err=%v", available, err)
	}
	clock.now = clock.now.Add(time.Second)
	second, err := registry.Register(first.Identity, "env", "web", Target{"127.0.0.1", 3001}, true)
	if err != nil || second.Revision <= first.Revision {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	if _, err := reporter.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := reporter.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 3 || sent[0] != sent[1] || sent[2].Revision != second.Revision || sent[2].Target.Port != 3001 {
		t.Fatalf("sent=%#v", sent)
	}
}

func TestReporterLifecycleStopsBoundedSend(t *testing.T) {
	registry, _ := New(Config{Prober: &fakeProber{}})
	_, _ = registry.Register("p-abcdefghijklmnopqrstuvwxyz", "env", "web", Target{"127.0.0.1", 3000}, true)
	entered := make(chan struct{})
	reporter, err := NewReporter(ReporterConfig{Registry: registry, Interval: time.Hour, Timeout: time.Hour, Sender: observationSenderFunc(func(ctx context.Context, _ Observation) error { close(entered); <-ctx.Done(); return ctx.Err() })})
	if err != nil {
		t.Fatal(err)
	}
	if err := reporter.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := reporter.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestReporterDropsPermanentlyRejectedObservationForNewerRevision(t *testing.T) {
	clock := &fixedClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	registry, _ := New(Config{Clock: clock, Prober: &fakeProber{}})
	first, _ := registry.Register("p-abcdefghijklmnopqrstuvwxyz", "env", "web", Target{"127.0.0.1", 3000}, true)
	var sent []Observation
	reporter, _ := NewReporter(ReporterConfig{Registry: registry, Sender: observationSenderFunc(func(_ context.Context, observation Observation) error {
		sent = append(sent, observation)
		if len(sent) == 1 {
			return &ObservationRejectedError{StatusCode: 400}
		}
		return nil
	})})
	if _, err := reporter.DeliverOnce(context.Background()); !IsPermanentObservationError(err) {
		t.Fatalf("permanent rejection = %v", err)
	}
	if available, err := reporter.DeliverOnce(context.Background()); available || err != nil {
		t.Fatalf("permanently rejected revision was retried: available=%v err=%v", available, err)
	}
	clock.now = clock.now.Add(time.Second)
	second, _ := registry.Probe(context.Background(), first.Identity)
	if _, err := reporter.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || sent[0].Revision != first.Revision || sent[1].Revision != second.Revision {
		t.Fatalf("sent=%#v", sent)
	}
}
