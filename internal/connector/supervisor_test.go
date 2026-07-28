package connector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type admissionSource struct {
	mu         sync.Mutex
	now        time.Time
	generation uint64
	calls      chan uint64
}

func (s *admissionSource) Admission(context.Context) (Admission, error) {
	s.mu.Lock()
	s.generation++
	generation := s.generation
	s.mu.Unlock()
	select {
	case s.calls <- generation:
	default:
	}
	return admission(generation, "jti_000"+string(rune('0'+generation)), s.now), nil
}

type recordingWaiter struct {
	mu     sync.Mutex
	delays []time.Duration
}

type supervisorMetric struct {
	mu       sync.Mutex
	recovery float64
}

func (m *supervisorMetric) Record(name string, value float64, _ map[string]string) error {
	if name == "paperboat_helper_connector_recovery_seconds" {
		m.mu.Lock()
		m.recovery = value
		m.mu.Unlock()
	}
	return nil
}

func (w *recordingWaiter) Wait(ctx context.Context, delay time.Duration, wake <-chan struct{}) error {
	w.mu.Lock()
	w.delays = append(w.delays, delay)
	w.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	default:
		return nil
	}
}

type recoveringDialer struct {
	mu          sync.Mutex
	failDials   int
	calls       int
	connections []*fakeConnection
}

func (d *recoveringDialer) Dial(context.Context, Transport, Admission) (Connection, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.calls <= d.failDials {
		return nil, errors.New("network unavailable")
	}
	connection := &fakeConnection{done: make(chan error, 1)}
	d.connections = append(d.connections, connection)
	return connection, nil
}

func TestSupervisorFetchesFreshAdmissionWithCappedBackoffAndReconnects(t *testing.T) {
	now := time.Now()
	dialer := &recoveringDialer{failDials: 4}
	manager := manager(t, dialer, now)
	source := &admissionSource{now: now, calls: make(chan uint64, 16)}
	waits := &recordingWaiter{}
	metric := &supervisorMetric{}
	supervisor, err := NewSupervisor(SupervisorConfig{Manager: manager, Admissions: source, InitialBackoff: time.Second, MaxBackoff: 2 * time.Second, Jitter: 0.1, RandomFloat: func() float64 { return 0.5 }, Waiter: waits, Metrics: metric})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !manager.Status().Connected {
		select {
		case <-poll.C:
		case <-deadline:
			dialer.mu.Lock()
			calls := dialer.calls
			dialer.mu.Unlock()
			source.mu.Lock()
			generation := source.generation
			source.mu.Unlock()
			t.Fatalf("connector did not recover: status=%#v dial_calls=%d admissions=%d", manager.Status(), calls, generation)
		}
	}
	metric.mu.Lock()
	recovery := metric.recovery
	metric.mu.Unlock()
	if recovery <= 0 {
		t.Fatalf("connector recovery metric=%v", recovery)
	}
	first := manager.Status().Generation
	dialer.mu.Lock()
	active := dialer.connections[len(dialer.connections)-1]
	dialer.mu.Unlock()
	active.done <- errors.New("connection lost")
	deadline = time.After(time.Second)
	for manager.Status().Generation == first {
		select {
		case <-poll.C:
		case <-deadline:
			t.Fatal("connector did not reconnect")
		}
	}
	waits.mu.Lock()
	if len(waits.delays) < 2 || waits.delays[0] != time.Second || waits.delays[1] != 2*time.Second {
		t.Fatalf("delays=%v", waits.delays)
	}
	waits.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

type wakeWaiter struct{ entered chan struct{} }

func (w wakeWaiter) Wait(ctx context.Context, _ time.Duration, wake <-chan struct{}) error {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	}
}

type failingSource struct{}

func (failingSource) Admission(context.Context) (Admission, error) {
	return Admission{}, ErrUnavailable
}

func TestNetworkChangeInterruptsBackoff(t *testing.T) {
	now := time.Now()
	manager := manager(t, &fakeDialer{}, now)
	entered := make(chan struct{}, 2)
	supervisor, err := NewSupervisor(SupervisorConfig{Manager: manager, Admissions: failingSource{}, InitialBackoff: time.Hour, MaxBackoff: time.Hour, Jitter: 0.1, RandomFloat: func() float64 { return 0.5 }, Waiter: wakeWaiter{entered}})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-entered
	supervisor.NetworkChanged()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("network change did not interrupt backoff")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRouteChangeRefreshesActiveAdmission(t *testing.T) {
	now := time.Now()
	dialer := &recoveringDialer{}
	manager := manager(t, dialer, now)
	source := &admissionSource{now: now, calls: make(chan uint64, 16)}
	supervisor, err := NewSupervisor(SupervisorConfig{Manager: manager, Admissions: source})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-source.calls:
	case <-time.After(time.Second):
		t.Fatal("initial admission was not requested")
	}
	for !manager.Status().Connected {
		time.Sleep(time.Millisecond)
	}
	first := manager.Status().Generation
	supervisor.RoutesChanged()
	select {
	case <-source.calls:
	case <-time.After(time.Second):
		t.Fatal("route change did not refresh admission")
	}
	deadline := time.After(time.Second)
	for manager.Status().Generation == first {
		select {
		case <-deadline:
			t.Fatal("route change did not replace connector")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionExpiryDoesNotReplaceHealthyConnector(t *testing.T) {
	now := time.Now()
	dialer := &recoveringDialer{}
	manager := manager(t, dialer, now)
	// Admission expiry limits new handshakes. It does not terminate an
	// established connector or cause periodic connector replacement.
	source := &admissionSource{now: now.Add(-59*time.Second - 900*time.Millisecond), calls: make(chan uint64, 4)}
	supervisor, err := NewSupervisor(SupervisorConfig{Manager: manager, Admissions: source})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-source.calls:
	case <-time.After(time.Second):
		t.Fatal("initial admission was not requested")
	}
	time.Sleep(250 * time.Millisecond)
	select {
	case generation := <-source.calls:
		t.Fatalf("healthy connector was replaced at admission expiry: generation=%d", generation)
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
