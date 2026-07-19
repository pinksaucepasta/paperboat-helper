package connector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fakeConnection struct {
	mu             sync.Mutex
	drains, closes int
	drainErr       error
	done           chan error
}

func (c *fakeConnection) Drain(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drains++
	return c.drainErr
}
func (c *fakeConnection) Close() error { c.mu.Lock(); defer c.mu.Unlock(); c.closes++; return nil }
func (c *fakeConnection) Done() <-chan error {
	if c.done == nil {
		c.done = make(chan error)
	}
	return c.done
}

type fakeDialer struct {
	mu              sync.Mutex
	started         chan struct{}
	startOnce       sync.Once
	calls           []Transport
	admissions      []Admission
	quicErr, tcpErr error
	connections     []*fakeConnection
	block           chan struct{}
}

func (d *fakeDialer) Dial(ctx context.Context, transport Transport, admission Admission) (Connection, error) {
	if d.started != nil {
		d.startOnce.Do(func() { close(d.started) })
	}
	d.mu.Lock()
	d.calls = append(d.calls, transport)
	d.admissions = append(d.admissions, admission)
	var err error
	if transport == QUIC {
		err = d.quicErr
	} else {
		err = d.tcpErr
	}
	connection := &fakeConnection{done: make(chan error, 1)}
	d.connections = append(d.connections, connection)
	block := d.block
	d.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	return connection, nil
}
func admission(generation uint64, jti string, now time.Time) Admission {
	return Admission{JTI: jti, Credential: "test-only-connector-admission-credential", EnvironmentID: "env", HelperID: "helper", Generation: generation, EdgePool: "default", EdgeNodeID: "edge_1", Endpoint: EdgeEndpoint{Host: "edge.test", Port: 7000}, Routes: []RouteHandoff{{RouteID: "route_1", Revision: 1, Kind: "helper_https_wss", PublicHost: "helper.test", ProxyName: "helper_1", LocalTarget: RouteTarget{Host: "127.0.0.1", Port: 8080}}}, ProtocolVersion: "1.0", ExpiresAt: now.Add(time.Minute)}
}
func manager(t *testing.T, dialer Dialer, now time.Time) *Manager {
	t.Helper()
	m, err := New(Config{EnvironmentID: "env", HelperID: "helper", EdgePool: "default", Dialer: dialer, Clock: fixedClock{now}, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestQUICFirstAndTCPFallback(t *testing.T) {
	now := time.Now()
	dialer := &fakeDialer{quicErr: errors.New("quic down")}
	m := manager(t, dialer, now)
	result, err := m.Accept(context.Background(), admission(1, "jti_0001", now))
	if err != nil || result.Transport != TCPTLS {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if got := m.ResourceCounts()["connectors"]; got != 1 {
		t.Fatalf("active connectors = %d", got)
	}
	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	if len(dialer.calls) != 2 || dialer.calls[0] != QUIC || dialer.calls[1] != TCPTLS {
		t.Fatalf("calls=%v", dialer.calls)
	}
	if dialer.admissions[1].Credential != "test-only-connector-admission-credential" {
		t.Fatal("admission credential was not carried to transport")
	}
}

func TestAdmissionReplayStaleAndBindingRejected(t *testing.T) {
	now := time.Now()
	m := manager(t, &fakeDialer{}, now)
	if _, err := m.Accept(context.Background(), admission(2, "jti_0001", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Accept(context.Background(), admission(3, "jti_0001", now)); !errors.Is(err, ErrAdmissionReplayed) {
		t.Fatalf("replay err=%v", err)
	}
	if _, err := m.Accept(context.Background(), admission(1, "jti_0002", now)); !errors.Is(err, ErrGenerationStale) {
		t.Fatalf("stale err=%v", err)
	}
	wrong := admission(3, "jti_0003", now)
	wrong.EnvironmentID = "other"
	if _, err := m.Accept(context.Background(), wrong); !errors.Is(err, ErrAdmissionInvalid) {
		t.Fatalf("binding err=%v", err)
	}
}

func TestReplacementDrainsAndClosesPreviousConnection(t *testing.T) {
	now := time.Now()
	dialer := &fakeDialer{}
	m := manager(t, dialer, now)
	m.Accept(context.Background(), admission(1, "jti_0001", now))
	result, err := m.Accept(context.Background(), admission(2, "jti_0002", now))
	if err != nil || !result.Replaced {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	first := dialer.connections[0]
	first.mu.Lock()
	defer first.mu.Unlock()
	if first.drains != 1 || first.closes != 1 {
		t.Fatalf("drains=%d closes=%d", first.drains, first.closes)
	}
}

func TestShutdownCancelsDialAndStopsAdmission(t *testing.T) {
	now := time.Now()
	dialer := &fakeDialer{block: make(chan struct{}), started: make(chan struct{})}
	m := manager(t, dialer, now)
	done := make(chan error)
	go func() { _, err := m.Accept(context.Background(), admission(1, "jti_0001", now)); done <- err }()
	<-dialer.started
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) && !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("accept err=%v", err)
	}
	if _, err := m.Accept(context.Background(), admission(2, "jti_0002", now)); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("post-shutdown err=%v", err)
	}
}
