package connector

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type fakeFRPClient struct {
	running  atomic.Bool
	closed   atomic.Bool
	graceful atomic.Bool
	exited   atomic.Bool
}

type drainingFRPClient struct {
	done     chan struct{}
	duration atomic.Int64
}

func (c *drainingFRPClient) Run(context.Context) error { <-c.done; return nil }
func (c *drainingFRPClient) Close()                    {}
func (c *drainingFRPClient) GracefulClose(duration time.Duration) {
	c.duration.Store(int64(duration))
	time.Sleep(duration)
	close(c.done)
}
func (*drainingFRPClient) ProxyRunning(string) bool { return true }

func (c *fakeFRPClient) Run(ctx context.Context) error {
	<-ctx.Done()
	c.exited.Store(true)
	return ctx.Err()
}
func (c *fakeFRPClient) Close()                      { c.closed.Store(true) }
func (c *fakeFRPClient) GracefulClose(time.Duration) { c.graceful.Store(true) }
func (c *fakeFRPClient) ProxyRunning(string) bool    { return c.running.Load() }

func TestFRPDialerReturnsOnlyAfterProxyReadiness(t *testing.T) {
	now := time.Now()
	client := &fakeFRPClient{}
	dialer, err := NewFRPDialer(FRPDialerConfig{ReadyTimeout: time.Second, Factory: func(got Admission, transport Transport) (FRPClient, error) {
		if transport != QUIC || got.Endpoint.Host != "edge.test" || got.Routes[0].ProxyName != "helper_1" {
			t.Fatalf("admission=%+v transport=%s", got, transport)
		}
		return client, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan Connection, 1)
	go func() {
		connection, _ := dialer.Dial(context.Background(), QUIC, admission(1, "jti_ready", now))
		done <- connection
	}()
	select {
	case <-done:
		t.Fatal("dial returned before proxy readiness")
	case <-time.After(20 * time.Millisecond):
	}
	client.running.Store(true)
	select {
	case connection := <-done:
		if connection == nil {
			t.Fatal("nil connection")
		}
		_ = connection.Close()
	case <-time.After(time.Second):
		t.Fatal("dial did not observe readiness")
	}
}

func TestFRPDialerConnectionOutlivesReadinessContext(t *testing.T) {
	client := &fakeFRPClient{}
	client.running.Store(true)
	dialer, err := NewFRPDialer(FRPDialerConfig{ReadyTimeout: time.Second, Factory: func(Admission, Transport) (FRPClient, error) {
		return client, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	connection, err := dialer.Dial(ctx, TCPTLS, admission(1, "jti_lifetime", time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	if client.exited.Load() {
		t.Fatal("established FRP session inherited readiness cancellation")
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFRPDialerTimeoutClosesClient(t *testing.T) {
	client := &fakeFRPClient{}
	dialer, _ := NewFRPDialer(FRPDialerConfig{ReadyTimeout: 20 * time.Millisecond, Factory: func(Admission, Transport) (FRPClient, error) { return client, nil }})
	_, err := dialer.Dial(context.Background(), TCPTLS, admission(1, "jti_timeout", time.Now()))
	if !errors.Is(err, ErrFRPReady) {
		t.Fatalf("err=%v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !client.exited.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !client.exited.Load() || client.closed.Load() {
		t.Fatalf("exited=%v close-called=%v", client.exited.Load(), client.closed.Load())
	}
}

func TestNativeFRPClientBuildsFromAuthenticatedHandoff(t *testing.T) {
	client, err := newFRPClient(admission(1, "jti_native", time.Now()), QUIC)
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("nil native client")
	}
}

func TestConnectorPortMatchesTransportAndSupportsLegacyAdmission(t *testing.T) {
	endpoint := EdgeEndpoint{Port: 7000, TCPPort: 7001, QUICPort: 7002}
	if got := connectorPort(endpoint, QUIC); got != 7002 {
		t.Fatalf("QUIC port = %d", got)
	}
	if got := connectorPort(endpoint, TCPTLS); got != 7001 {
		t.Fatalf("TCP port = %d", got)
	}
	if got := connectorPort(EdgeEndpoint{Port: 7000}, QUIC); got != 7000 {
		t.Fatalf("legacy port = %d", got)
	}
}

func TestAdmissionMetadataCarriesExactHandoffOnce(t *testing.T) {
	admission := admission(1, "jti_metadata", time.Now())
	metadata, err := admissionMetadata(admission)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		OperationID   string         `json:"operation_id"`
		Credential    string         `json:"credential"`
		EnvironmentID string         `json:"environment_id"`
		HelperID      string         `json:"helper_id"`
		Generation    uint64         `json:"connector_generation"`
		EdgeNodeID    string         `json:"edge_node_id"`
		Routes        []RouteHandoff `json:"routes"`
	}
	if err := json.Unmarshal([]byte(metadata), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.OperationID != admission.OperationID || decoded.Credential != admission.Credential || decoded.EnvironmentID != admission.EnvironmentID || decoded.HelperID != admission.HelperID || decoded.Generation != admission.Generation || decoded.EdgeNodeID != admission.EdgeNodeID || len(decoded.Routes) != len(admission.Routes) {
		t.Fatalf("metadata = %+v", decoded)
	}
	if len(metadata) > 64<<10 {
		t.Fatal("metadata is unbounded")
	}
}

func TestFRPDrainReservesDeadlineForTeardown(t *testing.T) {
	client := &drainingFRPClient{done: make(chan struct{})}
	dialer, err := NewFRPDialer(FRPDialerConfig{Factory: func(Admission, Transport) (FRPClient, error) { return client, nil }})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := dialer.Dial(context.Background(), TCPTLS, admission(1, "jti_drain", time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := connection.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	duration := time.Duration(client.duration.Load())
	if duration < 75*time.Millisecond || duration > 125*time.Millisecond || elapsed >= 175*time.Millisecond {
		t.Fatalf("grace=%s elapsed=%s", duration, elapsed)
	}
}
