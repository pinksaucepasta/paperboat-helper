package testharness

import (
	"context"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/connector"
	"github.com/pinksaucepasta/paperboat-helper/internal/testleak"
)

func admission() connector.Admission {
	return connector.Admission{JTI: "jti_harness_01", Credential: "credential-harness-012345678901234567890123456789", EnvironmentID: "env", HelperID: "helper", Generation: 1, EdgePool: "default", EdgeNodeID: "edge_1", Endpoint: connector.EdgeEndpoint{Host: "edge.test", Port: 7000}, Routes: []connector.RouteHandoff{{RouteID: "route_1", Revision: 1, Kind: "helper_https_wss", PublicHost: "helper.test", ProxyName: "proxy_1", LocalTarget: connector.RouteTarget{Host: "127.0.0.1", Port: 8080}}}, ProtocolVersion: "1.0", ExpiresAt: time.Now().Add(time.Minute)}
}

func TestEdgeAcceptsAndRevokesConnector(t *testing.T) {
	edge := New()
	dialer, err := connector.NewFRPDialer(connector.FRPDialerConfig{Factory: edge.Factory, ReadyTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := dialer.Dial(context.Background(), connector.QUIC, admission())
	if err != nil {
		t.Fatal(err)
	}
	edge.Revoke()
	select {
	case <-connection.(connector.LifecycleConnection).Done():
	case <-time.After(time.Second):
		t.Fatal("revocation did not close connector")
	}
	_ = connection.Close()
	if len(edge.Accepted()) != 1 {
		t.Fatalf("accepted=%d", len(edge.Accepted()))
	}
	if edge.Active() != 0 {
		t.Fatalf("active=%d", edge.Active())
	}
}

func TestRepeatedConnectorLifecycleRetainsNoClients(t *testing.T) {
	baseline, err := testleak.Take()
	if err != nil {
		t.Skipf("leak accounting unavailable: %v", err)
	}
	for cycle := 0; cycle < 100; cycle++ {
		edge := New()
		dialer, _ := connector.NewFRPDialer(connector.FRPDialerConfig{Factory: edge.Factory, ReadyTimeout: time.Second})
		request := admission()
		request.JTI = "jti_soak_" + time.Now().Add(time.Duration(cycle)).Format("150405.000000000")
		connection, err := dialer.Dial(context.Background(), connector.TCPTLS, request)
		if err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
		edge.Revoke()
		select {
		case <-connection.(connector.LifecycleConnection).Done():
		case <-time.After(time.Second):
			t.Fatalf("cycle %d did not stop", cycle)
		}
		if edge.Active() != 0 {
			t.Fatalf("cycle %d active=%d", cycle, edge.Active())
		}
	}
	if err := testleak.WaitForBaseline(baseline, 2*time.Second); err != nil {
		t.Fatal(err)
	}
}
