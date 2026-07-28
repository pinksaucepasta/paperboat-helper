package preview

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type agentControl struct {
	records []ControlRecord
	target  Target
	ack     bool
}

func (c *agentControl) List(context.Context) ([]ControlRecord, error) {
	return append([]ControlRecord(nil), c.records...), nil
}
func (c *agentControl) Register(_ context.Context, logical string, target Target, ack bool, _ time.Duration, _ bool) (ControlRecord, error) {
	c.target, c.ack = target, ack
	record := ControlRecord{ID: "prv_1", EnvironmentID: "env_1", LogicalName: logical, PreviewKey: "p-abcdefghijklmnopqrstuvwxyz", URL: "https://p-abcdefghijklmnopqrstuvwxyz.preview.test", TargetPort: int32(target.Port), State: "registering"}
	c.records = []ControlRecord{record}
	return record, nil
}
func (c *agentControl) Remove(_ context.Context, logical string) (ControlRecord, error) {
	record := c.records[0]
	record.LogicalName = logical
	record.State = "removed"
	c.records = nil
	return record, nil
}

type agentHealthyProber struct{}

func (agentHealthyProber) Probe(context.Context, Target) error { return nil }

func TestAgentHandlerRequiresTokenAndForcesEnvironmentLocalTarget(t *testing.T) {
	registry, err := New(Config{Prober: agentHealthyProber{}})
	if err != nil {
		t.Fatal(err)
	}
	control := &agentControl{}
	routeChanges := 0
	handler, err := NewAgentHandler(AgentHandlerConfig{Token: "01234567890123456789012345678901", EnvironmentID: "env_1", Registry: registry, Control: control, RoutesChanged: func() { routeChanges++ }})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/preview-registrations", bytes.NewBufferString(`{"action":"create","logical_name":"web","target_port":3000,"public_acknowledgement":true}`))
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, request)
	if unauthorized.Code != http.StatusNotFound || len(control.records) != 0 {
		t.Fatalf("unauthorized status=%d records=%v", unauthorized.Code, control.records)
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/preview-registrations", bytes.NewBufferString(`{"action":"create","logical_name":"web","target_port":3000,"public_acknowledgement":true}`))
	request.Header.Set("Authorization", "Bearer 01234567890123456789012345678901")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || control.target != (Target{Host: "127.0.0.1", Port: 3000}) || !control.ack {
		t.Fatalf("status=%d target=%+v ack=%v body=%s", response.Code, control.target, control.ack, response.Body.String())
	}
	if records := registry.ListEnvironment("env_1"); len(records) != 1 || records[0].PublicURL == "" {
		t.Fatalf("local records=%+v", records)
	}
	if routeChanges != 1 {
		t.Fatalf("route changes=%d", routeChanges)
	}
}

func TestAgentHandlerRejectsMissingPublicAcknowledgementAndCrossEnvironmentList(t *testing.T) {
	registry, _ := New(Config{Prober: agentHealthyProber{}})
	control := &agentControl{records: []ControlRecord{{ID: "prv_other", EnvironmentID: "env_other", LogicalName: "secret", PreviewKey: "p-zyxwvutsrqponmlkjihgfedcba", URL: "https://p-zyxwvutsrqponmlkjihgfedcba.preview.test", TargetPort: 4000, State: "ready"}}}
	handler, _ := NewAgentHandler(AgentHandlerConfig{Token: "01234567890123456789012345678901", EnvironmentID: "env_1", Registry: registry, Control: control})
	for _, body := range []string{
		`{"action":"create","logical_name":"web","target_port":3000}`,
		`{"action":"list"}`,
	} {
		request := httptest.NewRequest(http.MethodPost, "/v1/preview-registrations", bytes.NewBufferString(body))
		request.Header.Set("Authorization", "Bearer 01234567890123456789012345678901")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code < 400 {
			var decoded any
			_ = json.Unmarshal(response.Body.Bytes(), &decoded)
			t.Fatalf("body=%s status=%d response=%v", body, response.Code, decoded)
		}
	}
}

func TestAgentHandlerReconcilesServerEvictionBeforeRegisteringReplacement(t *testing.T) {
	registry, err := New(Config{Prober: agentHealthyProber{}, MaxTargets: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.RegisterCanonical("p-zyxwvutsrqponmlkjihgfedcba", "https://p-zyxwvutsrqponmlkjihgfedcba.preview.test", "env_1", "old", Target{Host: "127.0.0.1", Port: 2999}); err != nil {
		t.Fatal(err)
	}
	control := &agentControl{}
	handler, err := NewAgentHandler(AgentHandlerConfig{Token: "01234567890123456789012345678901", EnvironmentID: "env_1", Registry: registry, Control: control})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/preview-registrations", bytes.NewBufferString(`{"action":"create","logical_name":"web","target_port":3000,"public_acknowledgement":true}`))
	request.Header.Set("Authorization", "Bearer 01234567890123456789012345678901")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	records := registry.ListEnvironment("env_1")
	if len(records) != 1 || records[0].LogicalName != "web" {
		t.Fatalf("reconciled records=%+v", records)
	}
}
