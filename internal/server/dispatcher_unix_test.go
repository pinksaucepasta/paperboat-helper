//go:build darwin || linux

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/activity"
	"github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/health"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/preview"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

type healthyProber struct{}

func (healthyProber) Probe(context.Context, preview.Target) error { return nil }

func verticalServer(t *testing.T) (*Server, *activity.Collector) {
	t.Helper()
	root := t.TempDir()
	adapter, err := pty.NewAdapter(root)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.NewManager(session.ManagerConfig{Launch: func(command pty.Command) (session.PTYProcess, error) { return adapter.Start(command) }, MaxSessions: 4})
	if err != nil {
		t.Fatal(err)
	}
	collector, err := activity.New(activity.Config{})
	if err != nil {
		t.Fatal(err)
	}
	previews, err := preview.New(preview.Config{Prober: healthyProber{}})
	if err != nil {
		t.Fatal(err)
	}
	readiness := health.New("test", []string{"terminal.v1", "health.v1", "preview.public.v1", "activity.v1"}, nil)
	readiness.Set("terminal.v1", health.Ready, "", 0)
	readiness.Set("health.v1", health.Ready, "", 0)
	dispatcher, err := NewDispatcher(DispatcherConfig{
		Sessions: sessions, Previews: previews, Activity: collector, Health: readiness,
		ShellPath: "/bin/sh", ShellArgs: []string{"-c", "printf stream-data; read line"}, ShellEnv: []string{"PATH=/usr/bin:/bin", "TERM=xterm"}, WorkspaceRoot: root,
		Random: bytes.NewReader(bytes.Repeat([]byte{1}, 256)),
	})
	if err != nil {
		t.Fatal(err)
	}
	journal, _ := operation.NewJournal(32)
	server, err := New(Config{
		Negotiator: protocol.Negotiator{Profile: config.BYOD, Available: map[string]bool{"terminal.v1": true, "health.v1": true, "preview.public.v1": true, "activity.v1": true}},
		Journal:    journal,
		Authorizer: authorizerFunc(func(context.Context, protocol.Frame) (Authorization, error) {
			return Authorization{JournalBinding: "env:env_test_01:user:usr_1", EnvironmentID: "env_test_01", UserID: "usr_1", ClientID: "cli_1", ResourceID: "p-abcdefghijklmnopqrstuvwxyz"}, nil
		}),
		Handler: dispatcher, MaxConcurrent: 4, HeartbeatInterval: time.Hour, PeerTimeout: 2 * time.Hour, MutationDeadline: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = sessions.Shutdown(ctx)
	})
	return server, collector
}

func TestAttachStreamsReplayAndLiveOutputAsBinaryFrames(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("requires /bin/sh")
	}
	server, _ := verticalServer(t)
	client, peer := net.Pipe()
	go server.Serve(peer)
	payload := json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1"]}`)
	_ = sendRequest(t, client, protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: payload})
	response := sendRequest(t, client, request("req_create", "op_create_0001", json.RawMessage(`{"action":"create","name":"stream","columns":80,"rows":24}`)))
	var created struct {
		Result struct {
			ID string `json:"id"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Payload, &created); err != nil || created.Result.ID == "" {
		t.Fatalf("create=%s err=%v", response.Payload, err)
	}
	attachPayload, _ := json.Marshal(map[string]any{"action": "attach", "session_id": created.Result.ID, "from_sequence": 0})
	response = sendRequest(t, client, request("req_attach", "op_attach_0001", attachPayload))
	if response.Type != "response" {
		t.Fatalf("attach=%s", response.Payload)
	}
	var attached struct {
		Result struct {
			AttachmentID string `json:"attachment_id"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Payload, &attached); err != nil || attached.Result.AttachmentID == "" {
		t.Fatalf("attach payload=%s err=%v", response.Payload, err)
	}
	binary, err := protocol.ReadBinaryFrame(client)
	if err != nil {
		t.Fatal(err)
	}
	if binary.Channel != protocol.Stdout || binary.StartSequence != 0 || string(binary.Data) != "stream-data" {
		t.Fatalf("binary=%#v", binary)
	}
	control, _ := json.Marshal(map[string]any{"session_id": created.Result.ID, "attachment_id": attached.Result.AttachmentID, "next_sequence": len(binary.Data)})
	if response = sendRequest(t, client, protocol.Frame{Type: "ack", RequestID: "req_ack", Version: "1.0", Payload: control}); response.Type != "response" {
		t.Fatalf("ack=%s", response.Payload)
	}
	control, _ = json.Marshal(map[string]any{"session_id": created.Result.ID, "attachment_id": attached.Result.AttachmentID})
	if response = sendRequest(t, client, protocol.Frame{Type: "detach", RequestID: "req_detach", Version: "1.0", Payload: control}); response.Type != "response" {
		t.Fatalf("detach=%s", response.Payload)
	}
	// A health request proves explicit detach left the connection usable.
	healthFrame := request("req_health_after_detach", "op_health_after_detach", json.RawMessage(`{}`))
	healthFrame.Capability = "health.v1"
	if response = sendRequest(t, client, healthFrame); response.Type != "response" {
		t.Fatalf("post-detach health=%s", response.Payload)
	}
	_ = client.Close()
}

func sendRequest(t *testing.T, conn net.Conn, frame protocol.Frame) protocol.Frame {
	t.Helper()
	if err := protocol.WriteFrame(conn, frame); err != nil {
		t.Fatal(err)
	}
	response, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestVerticalFramedTerminalPreviewActivityAndReadiness(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("requires /bin/sh")
	}
	server, collector := verticalServer(t)
	client, peer := net.Pipe()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(peer) }()
	payload := json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1","preview.public.v1","activity.v1"]}`)
	response := sendRequest(t, client, protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: payload})
	if response.Type != "welcome" {
		t.Fatalf("welcome=%#v", response)
	}

	create := request("req_create", "op_create_0001", json.RawMessage(`{"action":"create","name":"default","cwd":".","columns":80,"rows":24}`))
	response = sendRequest(t, client, create)
	if response.Type != "response" {
		t.Fatalf("create=%s", response.Payload)
	}
	var envelope struct {
		Result struct {
			ID string `json:"id"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Payload, &envelope); err != nil || envelope.Result.ID == "" {
		t.Fatalf("create payload=%s err=%v", response.Payload, err)
	}

	previewFrame := request("req_preview", "op_preview_0001", json.RawMessage(`{"action":"register","logical_name":"web","target_host":"127.0.0.1","target_port":3000,"public_acknowledgement":true}`))
	previewFrame.Capability = "preview.public.v1"
	if response = sendRequest(t, client, previewFrame); response.Type != "response" {
		t.Fatalf("preview=%s", response.Payload)
	}

	activityFrame := request("req_activity", "op_activity_0001", json.RawMessage(`{"environment_id":"env_test_01","source_id":"cli_1","source":"cli_activity","sequence":1,"occurred_at":"2026-01-01T00:00:00Z","observed_at":"2026-01-01T00:00:01Z"}`))
	activityFrame.Capability = "activity.v1"
	if response = sendRequest(t, client, activityFrame); response.Type != "response" {
		t.Fatalf("activity=%s", response.Payload)
	}
	if collector.LastActivity().IsZero() == false {
		// The fixture timestamp is intentionally stale and must not extend idle state.
		t.Fatalf("stale activity extended idle: %s", collector.LastActivity())
	}

	healthFrame := request("req_health", "op_health_0001", json.RawMessage(`{}`))
	healthFrame.Capability = "health.v1"
	if response = sendRequest(t, client, healthFrame); response.Type != "response" {
		t.Fatalf("health=%s", response.Payload)
	}

	closeFrame := request("req_close", "op_close_0001", json.RawMessage(`{"action":"close","session_id":"`+envelope.Result.ID+`"}`))
	if response = sendRequest(t, client, closeFrame); response.Type != "response" {
		t.Fatalf("close=%s", response.Payload)
	}
	_ = client.Close()
	<-serveDone
}

func TestDispatcherRejectsCrossEnvironmentActivityAndEscapedCWD(t *testing.T) {
	server, _ := verticalServer(t)
	client, peer := net.Pipe()
	go server.Serve(peer)
	payload := json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1","activity.v1"]}`)
	_ = sendRequest(t, client, protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: payload})

	activityFrame := request("req_activity", "op_activity_0001", json.RawMessage(`{"environment_id":"env_other","source_id":"cli_1","source":"cli_activity","sequence":1,"occurred_at":"2026-01-01T00:00:00Z"}`))
	activityFrame.Capability = "activity.v1"
	if response := sendRequest(t, client, activityFrame); response.Type != "error" {
		t.Fatalf("cross-environment activity=%#v", response)
	}
	previewFrame := request("req_preview", "op_preview_0001", json.RawMessage(`{"action":"register","identity":"p-attacker-controlled-identity","logical_name":"web","target_host":"127.0.0.1","target_port":3000,"public_acknowledgement":true}`))
	previewFrame.Capability = "preview.public.v1"
	if response := sendRequest(t, client, previewFrame); response.Type != "error" {
		t.Fatalf("overridden preview identity=%#v", response)
	}
	escape := filepath.Join("..", "outside")
	createPayload, _ := json.Marshal(map[string]any{"action": "create", "name": "bad", "cwd": escape, "columns": 80, "rows": 24})
	if response := sendRequest(t, client, request("req_create", "op_create_0001", createPayload)); response.Type != "error" {
		t.Fatalf("escaped cwd=%#v", response)
	}
}
