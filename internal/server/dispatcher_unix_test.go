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

	"github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/health"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/preview"
	"github.com/pinksaucepasta/paperboat-helper/internal/process"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

type healthyProber struct{}

func (healthyProber) Probe(context.Context, preview.Target) error { return nil }

type testSessionLauncher struct {
	sessions *session.Manager
	args     []string
}

func (l testSessionLauncher) Launch(ctx context.Context, request process.LaunchRequest) (session.Snapshot, error) {
	return l.sessions.Create(ctx, session.CreateRequest{ID: request.ID, Name: request.Name, Command: pty.Command{Path: "/bin/sh", Args: append([]string(nil), l.args...), Env: []string{"PATH=/usr/bin:/bin", "TERM=xterm"}, CWD: request.CWD, Dimensions: request.Dimensions}})
}

func verticalServer(t *testing.T) *Server {
	return verticalServerCommand(t, []string{"-c", "printf stream-data; read line"})
}

func verticalServerCommand(t *testing.T, shellArgs []string) *Server {
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
	previews, err := preview.New(preview.Config{Prober: healthyProber{}})
	if err != nil {
		t.Fatal(err)
	}
	readiness := health.New("test", []string{"terminal.v1", "health.v1", "preview.public.v1"}, nil)
	readiness.Set("terminal.v1", health.Ready, "", 0)
	readiness.Set("health.v1", health.Ready, "", 0)
	dispatcher, err := NewDispatcher(DispatcherConfig{
		Sessions: sessions, Previews: previews, Health: readiness,
		SessionLauncher: testSessionLauncher{sessions: sessions, args: shellArgs}, WorkspaceRoot: root,
		Random: bytes.NewReader(bytes.Repeat([]byte{1}, 256)),
	})
	if err != nil {
		t.Fatal(err)
	}
	journal, _ := operation.NewJournal(32)
	server, err := New(Config{
		Negotiator: protocol.Negotiator{Profile: config.BYOD, Available: map[string]bool{"terminal.v1": true, "health.v1": true, "preview.public.v1": true}},
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
	return server
}

func TestTerminalStreamEndFollowsOutputWithExactExit(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("requires /bin/sh")
	}
	for _, test := range []struct {
		name    string
		command string
		code    int
		signal  string
	}{
		{name: "zero", command: "printf final-output; exit 0", code: 0},
		{name: "nonzero", command: "printf final-output; exit 7", code: 7},
		{name: "signal", command: "printf final-output; kill -TERM $$", code: 143, signal: "terminated"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := verticalServerCommand(t, []string{"-c", test.command})
			client, peer := net.Pipe()
			go server.Serve(peer)
			hello := json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1"]}`)
			_ = sendRequest(t, client, protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: hello})
			created := sendRequest(t, client, request("req_create", "op_create_exit", json.RawMessage(`{"action":"create","name":"exit-test","columns":80,"rows":24}`)))
			var createResponse struct {
				Result struct {
					ID string `json:"id"`
				} `json:"result"`
			}
			if json.Unmarshal(created.Payload, &createResponse) != nil || createResponse.Result.ID == "" {
				t.Fatalf("create=%s", created.Payload)
			}
			attachPayload, _ := json.Marshal(map[string]any{"action": "attach", "session_id": createResponse.Result.ID, "from_sequence": 0})
			if attached := sendRequest(t, client, request("req_attach", "op_attach_exit", attachPayload)); attached.Type != "response" {
				t.Fatalf("attach=%s", attached.Payload)
			}
			output, err := protocol.ReadBinaryFrame(client)
			if err != nil || string(output.Data) != "final-output" {
				t.Fatalf("output=%q err=%v", output.Data, err)
			}
			end, err := protocol.ReadFrame(client)
			if err != nil || end.Type != "event" || end.Capability != "terminal.v1" {
				t.Fatalf("end=%#v err=%v", end, err)
			}
			var payload struct {
				Event         string `json:"event"`
				SessionID     string `json:"session_id"`
				State         string `json:"state"`
				FinalSequence uint64 `json:"final_sequence"`
				Exit          struct {
					Code   int    `json:"code"`
					Signal string `json:"signal"`
				} `json:"exit"`
			}
			if json.Unmarshal(end.Payload, &payload) != nil || payload.Event != "terminal_stream_end" || payload.SessionID != createResponse.Result.ID || payload.State != "exited" || payload.FinalSequence != uint64(len("final-output")) || payload.Exit.Code != test.code || payload.Exit.Signal != test.signal {
				t.Fatalf("payload=%s", end.Payload)
			}
			_ = client.Close()
		})
	}
}

func TestAttachStreamsReplayAndLiveOutputAsBinaryFrames(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("requires /bin/sh")
	}
	server := verticalServer(t)
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
	control, _ := json.Marshal(map[string]any{"session_id": created.Result.ID, "attachment_id": attached.Result.AttachmentID})
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

func TestAttachLargeReplayUsesBinaryStreamNotStructuredResponse(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("requires /bin/sh")
	}
	server := verticalServerCommand(t, []string{"-c", "dd if=/dev/zero bs=65536 count=1 2>/dev/null; read line"})
	client, peer := net.Pipe()
	go server.Serve(peer)
	hello := json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1"]}`)
	_ = sendRequest(t, client, protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: hello})
	response := sendRequest(t, client, request("req_create", "op_create_large", json.RawMessage(`{"action":"create","name":"large-replay","columns":80,"rows":24}`)))
	var created struct {
		Result struct {
			ID string `json:"id"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Payload, &created); err != nil || created.Result.ID == "" {
		t.Fatalf("create=%s err=%v", response.Payload, err)
	}
	// Let the PTY output enter retained history before attaching.
	time.Sleep(100 * time.Millisecond)
	attachPayload, _ := json.Marshal(map[string]any{"action": "attach", "session_id": created.Result.ID, "from_sequence": 0})
	response = sendRequest(t, client, request("req_attach", "op_attach_large", attachPayload))
	if response.Type != "response" || len(response.Payload) > protocol.MaxStructuredFrame/8 || bytes.Contains(response.Payload, []byte(`"events"`)) {
		t.Fatalf("attach response=%d bytes payload=%s", len(response.Payload), response.Payload)
	}
	frame, err := protocol.ReadBinaryFrame(client)
	if err != nil || frame.Channel != protocol.Stdout || len(frame.Data) == 0 {
		t.Fatalf("binary=%#v err=%v", frame, err)
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

func TestVerticalFramedTerminalPreviewAndReadiness(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("requires /bin/sh")
	}
	server := verticalServer(t)
	client, peer := net.Pipe()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(peer) }()
	payload := json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1","preview.public.v1"]}`)
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

func TestDispatcherRejectsEscapedCWDAndOverriddenPreviewIdentity(t *testing.T) {
	server := verticalServer(t)
	client, peer := net.Pipe()
	go server.Serve(peer)
	payload := json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1"]}`)
	_ = sendRequest(t, client, protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: payload})

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
