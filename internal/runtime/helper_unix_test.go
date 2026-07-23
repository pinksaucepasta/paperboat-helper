//go:build darwin || linux

package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/pinksaucepasta/paperboat-helper/internal/activity"
	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/configapply"
	"github.com/pinksaucepasta/paperboat-helper/internal/preview"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/server"
	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

type helperAuthorizer struct{}

func (helperAuthorizer) Authorize(context.Context, protocol.Frame) (server.Authorization, error) {
	return server.Authorization{JournalBinding: "phase2:user:client", EnvironmentID: "env_test", UserID: "user_test", ClientID: "client_test"}, nil
}

type helperProber struct{}

func (helperProber) Probe(context.Context, preview.Target) error { return nil }

type hostedLifecycleStub struct{}

func (hostedLifecycleStub) Start(context.Context) error    { return nil }
func (hostedLifecycleStub) Shutdown(context.Context) error { return nil }
func (hostedLifecycleStub) Capabilities() []string         { return []string{"hosted.lifecycle.v1"} }

type helperListener struct {
	mu     sync.Mutex
	conn   net.Conn
	closed chan struct{}
}

func (l *helperListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.conn != nil {
		connection := l.conn
		l.conn = nil
		l.mu.Unlock()
		return connection, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, net.ErrClosed
}

func (l *helperListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func (*helperListener) Addr() net.Addr { return helperAddress("helper") }

type helperAddress string

func (a helperAddress) Network() string { return "pipe" }
func (a helperAddress) String() string  { return string(a) }

func TestHelperCompositionNegotiatesAuthenticatedHealthAndClosesDurableState(t *testing.T) {
	root := t.TempDir()
	serverSide, clientSide := net.Pipe()
	listener := &helperListener{conn: serverSide, closed: make(chan struct{})}
	config := helperconfig.Config{Profile: helperconfig.BYOD, StateRoot: root, Version: "test", Limits: helperconfig.DefaultLimits, Resources: helperconfig.DefaultResources}
	previews, err := preview.New(preview.Config{Prober: helperProber{}, MaxTargets: 4, MaxConcurrentProbes: 1})
	if err != nil {
		t.Fatal(err)
	}
	activityCollector, err := activity.New(activity.Config{MaxQueued: 16, MaxDiagnostics: 16})
	if err != nil {
		t.Fatal(err)
	}
	helper, err := NewHelper(context.Background(), HelperConfig{
		Runtime: config, ListenAddress: "127.0.0.1:0", WorkspaceRoot: root,
		EnvironmentID: "env_test",
	}, HelperDependencies{
		Authorizer: func(token string) (server.Authorizer, error) {
			if token != "signed-operation-credential" {
				t.Fatalf("token=%q", token)
			}
			return helperAuthorizer{}, nil
		},
		Listener: func() (net.Listener, error) { return listener, nil },
		Previews: previews, Activity: activityCollector,
		ConfigApply: configapply.ConformanceHandler{}, ConfigApplyProof: true,
		SessionLauncherFactory: commandSessionLauncherFactory("/bin/sh", []string{"-l"}, []string{"PATH=/usr/bin:/bin"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := helper.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { return clientSide, nil }}
	connection, response, err := websocket.Dial(context.Background(), "ws://helper.test/v1/runtime", &websocket.DialOptions{
		HTTPClient:   &http.Client{Transport: transport},
		HTTPHeader:   http.Header{"Authorization": []string{"Bearer signed-operation-credential"}},
		Subprotocols: []string{server.DefaultWebSocketSubprotocol},
	})
	if err != nil {
		if response != nil {
			response.Body.Close()
		}
		t.Fatal(err)
	}
	writeFrame := func(frame protocol.Frame) {
		t.Helper()
		var encoded bytes.Buffer
		if err := protocol.WriteFrame(&encoded, frame); err != nil {
			t.Fatal(err)
		}
		if err := connection.Write(context.Background(), websocket.MessageText, encoded.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	readFrame := func() protocol.Frame {
		t.Helper()
		messageType, encoded, err := connection.Read(context.Background())
		if err != nil || messageType != websocket.MessageText {
			t.Fatalf("read type=%v err=%v", messageType, err)
		}
		frame, err := protocol.ReadFrame(bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	writeFrame(protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1","preview.public.v1","activity.v1","config.apply.v1"]}`)})
	if welcome := readFrame(); welcome.Type != "welcome" || !bytes.Contains(welcome.Payload, []byte(`"preview.public.v1"`)) || !bytes.Contains(welcome.Payload, []byte(`"activity.v1"`)) || !bytes.Contains(welcome.Payload, []byte(`"config.apply.v1"`)) {
		t.Fatalf("welcome=%#v", welcome)
	}
	writeFrame(protocol.Frame{Type: "request", RequestID: "req_health", Version: "1.0", OperationID: "op_health_0001", Capability: "health.v1", DeadlineMS: 5_000, Payload: json.RawMessage(`{}`)})
	responseFrame := readFrame()
	if responseFrame.Type != "response" || !bytes.Contains(responseFrame.Payload, []byte(`"live":true`)) {
		t.Fatalf("response=%#v", responseFrame)
	}
	_ = connection.Close(websocket.StatusNormalClosure, "done")
	transport.CloseIdleConnections()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := helper.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if helper.State() != Stopped {
		t.Fatalf("state=%s", helper.State())
	}
	reopened, err := store.Open(context.Background(), store.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHelperCompositionRejectsMissingTrustBoundaryBeforeStateCreation(t *testing.T) {
	root := t.TempDir()
	config := helperconfig.Config{Profile: helperconfig.BYOD, StateRoot: root, Version: "test", Limits: helperconfig.DefaultLimits, Resources: helperconfig.DefaultResources}
	if _, err := NewHelper(context.Background(), HelperConfig{Runtime: config, ListenAddress: "127.0.0.1:0", WorkspaceRoot: root}, HelperDependencies{}); !errors.Is(err, ErrHelperInvalid) {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "state.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state file err=%v", err)
	}
}

func TestHelperCompositionEnforcesHostedProfileBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile helperconfig.Profile
		hosted  HostedLifecycle
	}{
		{name: "hosted requires lifecycle", profile: helperconfig.Hosted},
		{name: "byod forbids lifecycle", profile: helperconfig.BYOD, hosted: hostedLifecycleStub{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			runtimeConfig := helperconfig.Config{Profile: tc.profile, StateRoot: root, Version: "test", Limits: helperconfig.DefaultLimits, Resources: helperconfig.DefaultResources}
			_, err := NewHelper(context.Background(), HelperConfig{Runtime: runtimeConfig, ListenAddress: "127.0.0.1:0", WorkspaceRoot: root}, HelperDependencies{
				Authorizer: func(string) (server.Authorizer, error) { return helperAuthorizer{}, nil }, HostedLifecycle: tc.hosted,
			})
			if !errors.Is(err, ErrHelperInvalid) {
				t.Fatalf("error=%v", err)
			}
			if _, err := os.Stat(filepath.Join(root, "state.db")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("state file error=%v", err)
			}
		})
	}
}

func TestHelperCompositionRejectsUnsafeBindBeforeStateCreation(t *testing.T) {
	root := t.TempDir()
	config := helperconfig.Config{Profile: helperconfig.BYOD, StateRoot: root, Version: "test", Limits: helperconfig.DefaultLimits, Resources: helperconfig.DefaultResources}
	_, err := NewHelper(context.Background(), HelperConfig{Runtime: config, ListenAddress: "0.0.0.0:8080", WorkspaceRoot: root}, HelperDependencies{
		Authorizer: func(string) (server.Authorizer, error) { return helperAuthorizer{}, nil },
	})
	if !errors.Is(err, ErrHelperInvalid) {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "state.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state file err=%v", err)
	}
}
