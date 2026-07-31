//go:build darwin || linux

package hostservice

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
)

type fakeApplier struct {
	mu    sync.Mutex
	modes []string
	err   error
}

type fakeUpdateActivator struct {
	worker bootstrap.ArtifactManifest
	host   bootstrap.ArtifactManifest
	err    error
}

func (a *fakeUpdateActivator) Activate(_ context.Context, worker, host bootstrap.ArtifactManifest) (string, error) {
	a.worker, a.host = worker, host
	return worker.Version, a.err
}

func (a *fakeApplier) Apply(_ context.Context, mode string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.modes = append(a.modes, mode)
	return a.err
}
func (a *fakeApplier) Close(context.Context) error { return nil }

func TestNewAllowsRootPeerIdentity(t *testing.T) {
	root := t.TempDir()
	server, err := New(Config{
		SocketPath: filepath.Join(root, "host.sock"),
		StatePath:  filepath.Join(root, "policy.json"),
		UID:        0,
		GID:        0,
		Applier:    &fakeApplier{},
		Version:    "test",
	})
	if err != nil || server.config.UID != 0 || server.config.GID != 0 {
		t.Fatalf("server=%v error=%v", server, err)
	}
}

func TestProtocolAppliesMonotonicPolicyAndIsIdempotent(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("peer test requires a non-root enrolled user")
	}
	applier := &fakeApplier{}
	server := testServer(t, os.Getuid(), applier)
	first := requestServer(t, server, Request{Schema: ProtocolV1, Operation: "apply_availability", Mode: KeepAwake, Version: 1})
	if first.Status != "applied" || first.ObservedMode != KeepAwake || first.ObservedVersion != 1 {
		t.Fatalf("first response=%+v", first)
	}
	second := requestServer(t, server, Request{Schema: ProtocolV1, Operation: "apply_availability", Mode: KeepAwake, Version: 1})
	if second.Status != "applied" || len(applier.modes) != 1 {
		t.Fatalf("idempotent response=%+v modes=%v", second, applier.modes)
	}
	stale := requestServer(t, server, Request{Schema: ProtocolV1, Operation: "apply_availability", Mode: AllowSleep, Version: 1})
	if stale.ErrorCode != "stale_policy" || len(applier.modes) != 1 {
		t.Fatalf("stale response=%+v modes=%v", stale, applier.modes)
	}
}

func TestProtocolRejectsUnknownFieldsAndWrongPeer(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("peer test requires a non-root enrolled user")
	}
	server := testServer(t, os.Getuid(), &fakeApplier{})
	response := rawRequestServer(t, server, []byte(`{"schema":"paperboat.host-service/v1","operation":"apply_availability","mode":"keep_awake","version":1,"command":"sh"}`))
	if response.ErrorCode != "invalid_request" {
		t.Fatalf("response=%+v", response)
	}
	denied := testServer(t, os.Getuid()+1, &fakeApplier{})
	serverSide, clientSide := unixPair(t)
	done := make(chan error, 1)
	go func() { done <- denied.serve(serverSide); serverSide.Close() }()
	_ = json.NewEncoder(clientSide).Encode(Request{Schema: ProtocolV1, Operation: "apply_availability", Mode: KeepAwake, Version: 1})
	clientSide.Close()
	if err := <-done; !errors.Is(err, ErrPeerDenied) {
		t.Fatalf("peer error=%v", err)
	}
}

func TestDesiredPolicyPersistsBeforeApplicationFailure(t *testing.T) {
	applier := &fakeApplier{err: errors.New("inhibitor unavailable")}
	server := testServer(t, max(1, os.Getuid()), applier)
	if err := server.apply(context.Background(), KeepAwake, 7); err == nil {
		t.Fatal("application failure was ignored")
	}
	body, err := os.ReadFile(server.config.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var state State
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatal(err)
	}
	if state.DesiredMode != KeepAwake || state.DesiredVersion != 7 || state.Status != "error" || state.ErrorCode != "availability_apply_failed" {
		t.Fatalf("state=%+v", state)
	}
}

func TestProtocolAllowsOnlyPairedSignedUpdateManifests(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("peer test requires a non-root enrolled user")
	}
	activator := &fakeUpdateActivator{}
	server := testServer(t, os.Getuid(), &fakeApplier{})
	server.config.Updates = activator
	worker := &bootstrap.ArtifactManifest{Schema: bootstrap.ArtifactSchemaV1, Kind: bootstrap.ArtifactKindWorker, Version: "2026.07.26"}
	host := &bootstrap.ArtifactManifest{Schema: bootstrap.ArtifactSchemaV1, Kind: bootstrap.ArtifactKindHostService, Version: worker.Version}
	response := requestServer(t, server, Request{Schema: ProtocolV1, Operation: "activate_update", WorkerArtifact: worker, HostServiceArtifact: host})
	if response.ErrorCode != "" || response.UpdateVersion != worker.Version || activator.worker.Kind != bootstrap.ArtifactKindWorker || activator.host.Kind != bootstrap.ArtifactKindHostService {
		t.Fatalf("response=%+v activator=%+v", response, activator)
	}
	for name, request := range map[string]Request{
		"missing host": {Schema: ProtocolV1, Operation: "activate_update", WorkerArtifact: worker},
		"mode smuggle": {Schema: ProtocolV1, Operation: "activate_update", Mode: KeepAwake, WorkerArtifact: worker, HostServiceArtifact: host},
	} {
		t.Run(name, func(t *testing.T) {
			if result := requestServer(t, server, request); result.ErrorCode != "invalid_request" {
				t.Fatalf("response=%+v", result)
			}
		})
	}
	if result := rawRequestServer(t, server, []byte(`{"schema":"paperboat.host-service/v1","operation":"activate_update","worker_artifact":{},"host_service_artifact":{},"path":"/tmp/pbh"}`)); result.ErrorCode != "invalid_request" {
		t.Fatalf("path smuggling response=%+v", result)
	}
}

func testServer(t *testing.T, uid int, applier Applier) *Server {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{SocketPath: filepath.Join(root, "host.sock"), StatePath: filepath.Join(root, "policy.json"), UID: uid, GID: max(1, os.Getgid()), Applier: applier, Version: "test", Now: func() time.Time { return time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func requestServer(t *testing.T, server *Server, request Request) Response {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return rawRequestServer(t, server, body)
}

func rawRequestServer(t *testing.T, server *Server, body []byte) Response {
	t.Helper()
	serverSide, clientSide := unixPair(t)
	done := make(chan error, 1)
	go func() { done <- server.serve(serverSide); serverSide.Close() }()
	if _, err := clientSide.Write(append(body, '\n')); err != nil {
		t.Fatal(err)
	}
	if err := clientSide.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(clientSide).Decode(&response); err != nil {
		t.Fatal(err)
	}
	clientSide.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	return response
}

func unixPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	placeholder, err := os.CreateTemp("/tmp", "pbh-host-socket-")
	if err != nil {
		t.Fatal(err)
	}
	path := placeholder.Name()
	placeholder.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	server, err := listener.AcceptUnix()
	listener.Close()
	if err != nil {
		t.Fatal(err)
	}
	return server, client
}
