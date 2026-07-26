//go:build darwin || linux

package availability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/hostservice"
)

type tokenStub struct{}

func (tokenStub) Token(context.Context) (string, error) { return "identity", nil }

type proofStub struct{ body []byte }

func (p *proofStub) Proof(_ context.Context, operationID, method, path string, body []byte) ([]byte, error) {
	if operationID != "operation" || method != http.MethodPost || path != "/v1/helper-runtime-policies/resolve" {
		return nil, errors.New("wrong proof binding")
	}
	p.body = append([]byte(nil), body...)
	return []byte("proof"), nil
}

func TestResolverUsesExactProofAndStrictResponse(t *testing.T) {
	proofs := &proofStub{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer identity" || r.Header.Get("X-Paperboat-Helper-Proof") == "" {
			t.Error("missing helper authentication")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": Resolution{Schema: PolicySchemaV1, UserMachineID: "um_1", Mode: "keep_awake", Version: 0}})
	}))
	defer server.Close()
	client := server.Client()
	client.Timeout = time.Second
	resolver, err := NewResolver(server.URL+"/v1/helper-runtime-policies/resolve", tokenStub{}, proofs, func() (string, error) { return "operation", nil }, client)
	if err != nil {
		t.Fatal(err)
	}
	result, err := resolver.Resolve(context.Background())
	if err != nil || result.Version != 0 || result.Mode != "keep_awake" {
		t.Fatalf("resolution=%+v err=%v", result, err)
	}
	if !bytes.Equal(proofs.body, []byte("{}")) {
		t.Fatalf("proof body=%q", proofs.body)
	}
}

func TestHostClientAppliesVersionZeroAndRejectsMismatchedResponse(t *testing.T) {
	for name, response := range map[string]hostservice.Response{
		"valid":    {Schema: hostservice.ProtocolV1, Status: "applied", DesiredMode: "allow_sleep", DesiredVersion: 0, ObservedMode: "allow_sleep", ObservedVersion: 0, ObservedAt: time.Now().UTC(), HostServiceVersion: "test", Scope: "system"},
		"mismatch": {Schema: hostservice.ProtocolV1, Status: "applied", DesiredMode: "keep_awake", DesiredVersion: 1, ObservedMode: "keep_awake", ObservedVersion: 1, ObservedAt: time.Now().UTC(), HostServiceVersion: "test", Scope: "system"},
	} {
		t.Run(name, func(t *testing.T) {
			root, err := os.MkdirTemp("/tmp", "pba-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(root)
			socket := filepath.Join(root, "host.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			go func() {
				connection, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				defer connection.Close()
				var request hostservice.Request
				_ = json.NewDecoder(connection).Decode(&request)
				_ = json.NewEncoder(connection).Encode(response)
			}()
			client, err := NewHostClient(socket, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			observation, err := client.Apply(context.Background(), Resolution{Schema: PolicySchemaV1, UserMachineID: "um_1", Mode: "allow_sleep", Version: 0})
			if name == "valid" && (err != nil || observation.Version != 0) {
				t.Fatalf("observation=%+v err=%v", observation, err)
			}
			if name == "mismatch" && !errors.Is(err, ErrInvalid) {
				t.Fatalf("mismatch error=%v", err)
			}
		})
	}
}

type flakyResolver struct {
	mu    sync.Mutex
	calls int
}

func (r *flakyResolver) Resolve(context.Context) (Resolution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.calls == 1 {
		return Resolution{}, errors.New("offline")
	}
	return Resolution{Schema: PolicySchemaV1, UserMachineID: "um_1", Mode: "keep_awake", Version: 2}, nil
}

type hostStub struct{ applied chan Resolution }

func (h hostStub) Apply(_ context.Context, resolution Resolution) (Observation, error) {
	h.applied <- resolution
	return Observation{Schema: PolicySchemaV1, Mode: resolution.Mode, Version: resolution.Version, Status: "applied", ObservedAt: time.Now().UTC(), HostServiceVersion: "test", HostServiceScope: "system"}, nil
}

func TestServiceStartsOfflineAndEventuallyPublishesObservation(t *testing.T) {
	resolver := &flakyResolver{}
	host := hostStub{applied: make(chan Resolution, 1)}
	service, err := NewService(resolver, host, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := service.Start(ctx); err != nil {
		t.Fatalf("offline start: %v", err)
	}
	select {
	case policy := <-host.applied:
		if policy.Version != 2 {
			t.Fatalf("policy=%+v", policy)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("policy was not retried")
	}
	if observation := service.Observation(); observation == nil || observation.Version != 2 {
		t.Fatalf("observation=%+v", observation)
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := service.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
}
