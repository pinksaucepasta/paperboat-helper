package configsync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type tokenSourceFunc func(context.Context) (string, error)

func (f tokenSourceFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

type proofCall struct {
	operationID string
	method      string
	path        string
	body        string
}

type recordingProofSource struct {
	mu    sync.Mutex
	calls []proofCall
}

func (s *recordingProofSource) Proof(_ context.Context, operationID, method, path string, body []byte) ([]byte, error) {
	s.mu.Lock()
	s.calls = append(s.calls, proofCall{operationID, method, path, string(body)})
	s.mu.Unlock()
	return []byte("proof:" + operationID), nil
}

func TestControlClientCredentialAndLeaseLifecycle(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	var credentialRequests int
	var leaseID = "lease-1"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Paperboat-Helper-Proof") == "" {
			t.Fatalf("missing bounded helper headers: %#v", r.Header)
		}
		switch r.URL.Path {
		case "/v1/config/credentials":
			credentialRequests++
			if r.Header.Get("Authorization") != "Bearer helper-identity" || r.Header.Get("X-Paperboat-Helper-Identity") != "" {
				t.Fatalf("credential authorization = %#v", r.Header)
			}
			writeTestJSON(t, w, map[string]any{"data": map[string]any{
				"credential": "config-credential", "environment_id": "env-1", "helper_id": "helper-1",
				"assignment_id": "assignment-1", "warning_revision": "warning-1", "expires_at": now.Add(5 * time.Minute),
			}})
		case "/v1/config/leases/acquire":
			assertLeaseAuthorization(t, r)
			var body struct {
				OperationID        string `json:"operation_id"`
				BaseRemoteRevision string `json:"base_remote_revision"`
				TTLSeconds         int64  `json:"ttl_seconds"`
			}
			decodeTestJSON(t, r, &body)
			if body.OperationID != "operation-1" || body.BaseRemoteRevision != "head-1" || body.TTLSeconds != 30 {
				t.Fatalf("acquire body = %#v", body)
			}
			writeTestJSON(t, w, map[string]any{"data": Lease{LeaseID: leaseID, RepositoryID: "repo-1", AssignmentID: "assignment-1", EnvironmentID: "env-1", HelperID: "helper-1", FencingToken: 7, BaseRevision: "head-1", ExpiresAt: now.Add(30 * time.Second)}})
		case "/v1/config/leases/renew":
			assertLeaseAuthorization(t, r)
			writeTestJSON(t, w, map[string]any{"data": Lease{LeaseID: leaseID, RepositoryID: "repo-1", AssignmentID: "assignment-1", EnvironmentID: "env-1", HelperID: "helper-1", FencingToken: 7, BaseRevision: "head-1", ExpiresAt: now.Add(time.Minute)}})
		case "/v1/config/leases/release":
			assertLeaseAuthorization(t, r)
			writeTestJSON(t, w, map[string]any{"data": map[string]bool{"released": true}})
		case "/v1/config/status":
			if r.Header.Get("Authorization") != "Bearer helper-identity" || r.Header.Get("X-Paperboat-Helper-Identity") != "" {
				t.Fatalf("status authorization = %#v", r.Header)
			}
			w.WriteHeader(http.StatusAccepted)
			writeTestJSON(t, w, map[string]any{"data": map[string]any{"sync_revision": 1}})
		case "/v1/config/repository-access":
			assertLeaseAuthorization(t, r)
			writeTestJSON(t, w, map[string]any{"data": RepositoryAccess{
				RepositoryID: "repo-1", AssignmentID: "assignment-1", EnvironmentID: "env-1", HelperID: "helper-1",
				CloneURL: "https://github.example.test/owner/config.git", PublishURL: "https://github.example.test/owner/config.git",
				Branch: "main", Username: "x-access-token", Password: "installation-token", ExpiresAt: now.Add(time.Hour),
				Capability: "repository_contents_write",
			}})
		case "/v1/config/runtime":
			assertLeaseAuthorization(t, r)
			writeTestJSON(t, w, map[string]any{"data": RuntimeDescriptor{
				WriteMode: "leased_writes", Mode: ModeBidirectional,
				RepositoryID: "repo-1", AssignmentID: "assignment-1", EnvironmentID: "env-1", HelperID: "helper-1",
				HelperGeneration: 1,
				WarningRevision:  "warning-1",
				Policy: RuntimePolicy{
					Format: "paperboat-config-plaintext-v1", Revision: "policy-1",
					ManifestContract: ManifestContractVersion, ManifestMaxBytes: DefaultManifestMaxBytes,
					ManifestMaxLines: DefaultManifestMaxLines, ManifestMaxPatternBytes: DefaultManifestMaxPatternBytes,
					MaxFileBytes: 5 << 20, MaxBatchBytes: 25 << 20, Debounce: 10 * time.Second,
					MinimumPushInterval: 5 * time.Minute, MaximumDirtyDelay: 5 * time.Minute,
					RemotePollInterval: time.Minute, RetryLimit: 5, ShutdownFlushTimeout: 30 * time.Second, SummaryLimit: 50,
				},
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var operation int
	proofs := &recordingProofSource{}
	client, err := NewControlClient(ControlClientConfig{
		BaseURL: server.URL, AllowedHosts: []string{"127.0.0.1"}, RepositoryHosts: []string{"github.example.test"},
		Identities: tokenSourceFunc(func(context.Context) (string, error) { return "helper-identity", nil }),
		Proofs:     proofs, OperationID: func() (string, error) {
			operation++
			return "operation-" + string(rune('0'+operation)), nil
		},
		Transport: server.Client().Transport, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := client.AcquireLease(context.Background(), "head-1", 30*time.Second)
	if err != nil || lease.FencingToken != 7 {
		t.Fatalf("acquire = %#v, %v", lease, err)
	}
	renewed, err := client.RenewLease(context.Background(), lease, time.Minute)
	if err != nil || !renewed.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("renew = %#v, %v", renewed, err)
	}
	if err := client.ReleaseLease(context.Background(), renewed); err != nil {
		t.Fatal(err)
	}
	if err := client.ReportStatus(context.Background(), Status{
		State: "healthy", Mode: ModeBidirectional, RepositoryID: "repo-1", AssignmentID: "assignment-1", EnvironmentID: "env-1",
		HelperID: "helper-1", HelperGeneration: 1, WarningRevision: "warning-1", PolicyRevision: "policy-1",
		SyncRevision: 1, RemoteRevision: "head-1", UpdatedAt: now,
	}, 10); err != nil {
		t.Fatal(err)
	}
	access, err := client.RepositoryAccess(context.Background())
	if err != nil || access.Password != "installation-token" || access.RepositoryID != "repo-1" {
		t.Fatalf("repository access = %#v, %v", access, err)
	}
	descriptor, err := client.RuntimeDescriptor(context.Background())
	if err != nil || descriptor.Policy.Format != "paperboat-config-plaintext-v1" {
		t.Fatalf("runtime descriptor = %#v, %v", descriptor, err)
	}
	if credentialRequests != 1 {
		t.Fatalf("credential requests = %d", credentialRequests)
	}
	proofs.mu.Lock()
	defer proofs.mu.Unlock()
	if len(proofs.calls) != 7 || proofs.calls[0].operationID != "operation-2" ||
		proofs.calls[1].operationID != "operation-1" || proofs.calls[1].path != "/v1/config/leases/acquire" {
		t.Fatalf("proof calls = %#v", proofs.calls)
	}
	if got := base64.RawURLEncoding.EncodeToString([]byte("proof:operation-1")); got == "" {
		t.Fatal("proof encoding unexpectedly empty")
	}
}

func TestControlClientRevalidationRetainsUnexpiredRepositoryAccess(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	var credentialRequests, accessRequests int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/config/credentials":
			credentialRequests++
			writeTestJSON(t, w, map[string]any{"data": map[string]any{
				"credential": "credential", "environment_id": "env", "helper_id": "helper",
				"assignment_id": "assignment", "warning_revision": "warning",
				"expires_at": now.Add(5 * time.Minute),
			}})
		case "/v1/config/repository-access":
			accessRequests++
			writeTestJSON(t, w, map[string]any{"data": RepositoryAccess{
				RepositoryID: "repository", AssignmentID: "assignment", EnvironmentID: "env",
				HelperID: "helper", CloneURL: "https://github.example.test/example/config.git",
				PublishURL: "https://github.example.test/example/config.git", Branch: "main",
				Username: "x-access-token", Password: "token", Capability: "repository_contents_write",
				ExpiresAt: now.Add(time.Hour),
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewControlClient(ControlClientConfig{
		BaseURL: server.URL, AllowedHosts: []string{"127.0.0.1"}, RepositoryHosts: []string{"github.example.test"},
		Identities: tokenSourceFunc(func(context.Context) (string, error) { return "identity", nil }),
		Proofs:     &recordingProofSource{}, OperationID: func() (string, error) { return "operation", nil },
		Transport: server.Client().Transport, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.RepositoryAccess(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.RevalidateCredential()
	if _, err := client.Credential(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RepositoryAccess(context.Background()); err != nil {
		t.Fatal(err)
	}
	if credentialRequests != 2 || accessRequests != 1 {
		t.Fatalf("credential requests=%d access requests=%d", credentialRequests, accessRequests)
	}
}

func TestControlClientMapsLeaseFailuresAndInvalidatesAuthorization(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	status, code := http.StatusConflict, "lease_busy"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/config/credentials" {
			writeTestJSON(t, w, map[string]any{"data": map[string]any{
				"credential": "config-credential", "environment_id": "env", "helper_id": "helper",
				"assignment_id": "assignment", "warning_revision": "warning", "expires_at": now.Add(5 * time.Minute),
			}})
			return
		}
		w.WriteHeader(status)
		writeTestJSON(t, w, map[string]any{"error": map[string]any{"code": code}})
	}))
	defer server.Close()
	client, err := NewControlClient(ControlClientConfig{
		BaseURL: server.URL, AllowedHosts: []string{"127.0.0.1"},
		Identities: tokenSourceFunc(func(context.Context) (string, error) { return "identity", nil }),
		Proofs:     &recordingProofSource{}, OperationID: func() (string, error) { return "operation", nil },
		Transport: server.Client().Transport, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AcquireLease(context.Background(), "head", 30*time.Second); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("busy error = %v", err)
	}
	status, code = http.StatusUnauthorized, "lease_invalid"
	if _, err := client.AcquireLease(context.Background(), "head", 30*time.Second); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("authorization error = %v", err)
	}
}

func TestControlClientRejectsMismatchedLeaseBindings(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	for name, mutate := range map[string]func(*Lease){
		"assignment":  func(lease *Lease) { lease.AssignmentID = "other" },
		"environment": func(lease *Lease) { lease.EnvironmentID = "other" },
		"helper":      func(lease *Lease) { lease.HelperID = "other" },
		"base":        func(lease *Lease) { lease.BaseRevision = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/config/credentials" {
					writeTestJSON(t, w, map[string]any{"data": map[string]any{
						"credential": "credential", "environment_id": "environment", "helper_id": "helper",
						"assignment_id": "assignment", "warning_revision": "warning", "expires_at": now.Add(5 * time.Minute),
					}})
					return
				}
				lease := Lease{
					LeaseID: "lease", RepositoryID: "repository", AssignmentID: "assignment",
					EnvironmentID: "environment", HelperID: "helper", FencingToken: 1,
					BaseRevision: "head", ExpiresAt: now.Add(time.Minute),
				}
				mutate(&lease)
				writeTestJSON(t, w, map[string]any{"data": lease})
			}))
			defer server.Close()
			client, err := NewControlClient(ControlClientConfig{
				BaseURL: server.URL, AllowedHosts: []string{"127.0.0.1"},
				Identities: tokenSourceFunc(func(context.Context) (string, error) { return "identity", nil }),
				Proofs:     &recordingProofSource{}, OperationID: func() (string, error) { return "operation", nil },
				Transport: server.Client().Transport, Clock: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.AcquireLease(context.Background(), "head", 30*time.Second); !errors.Is(err, ErrControlClientInvalid) {
				t.Fatalf("mismatched lease error = %v", err)
			}
		})
	}
}

func TestControlClientRejectsChangedLeaseIdentityOnRenewal(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	lease := Lease{
		LeaseID: "lease", RepositoryID: "repository", AssignmentID: "assignment",
		EnvironmentID: "environment", HelperID: "helper", FencingToken: 1,
		BaseRevision: "head", ExpiresAt: now.Add(time.Minute),
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/config/credentials" {
			writeTestJSON(t, w, map[string]any{"data": map[string]any{
				"credential": "credential", "environment_id": "environment", "helper_id": "helper",
				"assignment_id": "assignment", "warning_revision": "warning", "expires_at": now.Add(5 * time.Minute),
			}})
			return
		}
		changed := lease
		changed.LeaseID = "other"
		changed.ExpiresAt = now.Add(2 * time.Minute)
		writeTestJSON(t, w, map[string]any{"data": changed})
	}))
	defer server.Close()
	client, err := NewControlClient(ControlClientConfig{
		BaseURL: server.URL, AllowedHosts: []string{"127.0.0.1"},
		Identities: tokenSourceFunc(func(context.Context) (string, error) { return "identity", nil }),
		Proofs:     &recordingProofSource{}, OperationID: func() (string, error) { return "operation", nil },
		Transport: server.Client().Transport, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.RenewLease(context.Background(), lease, 30*time.Second); !errors.Is(err, ErrControlClientInvalid) {
		t.Fatalf("changed renewal error = %v", err)
	}
}

func TestControlClientRejectsUntrustedEndpoints(t *testing.T) {
	source := tokenSourceFunc(func(context.Context) (string, error) { return "identity", nil })
	for _, base := range []string{"http://api.example.test", "https://user@api.example.test", "https://api.example.test?secret=x"} {
		if _, err := NewControlClient(ControlClientConfig{BaseURL: base, AllowedHosts: []string{"api.example.test"}, Identities: source, Proofs: &recordingProofSource{}, OperationID: func() (string, error) { return "operation", nil }}); !errors.Is(err, ErrControlClientInvalid) {
			t.Fatalf("base %q error = %v", base, err)
		}
	}
}

func assertLeaseAuthorization(t *testing.T, r *http.Request) {
	t.Helper()
	if r.Header.Get("Authorization") != "Bearer config-credential" || r.Header.Get("X-Paperboat-Helper-Identity") != "helper-identity" {
		t.Fatalf("lease authorization = %#v", r.Header)
	}
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}

func decodeTestJSON(t *testing.T, r *http.Request, target any) {
	t.Helper()
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatal(err)
	}
}
