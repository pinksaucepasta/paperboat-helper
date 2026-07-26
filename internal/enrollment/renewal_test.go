package enrollment

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type renewalMetric struct {
	mu    sync.Mutex
	count float64
}

func (m *renewalMetric) Record(name string, value float64, labels map[string]string) error {
	if name == "paperboat_helper_renewal_failures_total" && len(labels) == 0 {
		m.mu.Lock()
		m.count += value
		m.mu.Unlock()
	}
	return nil
}

func TestRenewingTokenSourceRenewsAndPersistsIdentity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	renewedCredential := "renewed-helper-credential-012345678901234567890"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/helper-enrollments":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"helper_id": "hlp_1", "environment_id": "env_1", "credential": "initial-helper-credential-012345678901234567890", "expires_at": now.Add(5 * time.Minute)}})
		case "/v1/helper-identity-renewals":
			if r.Header.Get("Authorization") != "" || r.Header.Get("X-Paperboat-Helper-Proof") == "" {
				t.Errorf("renew headers=%v", r.Header)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"helper_id": "hlp_1", "environment_id": "env_1", "credential": renewedCredential, "expires_at": now.Add(time.Hour)}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	root := t.TempDir()
	client, _ := NewClient(server.Client().Transport, time.Second)
	if _, err := client.Enroll(context.Background(), Config{ControlURL: server.URL, StateRoot: root, EnrollmentCredential: "grant-credential-012345678901234567890"}); err != nil {
		t.Fatal(err)
	}
	source, err := NewRenewingTokenSource(RenewingTokenConfig{ControlURL: server.URL, StateRoot: root, Transport: server.Client().Transport, RenewBefore: 10 * time.Minute, Timeout: time.Second, Clock: func() time.Time { return now }, OperationID: func() (string, error) { return "op_renew_0001", nil }})
	if err != nil {
		t.Fatal(err)
	}
	token, err := source.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token != renewedCredential {
		t.Fatalf("token=%q", token)
	}
	stored, err := LoadRuntimeIdentity(root, now)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Credential != renewedCredential || stored.KeyID == "" {
		t.Fatalf("stored=%#v", stored)
	}
}

func TestRenewingTokenSourceRecoversIdentityAfterLongOutage(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/helper-enrollments":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"helper_id": "hlp_1", "environment_id": "env_1", "credential": "initial-helper-credential-012345678901234567890", "expires_at": now.Add(time.Hour)}})
		case "/v1/helper-identity-renewals":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"helper_id": "hlp_1", "environment_id": "env_1", "credential": "renewed-helper-credential-012345678901234567890", "expires_at": now.Add(time.Hour)}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	root := t.TempDir()
	client, _ := NewClient(server.Client().Transport, time.Second)
	if _, err := client.Enroll(context.Background(), Config{ControlURL: server.URL, StateRoot: root, EnrollmentCredential: "grant-credential-012345678901234567890"}); err != nil {
		t.Fatal(err)
	}
	var identity RuntimeIdentity
	body, _ := os.ReadFile(filepath.Join(root, "runtime-identity.json"))
	if err := json.Unmarshal(body, &identity); err != nil {
		t.Fatal(err)
	}
	identity.ExpiresAt = now.Add(-30 * 24 * time.Hour)
	if err := writeIdentity(root, identity); err != nil {
		t.Fatal(err)
	}
	source, err := NewRenewingTokenSource(RenewingTokenConfig{ControlURL: server.URL, StateRoot: root, Transport: server.Client().Transport, Clock: func() time.Time { return now }, OperationID: func() (string, error) { return "op_renew_expired", nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Token(context.Background()); err != nil {
		t.Fatalf("renew expired identity: %v", err)
	}
}

func TestRenewingTokenSourceRecordsRenewalFailure(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	root := t.TempDir()
	if err := writeIdentity(root, RuntimeIdentity{Version: 1, HelperID: "hlp_1", EnvironmentID: "env_1", Credential: "expired-helper-credential-012345678901234567890", ExpiresAt: now.Add(-time.Hour), KeyID: "ed25519:test"}); err != nil {
		t.Fatal(err)
	}
	metric := &renewalMetric{}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	source, err := NewRenewingTokenSource(RenewingTokenConfig{ControlURL: "https://127.0.0.1:1", StateRoot: root, Transport: transport, Timeout: time.Millisecond, Clock: func() time.Time { return now }, OperationID: func() (string, error) { return "op_renew_failure", nil }, Metrics: metric})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Token(context.Background()); err == nil {
		t.Fatal("renewal unexpectedly succeeded")
	}
	metric.mu.Lock()
	defer metric.mu.Unlock()
	if metric.count != 1 {
		t.Fatalf("renewal failure metric=%v", metric.count)
	}
}
