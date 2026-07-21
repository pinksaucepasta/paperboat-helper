package enrollment

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRenewingTokenSourceRenewsAndPersistsIdentity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	renewedCredential := "renewed-helper-credential-012345678901234567890"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/helpers/enroll":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"helper_id": "hlp_1", "environment_id": "env_1", "credential": "initial-helper-credential-012345678901234567890", "expires_at": now.Add(5 * time.Minute)}})
		case "/v1/helpers/renew":
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer initial-helper") || r.Header.Get("X-Paperboat-Helper-Proof") == "" {
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
