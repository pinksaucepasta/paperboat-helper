package preview

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

type controlToken func(context.Context) (string, error)

func (f controlToken) Token(ctx context.Context) (string, error) { return f(ctx) }

type controlProof func(context.Context, string, string, string, []byte) ([]byte, error)

func (f controlProof) Proof(ctx context.Context, op, method, path string, body []byte) ([]byte, error) {
	return f(ctx, op, method, path, body)
}

func TestControlClientSignsRegisterAndAcceptsCanonicalIdentity(t *testing.T) {
	var gotBody string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		if r.Header.Get("Authorization") != "Bearer preview" || r.Header.Get("X-Paperboat-Helper-Identity") != "identity" || r.Header.Get("X-Paperboat-Helper-Proof") != base64.RawURLEncoding.EncodeToString([]byte("proof")) {
			t.Errorf("authentication headers missing")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"id":"prv_1","environment_id":"env_1","logical_name":"web","preview_key":"p-abcdefghijklmnopqrstuvwxyz","url":"https://p-abcdefghijklmnopqrstuvwxyz.preview.test","target_port":3000,"state":"registering"}}`))
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client, err := NewControlClient(ControlClientConfig{Endpoint: server.URL + "/v1/previews/operations", AllowedHosts: []string{u.Hostname()}, EnvironmentID: "env_1", Tokens: controlToken(func(context.Context) (string, error) { return "preview", nil }), Identities: controlToken(func(context.Context) (string, error) { return "identity", nil }), Proofs: controlProof(func(_ context.Context, op, method, path string, body []byte) ([]byte, error) {
		if len(op) < 8 || method != "POST" || path != "/v1/previews/operations" {
			t.Fatalf("proof input %q %q %q", op, method, path)
		}
		return []byte("proof"), nil
	}), Transport: server.Client().Transport})
	if err != nil {
		t.Fatal(err)
	}
	record, err := client.Register(context.Background(), "web", Target{Host: "127.0.0.1", Port: 3000}, true, 0, false)
	if err != nil || record.PreviewKey != "p-abcdefghijklmnopqrstuvwxyz" || record.EnvironmentID != "env_1" {
		t.Fatalf("record=%#v err=%v", record, err)
	}
	if gotBody == "" {
		t.Fatal("missing request body")
	}
}

func TestControlClientRejectsCrossEnvironmentResponse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"prv_2","environment_id":"env_other","logical_name":"secret","preview_key":"p-zyxwvutsrqponmlkjihgfedcba","url":"https://p-zyxwvutsrqponmlkjihgfedcba.preview.test","target_port":4000,"state":"ready"}]}`))
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	client, err := NewControlClient(ControlClientConfig{Endpoint: server.URL + "/v1/previews/operations", AllowedHosts: []string{u.Hostname()}, EnvironmentID: "env_1", Tokens: controlToken(func(context.Context) (string, error) { return "preview", nil }), Identities: controlToken(func(context.Context) (string, error) { return "identity", nil }), Proofs: controlProof(func(context.Context, string, string, string, []byte) ([]byte, error) { return []byte("proof"), nil }), Transport: server.Client().Transport})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.List(context.Background()); err == nil {
		t.Fatal("cross-environment response accepted")
	}
}
