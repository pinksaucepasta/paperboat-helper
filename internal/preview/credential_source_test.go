package preview

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestCredentialSourceCachesAndRefreshesInMemory(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		body, _ := io.ReadAll(r.Body)
		if string(body) != "{}" || r.Header.Get("Authorization") != "Bearer identity" || r.Header.Get("X-Paperboat-Helper-Proof") != base64.RawURLEncoding.EncodeToString([]byte("proof")) {
			t.Errorf("invalid credential request")
		}
		_, _ = fmt.Fprintf(w, `{"data":{"credential":"preview-%d","expires_at":%q}}`, requests, now.Add(5*time.Minute).Format(time.RFC3339))
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	source, err := NewCredentialSource(CredentialSourceConfig{Endpoint: server.URL + "/v1/previews/credentials", AllowedHosts: []string{u.Hostname()}, Identities: tokenSourceFunc(func(context.Context) (string, error) { return "identity", nil }), Proofs: controlProof(func(_ context.Context, op, method, path string, body []byte) ([]byte, error) {
		if op != "op_preview_credential" || method != http.MethodPost || path != "/v1/previews/credentials" || string(body) != "{}" {
			t.Fatalf("invalid proof input")
		}
		return []byte("proof"), nil
	}), OperationID: func() (string, error) { return "op_preview_credential", nil }, Transport: server.Client().Transport, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	first, err := source.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := source.Token(context.Background())
	if err != nil || first != second || requests != 1 {
		t.Fatalf("first=%q second=%q requests=%d err=%v", first, second, requests, err)
	}
	now = now.Add(4*time.Minute + 31*time.Second)
	third, err := source.Token(context.Background())
	if err != nil || third == second || requests != 2 {
		t.Fatalf("third=%q requests=%d err=%v", third, requests, err)
	}
}
