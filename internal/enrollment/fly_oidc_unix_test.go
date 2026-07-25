//go:build unix

package enrollment

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRequestFlyWorkloadIdentityUsesLocalSocketAndExactAudience(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "pb-fly-oidc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socketPath := filepath.Join(root, "api.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/v1/tokens/oidc" {
				http.Error(w, "unexpected request", http.StatusBadRequest)
				return
			}
			var input struct {
				Audience string `json:"aud"`
			}
			if json.NewDecoder(r.Body).Decode(&input) != nil ||
				input.Audience != "https://control.example/v1/hosted-helper-enrollments" {
				http.Error(w, "unexpected audience", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte("header.payload.signature-with-enough-length"))
		}),
	}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()

	token, err := requestFlyWorkloadIdentityAt(
		context.Background(),
		"https://control.example/v1/hosted-helper-enrollments",
		time.Second,
		socketPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	if token != "header.payload.signature-with-enough-length" {
		t.Fatalf("token = %q", token)
	}
}

func TestRequestFlyWorkloadIdentityRejectsRelativeSocket(t *testing.T) {
	if _, err := requestFlyWorkloadIdentityAt(context.Background(), "audience", time.Second, "fly.sock"); err == nil {
		t.Fatal("relative Fly API socket was accepted")
	}
}
