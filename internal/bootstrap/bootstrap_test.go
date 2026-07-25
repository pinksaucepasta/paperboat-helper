package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestDashboardTokenPairingAndMaterialExchange(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().UTC().Add(time.Minute)
	var pairingCalls, materialCalls int
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/v1/user-machines/pairings":
			pairingCalls++
			var body map[string]any
			if json.NewDecoder(request.Body).Decode(&body) != nil || body["enrollment_token"] != "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOP" || body["platform"] != runtime.GOOS || body["architecture"] != runtime.GOARCH || body["workspace_root"] != workspace {
				t.Fatalf("pairing body=%v", body)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"data": Pairing{ID: "cmp_1", UserCode: "ABCD1234", ExpiresAt: expires}})
		case "/v1/user-machines/pairings/installation":
			materialCalls++
			if materialCalls == 1 {
				writer.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(writer).Encode(map[string]any{"error": map[string]string{"code": "user_machine_approval_pending", "message": "Machine approval is pending."}})
				return
			}
			manifest, publicKey := signedArtifact(t, server.URL, []byte("helper"))
			_ = json.NewEncoder(writer).Encode(map[string]any{"data": Material{Schema: "paperboat.byod-installation/v1", UserMachineID: "um_1", UserMachineEnrollmentID: "ume_1", EnvironmentID: "env_1", ControlURL: server.URL, HelperID: "helper_1", EnrollmentID: "enroll_1", EnrollmentCredential: "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOP", ExpiresAt: expires, Artifact: &manifest, ArtifactPublicKey: publicKey, HelperListenAddress: "127.0.0.1:38080"}})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	config := Config{ServerURL: server.URL, EnrollmentToken: "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOP", DisplayName: "Studio", WorkspaceRoot: workspace, Verifier: "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOP", HTTP: server.Client()}
	pairing, err := CreatePairing(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	material, err := WaitForMaterial(context.Background(), config, pairing.ExpiresAt, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if pairingCalls != 1 || materialCalls != 2 || material.EnvironmentID != "env_1" {
		t.Fatalf("pairing=%d material=%d result=%+v", pairingCalls, materialCalls, material)
	}
}

func TestWaitForMaterialStopsOnTerminalServerErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		code   string
		want   error
	}{
		{name: "denied", status: http.StatusForbidden, code: "user_machine_pairing_denied", want: ErrPairingDenied},
		{name: "expired", status: http.StatusGone, code: "user_machine_pairing_expired", want: ErrPairingExpired},
		{name: "unavailable", status: http.StatusGone, code: "user_machine_installation_unavailable", want: ErrInstallationUnavailable},
		{name: "server failure", status: http.StatusInternalServerError, code: "internal_error", want: ErrInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				calls++
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(test.status)
				_ = json.NewEncoder(writer).Encode(map[string]any{"error": map[string]string{"code": test.code, "message": "test"}})
			}))
			defer server.Close()
			config := Config{ServerURL: server.URL, EnrollmentToken: "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOP", DisplayName: "Studio", WorkspaceRoot: workspace, Verifier: "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOP", HTTP: server.Client()}
			_, err = WaitForMaterial(context.Background(), config, time.Now().UTC().Add(time.Minute), time.Millisecond)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if calls != 1 {
				t.Fatalf("requests = %d, want 1", calls)
			}
		})
	}
}

func TestValidateWorkspaceRejectsSymlink(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "workspace")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if ValidateWorkspace(link) == nil {
		t.Fatal("expected symlink workspace to be rejected")
	}
}
