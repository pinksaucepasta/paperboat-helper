package enrollment

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEnrollBindsKeyAndPersistsPrivateIdentity(t *testing.T) {
	stateRoot := filepath.Join(t.TempDir(), "state")
	credential := strings.Repeat("g", 32)
	var publicKey string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/control/v1/helper-enrollments" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		var input struct {
			Credential string `json:"credential"`
			PublicKey  string `json:"public_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Fatal(err)
		}
		if input.Credential != credential {
			t.Error("enrollment credential changed")
		}
		if decoded, err := base64.RawURLEncoding.DecodeString(input.PublicKey); err != nil || len(decoded) != 32 {
			t.Errorf("invalid public key: %q", input.PublicKey)
		}
		publicKey = input.PublicKey
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"helper_id":"helper_1","environment_id":"env_1","credential":"identity-credential-0123456789012345","expires_at":"2099-01-01T00:00:00Z"}}`))
	}))
	defer server.Close()

	client, err := NewClient(server.Client().Transport, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Enroll(context.Background(), Config{ControlURL: server.URL + "/control", StateRoot: stateRoot, EnrollmentCredential: credential})
	if err != nil {
		t.Fatal(err)
	}
	if result.HelperID != "helper_1" || result.EnvironmentID != "env_1" || result.KeyID == "" || publicKey == "" {
		t.Fatalf("unexpected result: %#v", result)
	}
	path := filepath.Join(stateRoot, "runtime-identity.json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode: %v %v", info, err)
	}
	body, _ := os.ReadFile(path)
	if !strings.Contains(string(body), `"helper_id":"helper_1"`) || !strings.Contains(string(body), `"key_id":"`+result.KeyID+`"`) {
		t.Fatalf("persisted identity: %s", body)
	}
	token, err := (TokenSource{StateRoot: stateRoot, Clock: func() time.Time { return time.Date(2098, 1, 1, 0, 0, 0, 0, time.UTC) }}).Token(context.Background())
	if err != nil || token != result.Credential {
		t.Fatalf("token=%q err=%v", token, err)
	}
	proof, err := (ProofSource{StateRoot: stateRoot, Clock: func() time.Time { return time.Date(2098, 1, 1, 0, 0, 0, 0, time.UTC) }}).Proof(context.Background(), "op_admission_0001", http.MethodPost, "/v1/connectors/admission", []byte(`{"edge_pool":"default"}`))
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Algorithm string `json:"alg"`
		Payload   string `json:"payload"`
		Signature string `json:"signature"`
	}
	if err := json.Unmarshal(proof, &envelope); err != nil || envelope.Algorithm != "EdDSA" {
		t.Fatalf("proof=%s err=%v", proof, err)
	}
	payload, payloadErr := base64.RawURLEncoding.DecodeString(envelope.Payload)
	signature, signatureErr := base64.RawURLEncoding.DecodeString(envelope.Signature)
	public, publicErr := base64.RawURLEncoding.DecodeString(publicKey)
	if payloadErr != nil || signatureErr != nil || publicErr != nil || !ed25519.Verify(ed25519.PublicKey(public), payload, signature) {
		t.Fatal("helper proof signature is invalid")
	}
}

func TestHostedBootstrapUsesHelperProofAndValidatesMemoryOnlyMaterial(t *testing.T) {
	stateRoot := filepath.Join(t.TempDir(), "state")
	script := "echo setup\n"
	digest := sha256.Sum256([]byte(script))
	expiresAt := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339Nano)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/helper-enrollments":
			_, _ = w.Write([]byte(`{"data":{"helper_id":"helper_1","environment_id":"env_1","credential":"identity-credential-0123456789012345","expires_at":"2099-01-01T00:00:00Z"}}`))
		case "/v1/hosted-helper-bootstrap":
			if r.Header.Get("Authorization") != "Bearer identity-credential-0123456789012345" ||
				r.Header.Get("X-Paperboat-Helper-Proof") == "" {
				t.Error("hosted bootstrap proof headers are missing")
			}
			_, _ = w.Write([]byte(`{"data":{"setup_script_ref":"setup_1","setup_script":` +
				strconv.Quote(script) + `,"setup_script_sha256":"` + hex.EncodeToString(digest[:]) +
				`","source_username":"x-access-token","source_password":"short-lived-token","source_expires_at":"` +
				expiresAt + `"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := NewClient(server.Client().Transport, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		ControlURL: server.URL, StateRoot: stateRoot,
		EnrollmentCredential: strings.Repeat("g", 32),
	}
	if _, err = client.Enroll(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	bootstrap, err := client.HostedBootstrap(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if bootstrap.SetupScript != script || bootstrap.SourceUsername != "x-access-token" ||
		bootstrap.SourcePassword != "short-lived-token" || bootstrap.SourceExpiresAt == nil {
		t.Fatalf("bootstrap = %#v", bootstrap)
	}
}

func TestLoadConfigRequiresPrivateRegularFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "enroll.json")
	body := `{"control_url":"https://api.test","state_root":"` + filepath.Join(root, "state") + `","enrollment_credential":"` + strings.Repeat("x", 32) + `"}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("public enrollment config accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil || config.ControlURL != "https://api.test" {
		t.Fatalf("config=%#v err=%v", config, err)
	}
}

func TestLoadConfigRejectsDuplicateKeys(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "enroll.json")
	body := `{"control_url":"https://api.test","control_url":"https://attacker.test","state_root":"` + filepath.Join(root, "state") + `","enrollment_credential":"` + strings.Repeat("x", 32) + `"}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("duplicate config key accepted")
	}
}

func TestEnrollRejectsHTTPAndOversizedResponse(t *testing.T) {
	client, _ := NewClient(http.DefaultTransport, time.Second)
	if _, err := client.Enroll(context.Background(), Config{ControlURL: "http://api.test", StateRoot: filepath.Join(t.TempDir(), "state"), EnrollmentCredential: strings.Repeat("x", 32)}); err == nil {
		t.Fatal("plaintext control URL accepted")
	}

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 64<<10+1)))
	}))
	defer server.Close()
	client, _ = NewClient(server.Client().Transport, time.Second)
	if _, err := client.Enroll(context.Background(), Config{ControlURL: server.URL, StateRoot: filepath.Join(t.TempDir(), "state"), EnrollmentCredential: strings.Repeat("x", 32)}); err == nil {
		t.Fatal("oversized enrollment response accepted")
	}
}

func TestRuntimeIdentityRejectsExpiredAndMismatchedKey(t *testing.T) {
	stateRoot := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	value := RuntimeIdentity{Version: 1, HelperID: "helper_1", EnvironmentID: "env_1", Credential: strings.Repeat("x", 32), ExpiresAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), KeyID: "ed25519:wrong"}
	if err := writeIdentity(stateRoot, value); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuntimeIdentity(stateRoot, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("expired runtime identity accepted")
	}
	value.ExpiresAt = time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := writeIdentity(stateRoot, value); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRuntimeIdentity(stateRoot, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("runtime identity for another key accepted")
	}
}
