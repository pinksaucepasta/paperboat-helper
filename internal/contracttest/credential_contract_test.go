package contracttest

import (
	"bufio"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestCredentialSigningVector(t *testing.T) {
	b, err := os.ReadFile("../../testdata/contracts/fixtures/credentials/terminal-operation.ed25519.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		TestOnly bool `json:"test_only"`
		Key      struct {
			Kid    string `json:"kid"`
			Public string `json:"public_base64url"`
		} `json:"key"`
		Header map[string]string `json:"header"`
		Claims struct {
			Audience        string   `json:"aud"`
			CredentialClass string   `json:"credential_class"`
			EnvironmentID   string   `json:"environment_id"`
			MachineID       string   `json:"machine_id"`
			Scopes          []string `json:"scope"`
		} `json:"claims"`
		Token string `json:"token"`
	}
	if err := json.Unmarshal(b, &vector); err != nil {
		t.Fatal(err)
	}
	if !vector.TestOnly {
		t.Fatal("credential vector must be marked test-only")
	}
	parts := strings.Split(vector.Token, ".")
	if len(parts) != 3 {
		t.Fatal("token is not compact JWS")
	}
	publicKey, err := base64.RawURLEncoding.DecodeString(vector.Key.Public)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(publicKey, []byte(parts[0]+"."+parts[1]), signature) {
		t.Fatal("credential signature is invalid")
	}
	if vector.Header["alg"] != "EdDSA" || vector.Header["kid"] != vector.Key.Kid {
		t.Fatalf("unexpected header: %#v", vector.Header)
	}
	if vector.Claims.Audience != "paperboat-machine" || vector.Claims.CredentialClass != "terminal_operation" || vector.Claims.EnvironmentID == "" || vector.Claims.MachineID == "" || len(vector.Claims.Scopes) != 1 || vector.Claims.Scopes[0] != "terminal:operate" {
		t.Fatalf("unexpected claims: %#v", vector.Claims)
	}
}

func TestCredentialNegativeVectorCoverage(t *testing.T) {
	required := map[string]bool{"wrong-audience": false, "wrong-environment": false, "wrong-scope": false, "unknown-key": false, "expired": false, "not-yet-valid": false, "replayed": false, "revoked": false}
	f, err := os.Open("../../testdata/contracts/fixtures/credentials/negative.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var vector struct {
			Case    string `json:"case"`
			Valid   bool   `json:"valid"`
			Error   string `json:"error"`
			Mutated bool   `json:"mutated"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &vector); err != nil {
			t.Fatal(err)
		}
		if _, ok := required[vector.Case]; !ok || vector.Valid || vector.Error == "" || vector.Mutated {
			t.Fatalf("invalid negative credential vector: %#v", vector)
		}
		required[vector.Case] = true
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for name, seen := range required {
		if !seen {
			t.Errorf("missing credential vector %q", name)
		}
	}
}
