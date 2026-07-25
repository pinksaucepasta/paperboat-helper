package runtime

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

type staticClock struct{ now time.Time }

func (c staticClock) Now() time.Time { return c.now }

func signStaticCredential(t *testing.T, private ed25519.PrivateKey, keyID string, claims auth.Claims) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "EdDSA", "kid": keyID, "typ": "paperboat-credential+jwt"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte(signingInput)))
}

func TestStaticAuthorizerVerifiesExactFakePeerCredentialPolicies(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	factory, err := NewStaticAuthorizer(StaticAuthConfig{Issuer: "https://control.test", EnvironmentID: "env_test", HelperID: "hlp_test", Keys: map[string]ed25519.PublicKey{"key-1": public}, RevokedJTIs: []string{"jti_revoked"}, Clock: staticClock{now}})
	if err != nil {
		t.Fatal(err)
	}
	claims := auth.Claims{Issuer: "https://control.test", Audience: "paperboat-helper", Subject: "usr_test", JTI: "jti_valid", IssuedAt: now.Add(-time.Minute).Unix(), ExpiresAt: now.Add(time.Minute).Unix(), Scope: []string{"terminal:operate"}, CredentialClass: "terminal_operation", EnvironmentID: "env_test", UserID: "usr_test", CLIClientSessionID: "cli_test", SessionID: "ses_test"}
	authorizer, err := factory(signStaticCredential(t, private, "key-1", claims))
	if err != nil {
		t.Fatal(err)
	}
	authorization, err := authorizer.Authorize(context.Background(), protocol.Frame{Capability: "health.v1"})
	if err != nil || authorization.EnvironmentID != "env_test" || authorization.ClientID != "cli_test" || authorization.SessionID != "ses_test" {
		t.Fatalf("authorization=%#v err=%v", authorization, err)
	}
	for name, mutate := range map[string]func(*auth.Claims){
		"cross_environment": func(value *auth.Claims) { value.EnvironmentID = "env_other" },
		"wrong_scope":       func(value *auth.Claims) { value.Scope = []string{"file:stage"} },
		"revoked":           func(value *auth.Claims) { value.JTI = "jti_revoked" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := claims
			invalid.Scope = append([]string(nil), claims.Scope...)
			mutate(&invalid)
			candidate, err := factory(signStaticCredential(t, private, "key-1", invalid))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := candidate.Authorize(context.Background(), protocol.Frame{Capability: "health.v1"}); err == nil {
				t.Fatal("invalid credential accepted")
			}
		})
	}
}

func TestStaticAuthorizerMapsConfigAssignmentOnlyAfterHelperVerification(t *testing.T) {
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now().UTC()
	factory, err := NewStaticAuthorizer(StaticAuthConfig{Issuer: "https://control.test", EnvironmentID: "env_test", HelperID: "hlp_test", Keys: map[string]ed25519.PublicKey{"key-1": public}, Clock: staticClock{now}})
	if err != nil {
		t.Fatal(err)
	}
	claims := auth.Claims{Issuer: "https://control.test", Audience: "paperboat-helper", Subject: "hlp_test", JTI: "jti_config", IssuedAt: now.Add(-time.Minute).Unix(), ExpiresAt: now.Add(time.Minute).Unix(), Scope: []string{"config:pull", "config:apply", "config:report"}, CredentialClass: "config_sync", EnvironmentID: "env_test", HelperID: "hlp_test", AssignmentID: "asn_test", WarningRevision: "warning_1"}
	authorizer, _ := factory(signStaticCredential(t, private, "key-1", claims))
	authorization, err := authorizer.Authorize(context.Background(), protocol.Frame{Capability: "config.apply.v1"})
	if err != nil || authorization.ResourceID != "asn_test" {
		t.Fatalf("authorization=%#v err=%v", authorization, err)
	}
	claims.HelperID = "hlp_other"
	authorizer, _ = factory(signStaticCredential(t, private, "key-1", claims))
	if _, err := authorizer.Authorize(context.Background(), protocol.Frame{Capability: "config.apply.v1"}); err == nil {
		t.Fatal("wrong helper accepted")
	}
}

func TestStaticAuthorizerRejectsInvalidConfiguration(t *testing.T) {
	if _, err := NewStaticAuthorizer(StaticAuthConfig{}); !errors.Is(err, ErrStaticAuthInvalid) {
		t.Fatalf("err=%v", err)
	}
}
