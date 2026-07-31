package auth

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type keySource struct {
	mu        sync.Mutex
	keys      map[string]ed25519.PublicKey
	refreshed map[string]ed25519.PublicKey
	refreshes int
}

func (s *keySource) Lookup(_ context.Context, kid string) (ed25519.PublicKey, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.keys[kid]
	return key, ok, nil
}
func (s *keySource) Refresh(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshes++
	for k, v := range s.refreshed {
		s.keys[k] = v
	}
	return nil
}

type replayStore struct {
	mu   sync.Mutex
	used map[string]bool
}

func (s *replayStore) Consume(jti string, _ time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.used[jti] {
		return false
	}
	s.used[jti] = true
	return true
}

type revocations struct{ revoked bool }

func (r revocations) Revoked(Claims) bool { return r.revoked }

type signingFixture struct {
	Key struct {
		Kid, Seed, Public string `json:"-"`
	} `json:"key"`
	Header header `json:"header"`
	Claims Claims `json:"claims"`
	Token  string `json:"token"`
}

func loadFixture(t *testing.T) (signingFixture, ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	b, err := os.ReadFile("../../testdata/contracts/fixtures/credentials/terminal-operation.ed25519.json")
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Key struct {
			Kid    string `json:"kid"`
			Seed   string `json:"seed_base64url"`
			Public string `json:"public_base64url"`
		} `json:"key"`
		Header header `json:"header"`
		Claims Claims `json:"claims"`
		Token  string `json:"token"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	seed, err := base64.RawURLEncoding.DecodeString(raw.Key.Seed)
	if err != nil {
		t.Fatal(err)
	}
	private := ed25519.NewKeyFromSeed(seed)
	public := private.Public().(ed25519.PublicKey)
	return signingFixture{Header: raw.Header, Claims: raw.Claims, Token: raw.Token}, private, public
}

func terminalPolicy(c Claims) Policy {
	return Policy{Issuer: c.Issuer, Audience: "paperboat-machine", CredentialClass: "terminal_operation", Scopes: []string{"terminal:operate"}, EnvironmentID: c.EnvironmentID, MachineID: c.MachineID, UserID: c.UserID, CLIClientSessionID: c.CLIClientSessionID, SessionID: c.SessionID, MaxLifetime: 5 * time.Minute}
}

func TestVerifierAcceptsSignedContractVector(t *testing.T) {
	fixture, _, public := loadFixture(t)
	keys := &keySource{keys: map[string]ed25519.PublicKey{"test-key-1": public}}
	v := Verifier{Keys: keys, Clock: fixedClock{time.Unix(fixture.Claims.IssuedAt+1, 0)}, ClockSkew: time.Minute}
	claims, err := v.Verify(context.Background(), fixture.Token, terminalPolicy(fixture.Claims))
	if err != nil || claims.JTI != fixture.Claims.JTI || claims.CLIClientSessionID == "" {
		t.Fatalf("claims=%#v err=%v", claims, err)
	}
}

func TestVerifierRejectsAudienceScopeBindingTimeAndRevocation(t *testing.T) {
	fixture, private, public := loadFixture(t)
	base := terminalPolicy(fixture.Claims)
	cases := []struct {
		name    string
		policy  Policy
		claims  Claims
		now     time.Time
		revoked bool
		want    Code
	}{
		{"audience", func() Policy { p := base; p.Audience = "paperboat-edge"; return p }(), fixture.Claims, time.Unix(fixture.Claims.IssuedAt+1, 0), false, AudienceInvalid},
		{"scope", func() Policy { p := base; p.Scopes = []string{"file:stage"}; return p }(), fixture.Claims, time.Unix(fixture.Claims.IssuedAt+1, 0), false, ScopeInvalid},
		{"environment", func() Policy { p := base; p.EnvironmentID = "env_other"; return p }(), fixture.Claims, time.Unix(fixture.Claims.IssuedAt+1, 0), false, BindingInvalid},
		{"expired", base, fixture.Claims, time.Unix(fixture.Claims.ExpiresAt+61, 0), false, Expired},
		{"not-yet", base, fixture.Claims, time.Unix(fixture.Claims.IssuedAt-61, 0), false, NotYetValid},
		{"revoked", base, fixture.Claims, time.Unix(fixture.Claims.IssuedAt+1, 0), true, Revoked},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token := signToken(t, fixture.Header, tc.claims, private)
			v := Verifier{Keys: &keySource{keys: map[string]ed25519.PublicKey{"test-key-1": public}}, Clock: fixedClock{tc.now}, ClockSkew: time.Minute, Revocations: revocations{tc.revoked}}
			_, err := v.Verify(context.Background(), token, tc.policy)
			var ae *Error
			if !errors.As(err, &ae) || ae.Code != tc.want {
				t.Fatalf("err=%v want=%s", err, tc.want)
			}
		})
	}
}

func TestUnknownKeyRefreshesOnceAndSingleUseRejectsReplay(t *testing.T) {
	fixture, _, public := loadFixture(t)
	keys := &keySource{keys: map[string]ed25519.PublicKey{}, refreshed: map[string]ed25519.PublicKey{"test-key-1": public}}
	replays := &replayStore{used: map[string]bool{}}
	policy := terminalPolicy(fixture.Claims)
	policy.SingleUse = true
	v := Verifier{Keys: keys, Clock: fixedClock{time.Unix(fixture.Claims.IssuedAt+1, 0)}, Replays: replays}
	if _, err := v.Verify(context.Background(), fixture.Token, policy); err != nil {
		t.Fatal(err)
	}
	if keys.refreshes != 1 {
		t.Fatalf("refreshes=%d", keys.refreshes)
	}
	_, err := v.Verify(context.Background(), fixture.Token, policy)
	var ae *Error
	if !errors.As(err, &ae) || ae.Code != Replayed {
		t.Fatalf("err=%v", err)
	}
}

func TestVerifierRejectsAlgorithmDowngradeBeforeMutation(t *testing.T) {
	fixture, private, public := loadFixture(t)
	fixture.Header.Algorithm = "HS256"
	token := signToken(t, fixture.Header, fixture.Claims, private)
	v := Verifier{Keys: &keySource{keys: map[string]ed25519.PublicKey{"test-key-1": public}}, Clock: fixedClock{time.Unix(fixture.Claims.IssuedAt+1, 0)}}
	_, err := v.Verify(context.Background(), token, terminalPolicy(fixture.Claims))
	var ae *Error
	if !errors.As(err, &ae) || ae.Code != AlgorithmInvalid {
		t.Fatalf("err=%v", err)
	}
}

func TestVerifierRejectsDuplicateHeaderKey(t *testing.T) {
	fixture, private, public := loadFixture(t)
	headerJSON := []byte(`{"alg":"EdDSA","alg":"EdDSA","kid":"test-key-1","typ":"paperboat-credential+jwt"}`)
	claimsJSON, err := json.Marshal(fixture.Claims)
	if err != nil {
		t.Fatal(err)
	}
	first := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)
	token := first + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte(first)))
	v := Verifier{Keys: &keySource{keys: map[string]ed25519.PublicKey{"test-key-1": public}}, Clock: fixedClock{time.Unix(fixture.Claims.IssuedAt+1, 0)}}
	_, err = v.Verify(context.Background(), token, terminalPolicy(fixture.Claims))
	var ae *Error
	if !errors.As(err, &ae) || ae.Code != Malformed {
		t.Fatalf("err=%v", err)
	}
}

func TestVerifierRejectsUserSubjectConfusion(t *testing.T) {
	fixture, private, public := loadFixture(t)
	fixture.Claims.Subject = "usr_other"
	verifier := Verifier{Keys: &keySource{keys: map[string]ed25519.PublicKey{"test-key-1": public}}, Clock: fixedClock{time.Unix(fixture.Claims.IssuedAt+1, 0)}}
	token := signToken(t, fixture.Header, fixture.Claims, private)
	_, err := verifier.Verify(context.Background(), token, terminalPolicy(fixture.Claims))
	var authErr *Error
	if !errors.As(err, &authErr) || authErr.Code != BindingInvalid {
		t.Fatalf("err=%v", err)
	}
}

func signToken(t *testing.T, h header, c Claims, key ed25519.PrivateKey) string {
	t.Helper()
	hb, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	first := base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb)
	return first + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(first)))
}
