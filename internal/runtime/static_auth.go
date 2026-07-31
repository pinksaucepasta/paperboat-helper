package runtime

import (
	"context"
	"crypto/ed25519"
	"errors"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/server"
)

var ErrStaticAuthInvalid = errors.New("invalid static authorization configuration")

type StaticAuthConfig struct {
	Issuer        string
	EnvironmentID string
	HelperID      string
	Keys          map[string]ed25519.PublicKey
	RevokedJTIs   []string
	Clock         auth.Clock
}

type CredentialAuthConfig struct {
	Issuer        string
	EnvironmentID string
	HelperID      string
	Verifier      server.CredentialVerifier
	Revocations   server.CredentialRevocationWatcher
}

func NewCredentialAuthorizer(config CredentialAuthConfig) (server.AuthorizerFactory, error) {
	if config.Issuer == "" || config.EnvironmentID == "" || config.HelperID == "" || config.Verifier == nil {
		return nil, ErrStaticAuthInvalid
	}
	resolver := staticPolicyResolver{issuer: config.Issuer, environmentID: config.EnvironmentID, helperID: config.HelperID}
	return func(token string) (server.Authorizer, error) {
		if token == "" || len(token) > 16<<10 {
			return nil, ErrStaticAuthInvalid
		}
		return &server.CredentialAuthorizer{Verifier: config.Verifier, Resolver: resolver, Token: token, Revocations: config.Revocations}, nil
	}, nil
}

func NewStaticAuthorizer(config StaticAuthConfig) (server.AuthorizerFactory, error) {
	if config.Issuer == "" || config.EnvironmentID == "" || config.Clock == nil || len(config.Keys) == 0 {
		return nil, ErrStaticAuthInvalid
	}
	keys := make(map[string]ed25519.PublicKey, len(config.Keys))
	for keyID, key := range config.Keys {
		if keyID == "" || len(key) != ed25519.PublicKeySize {
			return nil, ErrStaticAuthInvalid
		}
		keys[keyID] = append(ed25519.PublicKey(nil), key...)
	}
	revoked := make(map[string]bool, len(config.RevokedJTIs))
	for _, jti := range config.RevokedJTIs {
		if jti == "" || revoked[jti] {
			return nil, ErrStaticAuthInvalid
		}
		revoked[jti] = true
	}
	verifier := auth.Verifier{Keys: staticKeys{keys: keys}, Clock: config.Clock, Revocations: staticRevocations(revoked), ClockSkew: time.Minute}
	resolver := staticPolicyResolver{issuer: config.Issuer, environmentID: config.EnvironmentID, helperID: config.HelperID}
	return func(token string) (server.Authorizer, error) {
		if token == "" || len(token) > 16<<10 {
			return nil, ErrStaticAuthInvalid
		}
		return &server.CredentialAuthorizer{Verifier: verifier, Resolver: resolver, Token: token}, nil
	}, nil
}

type staticKeys struct{ keys map[string]ed25519.PublicKey }

func (s staticKeys) Lookup(_ context.Context, keyID string) (ed25519.PublicKey, bool, error) {
	key, ok := s.keys[keyID]
	return append(ed25519.PublicKey(nil), key...), ok, nil
}
func (staticKeys) Refresh(context.Context) error { return nil }

type staticRevocations map[string]bool

func (r staticRevocations) Revoked(claims auth.Claims) bool { return r[claims.JTI] }

type staticPolicyResolver struct {
	issuer        string
	environmentID string
	helperID      string
}

func (r staticPolicyResolver) Policy(frame protocol.Frame) (auth.Policy, error) {
	base := auth.Policy{Issuer: r.issuer, Audience: "paperboat-helper", EnvironmentID: r.environmentID}
	switch frame.Capability {
	case "terminal.v1", "health.v1", "preview.public.v1":
		base.CredentialClass = "terminal_operation"
		base.Scopes = []string{"terminal:operate"}
		base.MaxLifetime = 5 * time.Minute
	case "file-transfer.v1":
		base.CredentialClass = "file_transfer"
		base.Scopes = []string{"file:transfer"}
		base.MaxLifetime = 5 * time.Minute
	case "config.apply.v1":
		if r.helperID == "" {
			return auth.Policy{}, ErrStaticAuthInvalid
		}
		base.CredentialClass = "config_sync"
		base.Scopes = []string{"config:pull", "config:apply", "config:report"}
		base.HelperID = r.helperID
		base.MaxLifetime = 5 * time.Minute
	default:
		return auth.Policy{}, ErrStaticAuthInvalid
	}
	return base, nil
}
