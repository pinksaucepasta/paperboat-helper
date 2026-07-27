package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

var ErrCredentialPolicy = errors.New("credential policy unavailable")

type CredentialVerifier interface {
	Verify(context.Context, string, auth.Policy) (auth.Claims, error)
}

type CredentialRevocationWatcher interface {
	Watch(string) (*atomic.Bool, <-chan struct{}, func(), error)
}

type PolicyResolver interface {
	Policy(protocol.Frame) (auth.Policy, error)
}

type CredentialAuthorizer struct {
	Verifier      CredentialVerifier
	Resolver      PolicyResolver
	Token         string
	Revocations   CredentialRevocationWatcher
	mu            sync.Mutex
	jti           string
	revoked       *atomic.Bool
	revokedSignal <-chan struct{}
	release       func()
}

func (a *CredentialAuthorizer) Authorize(ctx context.Context, frame protocol.Frame) (Authorization, error) {
	if a.Verifier == nil || a.Resolver == nil || a.Token == "" {
		return Authorization{}, ErrCredentialPolicy
	}
	policy, err := a.Resolver.Policy(frame)
	if err != nil {
		return Authorization{}, err
	}
	claims, err := a.Verifier.Verify(ctx, a.Token, policy)
	if err != nil {
		return Authorization{}, err
	}
	var revoked *atomic.Bool
	if a.Revocations != nil {
		a.mu.Lock()
		if a.jti != "" && a.jti != claims.JTI {
			a.mu.Unlock()
			return Authorization{}, ErrCredentialPolicy
		}
		if a.revoked == nil {
			a.revoked, a.revokedSignal, a.release, err = a.Revocations.Watch(claims.JTI)
			a.jti = claims.JTI
		}
		revoked = a.revoked
		a.mu.Unlock()
		if err != nil {
			return Authorization{}, err
		}
	}
	binding, err := stableClaimsBinding(claims)
	if err != nil {
		return Authorization{}, err
	}
	return Authorization{
		JournalBinding: binding,
		EnvironmentID:  claims.EnvironmentID,
		UserID:         claims.UserID,
		ClientID:       claims.CLIClientSessionID,
		SessionID:      claims.SessionID,
		ResourceID:     claims.AssignmentID,
		ExpiresAt:      time.Unix(claims.ExpiresAt, 0).UTC(),
		Revoked:        revoked,
		RevokedSignal:  a.revokedSignal,
		Value:          claims,
	}, nil
}

func (a *CredentialAuthorizer) CloseAuthorization() {
	a.mu.Lock()
	if a.release != nil {
		a.release()
		a.release = nil
	}
	a.mu.Unlock()
}

func stableClaimsBinding(claims auth.Claims) (string, error) {
	// Exclude token-instance fields (JTI and timestamps) so a renewed credential
	// can retrieve the same durable operation result without crossing identity,
	// resource, class, or exact-scope boundaries.
	encoded, err := json.Marshal(struct {
		Issuer             string   `json:"issuer"`
		Subject            string   `json:"subject"`
		Class              string   `json:"class"`
		Scopes             []string `json:"scopes"`
		EnvironmentID      string   `json:"environment_id"`
		UserID             string   `json:"user_id"`
		CLIClientSessionID string   `json:"cli_client_session_id"`
		HelperID           string   `json:"helper_id"`
		SessionID          string   `json:"session_id"`
		AssignmentID       string   `json:"assignment_id"`
	}{claims.Issuer, claims.Subject, claims.CredentialClass, claims.Scope, claims.EnvironmentID, claims.UserID, claims.CLIClientSessionID, claims.HelperID, claims.SessionID, claims.AssignmentID})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
