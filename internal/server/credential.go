package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

var ErrCredentialPolicy = errors.New("credential policy unavailable")

type CredentialVerifier interface {
	Verify(context.Context, string, auth.Policy) (auth.Claims, error)
}

type PolicyResolver interface {
	Policy(protocol.Frame) (auth.Policy, error)
}

type CredentialAuthorizer struct {
	Verifier CredentialVerifier
	Resolver PolicyResolver
	Token    string
}

func (a CredentialAuthorizer) Authorize(ctx context.Context, frame protocol.Frame) (Authorization, error) {
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
	binding, err := stableClaimsBinding(claims)
	if err != nil {
		return Authorization{}, err
	}
	return Authorization{
		JournalBinding: binding,
		EnvironmentID:  claims.EnvironmentID,
		UserID:         claims.UserID,
		ClientID:       claims.ClientSessionID,
		SessionID:      claims.SessionID,
		ResourceID:     claims.AssignmentID,
		ExpiresAt:      time.Unix(claims.ExpiresAt, 0).UTC(),
		Value:          claims,
	}, nil
}

func stableClaimsBinding(claims auth.Claims) (string, error) {
	// Exclude token-instance fields (JTI and timestamps) so a renewed credential
	// can retrieve the same durable operation result without crossing identity,
	// resource, class, or exact-scope boundaries.
	encoded, err := json.Marshal(struct {
		Issuer          string   `json:"issuer"`
		Subject         string   `json:"subject"`
		Class           string   `json:"class"`
		Scopes          []string `json:"scopes"`
		EnvironmentID   string   `json:"environment_id"`
		UserID          string   `json:"user_id"`
		ClientSessionID string   `json:"client_session_id"`
		HelperID        string   `json:"helper_id"`
		SessionID       string   `json:"session_id"`
		AssignmentID    string   `json:"assignment_id"`
	}{claims.Issuer, claims.Subject, claims.CredentialClass, claims.Scope, claims.EnvironmentID, claims.UserID, claims.ClientSessionID, claims.HelperID, claims.SessionID, claims.AssignmentID})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
