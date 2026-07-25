package server

import (
	"context"
	"errors"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

type verifierFunc func(context.Context, string, auth.Policy) (auth.Claims, error)

func (f verifierFunc) Verify(ctx context.Context, token string, policy auth.Policy) (auth.Claims, error) {
	return f(ctx, token, policy)
}

type resolverFunc func(protocol.Frame) (auth.Policy, error)

func (f resolverFunc) Policy(frame protocol.Frame) (auth.Policy, error) { return f(frame) }

func TestCredentialAuthorizerReturnsStableBoundIdentity(t *testing.T) {
	claims := auth.Claims{Issuer: "https://api.test", Subject: "usr_1", JTI: "jti_1", IssuedAt: 1, ExpiresAt: 100, Scope: []string{"terminal:operate"}, CredentialClass: "terminal_operation", EnvironmentID: "env_1", UserID: "usr_1", CLIClientSessionID: "cli_1", SessionID: "ses_1", AssignmentID: "asn_1"}
	authorizer := CredentialAuthorizer{
		Token: "signed-token",
		Resolver: resolverFunc(func(frame protocol.Frame) (auth.Policy, error) {
			if frame.Capability != "terminal.v1" {
				return auth.Policy{}, ErrCredentialPolicy
			}
			return auth.Policy{Audience: "paperboat-helper"}, nil
		}),
		Verifier: verifierFunc(func(_ context.Context, token string, policy auth.Policy) (auth.Claims, error) {
			if token != "signed-token" || policy.Audience != "paperboat-helper" {
				return auth.Claims{}, errors.New("bad verification input")
			}
			return claims, nil
		}),
	}
	first, err := authorizer.Authorize(context.Background(), protocol.Frame{Capability: "terminal.v1"})
	if err != nil {
		t.Fatal(err)
	}
	claims.JTI, claims.IssuedAt, claims.ExpiresAt = "jti_2", 50, 150
	second, err := authorizer.Authorize(context.Background(), protocol.Frame{Capability: "terminal.v1"})
	if err != nil {
		t.Fatal(err)
	}
	if first.JournalBinding == "" || first.JournalBinding != second.JournalBinding {
		t.Fatalf("renewed binding first=%q second=%q", first.JournalBinding, second.JournalBinding)
	}
	if first.ResourceID != "asn_1" {
		t.Fatalf("resource id=%q", first.ResourceID)
	}
	claims.SessionID = "ses_2"
	third, err := authorizer.Authorize(context.Background(), protocol.Frame{Capability: "terminal.v1"})
	if err != nil {
		t.Fatal(err)
	}
	if third.JournalBinding == first.JournalBinding {
		t.Fatal("resource change did not change journal binding")
	}
}

func TestCredentialAuthorizerFailsClosedWithoutPolicy(t *testing.T) {
	authorizer := CredentialAuthorizer{Token: "token", Verifier: verifierFunc(func(context.Context, string, auth.Policy) (auth.Claims, error) { return auth.Claims{}, nil })}
	if _, err := authorizer.Authorize(context.Background(), protocol.Frame{}); !errors.Is(err, ErrCredentialPolicy) {
		t.Fatalf("err=%v", err)
	}
}
