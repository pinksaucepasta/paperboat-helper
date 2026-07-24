package configsync

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeCredentialSource struct {
	credential  Credential
	err         error
	invalidated int
}

func (s *fakeCredentialSource) Credential(context.Context) (Credential, error) {
	return s.credential, s.err
}
func (s *fakeCredentialSource) InvalidateCredential() { s.invalidated++ }

type fakeRuntime struct {
	starts, shutdowns int
	startErr          error
}

func (r *fakeRuntime) Start(context.Context) error {
	r.starts++
	return r.startErr
}
func (r *fakeRuntime) Shutdown(context.Context) error {
	r.shutdowns++
	return nil
}

func TestAuthorizedRuntimeDoesNotConstructFilesystemRuntimeWithoutEligibility(t *testing.T) {
	source := &fakeCredentialSource{err: ErrAuthorization}
	factoryCalls := 0
	_, err := NewAuthorizedRuntime(context.Background(), source, func(context.Context, Credential) (Runtime, error) {
		factoryCalls++
		return &fakeRuntime{}, nil
	})
	if !errors.Is(err, ErrAuthorization) || factoryCalls != 0 {
		t.Fatalf("activation error = %v, factory calls = %d", err, factoryCalls)
	}
}

func TestAuthorizedRuntimeRequiresCompleteBindingBeforeFactory(t *testing.T) {
	source := &fakeCredentialSource{credential: Credential{Value: "token", EnvironmentID: "env", HelperID: "helper", AssignmentID: "assignment", ExpiresAt: time.Now().Add(time.Minute)}}
	factoryCalls := 0
	_, err := NewAuthorizedRuntime(context.Background(), source, func(context.Context, Credential) (Runtime, error) {
		factoryCalls++
		return &fakeRuntime{}, nil
	})
	if !errors.Is(err, ErrAuthorization) || factoryCalls != 0 || source.invalidated != 1 {
		t.Fatalf("activation error = %v, factory calls = %d, invalidated = %d", err, factoryCalls, source.invalidated)
	}
}

func TestAuthorizedRuntimeRevalidatesAndClearsAuthorizationOnShutdown(t *testing.T) {
	source := &fakeCredentialSource{credential: Credential{
		Value: "token", EnvironmentID: "env", HelperID: "helper", AssignmentID: "assignment",
		WarningRevision: "warning-1", ExpiresAt: time.Now().Add(time.Minute),
	}}
	engine := &fakeRuntime{}
	authorized, err := NewAuthorizedRuntime(context.Background(), source, func(_ context.Context, got Credential) (Runtime, error) {
		if got.AssignmentID != "assignment" || got.WarningRevision != "warning-1" {
			t.Fatalf("factory credential = %#v", got)
		}
		return engine, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := authorized.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := authorized.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if engine.starts != 1 {
		t.Fatalf("starts = %d", engine.starts)
	}
	if err := authorized.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if source.invalidated != 1 || engine.shutdowns != 1 {
		t.Fatalf("invalidated = %d, shutdowns = %d", source.invalidated, engine.shutdowns)
	}
}
