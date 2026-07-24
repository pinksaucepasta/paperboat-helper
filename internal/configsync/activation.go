package configsync

import (
	"context"
	"errors"
	"sync"
)

var ErrActivationInvalid = errors.New("invalid config sync activation")

type CredentialSource interface {
	Credential(context.Context) (Credential, error)
	InvalidateCredential()
}

type Runtime interface {
	Start(context.Context) error
	Shutdown(context.Context) error
}

type RuntimeFactory func(context.Context, Credential) (Runtime, error)

// AuthorizedRuntime is constructed only after fresh server eligibility has
// been proven. The factory is the first boundary allowed to resolve or inspect
// managed filesystem roots, create watchers, or load repository access.
type AuthorizedRuntime struct {
	credentials CredentialSource
	runtime     Runtime

	mu      sync.Mutex
	started bool
}

func NewAuthorizedRuntime(ctx context.Context, credentials CredentialSource, factory RuntimeFactory) (*AuthorizedRuntime, error) {
	if credentials == nil || factory == nil {
		return nil, ErrActivationInvalid
	}
	credential, err := credentials.Credential(ctx)
	if err != nil {
		return nil, errors.Join(ErrAuthorization, err)
	}
	if credential.Value == "" || credential.EnvironmentID == "" || credential.HelperID == "" ||
		credential.AssignmentID == "" || credential.WarningRevision == "" || credential.ExpiresAt.IsZero() {
		credentials.InvalidateCredential()
		return nil, ErrAuthorization
	}
	runtime, err := factory(ctx, credential)
	if err != nil || runtime == nil {
		credentials.InvalidateCredential()
		return nil, errors.Join(ErrActivationInvalid, err)
	}
	return &AuthorizedRuntime{credentials: credentials, runtime: runtime}, nil
}

func (r *AuthorizedRuntime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return nil
	}
	credential, err := r.credentials.Credential(ctx)
	if err != nil {
		return errors.Join(ErrAuthorization, err)
	}
	if credential.Value == "" || credential.AssignmentID == "" || credential.WarningRevision == "" {
		r.credentials.InvalidateCredential()
		return ErrAuthorization
	}
	if err := r.runtime.Start(ctx); err != nil {
		return err
	}
	r.started = true
	return nil
}

func (r *AuthorizedRuntime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.credentials.InvalidateCredential()
	if !r.started {
		return nil
	}
	r.started = false
	return r.runtime.Shutdown(ctx)
}
