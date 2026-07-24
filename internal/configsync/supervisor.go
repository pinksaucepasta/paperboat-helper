package configsync

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrSupervisorInvalid = errors.New("invalid config sync supervisor")

type SupervisorConfig struct {
	Credentials CredentialSource
	Factory     RuntimeFactory
	Retry       time.Duration
}

// Supervisor keeps config sync disabled while authorization is unavailable.
// It is deliberately safe to run as a required BYOD helper component: lack of
// assignment or current consent does not prevent unrelated helper operation,
// and no runtime (therefore no managed filesystem access) exists before a
// fresh credential is returned.
type Supervisor struct {
	credentials CredentialSource
	factory     RuntimeFactory
	retry       time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan error
	active Runtime
}

func NewSupervisor(config SupervisorConfig) (*Supervisor, error) {
	if config.Credentials == nil || config.Factory == nil {
		return nil, ErrSupervisorInvalid
	}
	if config.Retry == 0 {
		config.Retry = 30 * time.Second
	}
	if config.Retry < time.Second || config.Retry > 5*time.Minute {
		return nil, ErrSupervisorInvalid
	}
	return &Supervisor{credentials: config.Credentials, factory: config.Factory, retry: config.Retry}, nil
}

func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	done := make(chan error, 1)
	s.done = done
	go func() {
		done <- s.run(runCtx)
		close(done)
	}()
	return nil
}

func (s *Supervisor) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.cancel, s.done, s.active = nil, nil, nil
	s.mu.Unlock()
	s.credentials.InvalidateCredential()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Supervisor) Apply(ctx context.Context) error {
	s.mu.Lock()
	runtime := s.active
	s.mu.Unlock()
	trigger, ok := runtime.(interface{ Apply(context.Context) error })
	if !ok {
		return ErrAuthorization
	}
	return trigger.Apply(ctx)
}

func (s *Supervisor) run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}

		credential, err := s.credentials.Credential(ctx)
		if err != nil || !validActivationCredential(credential) {
			s.credentials.InvalidateCredential()
			if !resetSupervisorTimer(ctx, timer, s.retry) {
				return nil
			}
			continue
		}
		runtime, err := s.factory(ctx, credential)
		if err != nil || runtime == nil {
			s.credentials.InvalidateCredential()
			if !resetSupervisorTimer(ctx, timer, s.retry) {
				return nil
			}
			continue
		}
		if err := runtime.Start(ctx); err != nil {
			_ = runtime.Shutdown(context.WithoutCancel(ctx))
			s.credentials.InvalidateCredential()
			if !resetSupervisorTimer(ctx, timer, s.retry) {
				return nil
			}
			continue
		}
		s.mu.Lock()
		if s.cancel != nil {
			s.active = runtime
		}
		s.mu.Unlock()
		revalidate := time.NewTicker(s.retry)
		authorized := true
		for authorized {
			select {
			case <-ctx.Done():
				authorized = false
			case <-revalidate.C:
				revalidateCredential(s.credentials)
				current, credentialErr := s.credentials.Credential(ctx)
				authorized = credentialErr == nil && sameCredentialBinding(credential, current)
			}
		}
		revalidate.Stop()
		s.mu.Lock()
		if s.active == runtime {
			s.active = nil
		}
		s.mu.Unlock()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		shutdownErr := runtime.Shutdown(shutdownCtx)
		shutdownCancel()
		if ctx.Err() != nil {
			return shutdownErr
		}
		s.credentials.InvalidateCredential()
		if !resetSupervisorTimer(ctx, timer, s.retry) {
			return nil
		}
	}
}

func revalidateCredential(credentials CredentialSource) {
	if revalidator, ok := credentials.(interface{ RevalidateCredential() }); ok {
		revalidator.RevalidateCredential()
		return
	}
	credentials.InvalidateCredential()
}

func validActivationCredential(credential Credential) bool {
	return credential.Value != "" && credential.EnvironmentID != "" && credential.HelperID != "" &&
		credential.AssignmentID != "" && credential.WarningRevision != "" && !credential.ExpiresAt.IsZero()
}

func sameCredentialBinding(expected, current Credential) bool {
	return validActivationCredential(current) &&
		expected.EnvironmentID == current.EnvironmentID &&
		expected.HelperID == current.HelperID &&
		expected.AssignmentID == current.AssignmentID &&
		expected.WarningRevision == current.WarningRevision
}

func resetSupervisorTimer(ctx context.Context, timer *time.Timer, duration time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	timer.Reset(duration)
	return true
}
