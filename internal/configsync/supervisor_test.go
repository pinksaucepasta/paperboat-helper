package configsync

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type supervisorCredentials struct {
	mu         sync.Mutex
	credential Credential
	err        error
	calls      chan struct{}
	invalid    int
}

func (s *supervisorCredentials) Credential(context.Context) (Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case s.calls <- struct{}{}:
	default:
	}
	return s.credential, s.err
}

func (s *supervisorCredentials) InvalidateCredential() {
	s.mu.Lock()
	s.invalid++
	s.mu.Unlock()
}

type supervisorRuntime struct {
	started  chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
	stopErr  error
}

func (r *supervisorRuntime) Start(context.Context) error {
	close(r.started)
	return nil
}

func (r *supervisorRuntime) Shutdown(context.Context) error {
	r.stopOnce.Do(func() { close(r.stopped) })
	return r.stopErr
}

func TestSupervisorDoesNotConstructRuntimeBeforeEligibility(t *testing.T) {
	credentials := &supervisorCredentials{err: ErrAuthorization, calls: make(chan struct{}, 4)}
	constructed := make(chan *supervisorRuntime, 1)
	supervisor, err := NewSupervisor(SupervisorConfig{
		Credentials: credentials, Retry: time.Second,
		Factory: func(context.Context, Credential) (Runtime, error) {
			runtime := &supervisorRuntime{started: make(chan struct{}), stopped: make(chan struct{})}
			constructed <- runtime
			return runtime, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := supervisor.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-credentials.calls:
	case <-time.After(time.Second):
		t.Fatal("authorization was not checked")
	}
	select {
	case <-constructed:
		t.Fatal("runtime constructed without eligibility")
	default:
	}

	credentials.mu.Lock()
	credentials.err = nil
	credentials.credential = Credential{
		Value: "credential", EnvironmentID: "environment", HelperID: "helper",
		AssignmentID: "assignment", WarningRevision: "warning", ExpiresAt: time.Now().Add(time.Minute),
	}
	credentials.mu.Unlock()
	select {
	case runtime := <-constructed:
		select {
		case <-runtime.started:
		case <-time.After(time.Second):
			t.Fatal("eligible runtime was not started")
		}
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCancel()
		if err := supervisor.Shutdown(shutdownCtx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-runtime.stopped:
		default:
			t.Fatal("runtime was not stopped")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("eligible runtime was not constructed")
	}
}

func TestSupervisorValidatesConfiguration(t *testing.T) {
	if _, err := NewSupervisor(SupervisorConfig{}); !errors.Is(err, ErrSupervisorInvalid) {
		t.Fatalf("error = %v", err)
	}
}

func TestSupervisorReturnsFinalFlushFailure(t *testing.T) {
	credentials := &supervisorCredentials{
		calls: make(chan struct{}, 4),
		credential: Credential{
			Value: "credential", EnvironmentID: "environment", HelperID: "helper",
			AssignmentID: "assignment", WarningRevision: "warning", ExpiresAt: time.Now().Add(time.Minute),
		},
	}
	flushErr := errors.New("flush failed")
	runtime := &supervisorRuntime{
		started: make(chan struct{}), stopped: make(chan struct{}), stopErr: flushErr,
	}
	supervisor, err := NewSupervisor(SupervisorConfig{
		Credentials: credentials, Retry: time.Second,
		Factory: func(context.Context, Credential) (Runtime, error) { return runtime, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runtime.started:
	case <-time.After(time.Second):
		t.Fatal("runtime did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.Shutdown(ctx); !errors.Is(err, flushErr) {
		t.Fatalf("shutdown error = %v", err)
	}
}
