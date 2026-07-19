package connector

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

var ErrSupervisorInvalid = errors.New("invalid connector supervisor configuration")

type AdmissionSource interface {
	Admission(context.Context) (Admission, error)
}

type RetryWaiter interface {
	Wait(context.Context, time.Duration, <-chan struct{}) error
}

type timerWaiter struct{}

func (timerWaiter) Wait(ctx context.Context, delay time.Duration, wake <-chan struct{}) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	case <-timer.C:
		return nil
	}
}

type SupervisorConfig struct {
	Manager        *Manager
	Admissions     AdmissionSource
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Jitter         float64
	RandomFloat    func() float64
	Waiter         RetryWaiter
	Metrics        interface {
		Record(string, float64, map[string]string) error
	}
}

type Supervisor struct {
	mu      sync.Mutex
	config  SupervisorConfig
	cancel  context.CancelFunc
	done    chan struct{}
	wake    chan struct{}
	running bool
}

func NewSupervisor(config SupervisorConfig) (*Supervisor, error) {
	if config.InitialBackoff == 0 {
		config.InitialBackoff = time.Second
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = time.Minute
	}
	if config.Jitter == 0 {
		config.Jitter = 0.2
	}
	if config.Waiter == nil {
		config.Waiter = timerWaiter{}
	}
	if config.RandomFloat == nil {
		config.RandomFloat = rand.Float64
	}
	if config.Manager == nil || config.Admissions == nil || config.InitialBackoff <= 0 || config.MaxBackoff < config.InitialBackoff || config.Jitter < 0 || config.Jitter > 1 {
		return nil, ErrSupervisorInvalid
	}
	return &Supervisor{config: config, wake: make(chan struct{}, 1)}, nil
}

func (s *Supervisor) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return ErrSupervisorInvalid
	}
	runCtx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done, s.running = cancel, make(chan struct{}), true
	go s.loop(runCtx)
	return nil
}

func (s *Supervisor) NetworkChanged() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Supervisor) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	cancel, done := s.cancel, s.done
	s.mu.Unlock()
	cancel()
	select {
	case <-done:
		return s.config.Manager.Shutdown(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Supervisor) loop(ctx context.Context) {
	defer func() { s.mu.Lock(); s.running = false; close(s.done); s.mu.Unlock() }()
	backoff := s.config.InitialBackoff
	for ctx.Err() == nil {
		admission, err := s.config.Admissions.Admission(ctx)
		if err == nil {
			result, acceptErr := s.config.Manager.Accept(ctx, admission)
			if acceptErr == nil {
				metricResult := "connected"
				if result.Replaced {
					metricResult = "replaced"
				}
				s.recordRetry(string(result.Transport), metricResult)
				backoff = s.config.InitialBackoff
				waitErr := s.config.Manager.WaitDisconnected(ctx, result.Generation)
				if waitErr != nil && ctx.Err() == nil {
					if s.config.Waiter.Wait(ctx, s.jitter(backoff), s.wake) != nil {
						return
					}
				}
				continue
			}
		}
		s.recordRetry("none", "failed")
		if s.config.Waiter.Wait(ctx, s.jitter(backoff), s.wake) != nil {
			return
		}
		backoff *= 2
		if backoff > s.config.MaxBackoff {
			backoff = s.config.MaxBackoff
		}
	}
}

func (s *Supervisor) recordRetry(transport, result string) {
	if s.config.Metrics != nil {
		_ = s.config.Metrics.Record("paperboat_helper_connector_retries_total", 1, map[string]string{"transport": transport, "result": result})
	}
}

func (s *Supervisor) jitter(delay time.Duration) time.Duration {
	if s.config.Jitter == 0 {
		return delay
	}
	// math/rand is used only for scheduling. Admission and credential entropy are
	// supplied by their security-owning components.
	variation := (s.config.RandomFloat()*2 - 1) * s.config.Jitter
	value := time.Duration(float64(delay) * (1 + variation))
	if value < 0 {
		return 0
	}
	return value
}
