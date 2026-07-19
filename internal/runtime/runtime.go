package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/health"
)

var (
	ErrInvalidConfiguration = errors.New("invalid runtime configuration")
	ErrInvalidState         = errors.New("invalid runtime state")
)

type Service interface {
	Start(context.Context) error
	Shutdown(context.Context) error
}
type Component struct {
	Capability string
	Required   bool
	Service    Service
}
type Config struct {
	Version         string
	Components      []Component
	ShutdownTimeout time.Duration
	Clock           health.Clock
}
type State string

const (
	New      State = "new"
	Starting State = "starting"
	Running  State = "running"
	Stopping State = "stopping"
	Stopped  State = "stopped"
	Failed   State = "failed"
)

type Runtime struct {
	opMu    sync.Mutex
	mu      sync.RWMutex
	config  Config
	state   State
	started []Component
	health  *health.Registry
}

func NewRuntime(config Config) (*Runtime, error) {
	if config.Version == "" || len(config.Components) == 0 {
		return nil, ErrInvalidConfiguration
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = 30 * time.Second
	}
	if config.ShutdownTimeout <= 0 {
		return nil, ErrInvalidConfiguration
	}
	seen := make(map[string]bool)
	capabilities := make([]string, 0, len(config.Components))
	for _, component := range config.Components {
		if component.Capability == "" || component.Service == nil || seen[component.Capability] {
			return nil, ErrInvalidConfiguration
		}
		seen[component.Capability] = true
		capabilities = append(capabilities, component.Capability)
	}
	return &Runtime{config: config, state: New, health: health.New(config.Version, capabilities, config.Clock)}, nil
}

func (r *Runtime) Start(ctx context.Context) error {
	r.opMu.Lock()
	defer r.opMu.Unlock()
	r.mu.Lock()
	if r.state != New {
		r.mu.Unlock()
		return ErrInvalidState
	}
	r.state = Starting
	r.mu.Unlock()
	for _, component := range r.config.Components {
		if err := component.Service.Start(ctx); err != nil {
			cleanupErr := r.cleanupFailed(component)
			if !component.Required {
				r.health.Set(component.Capability, health.Unavailable, "start_failed", 0)
				continue
			}
			r.health.Set(component.Capability, health.Unavailable, "start_failed", 0)
			rollbackErr := r.rollback()
			r.mu.Lock()
			r.state = Failed
			r.mu.Unlock()
			r.health.SetLive(false)
			return errors.Join(fmt.Errorf("start %s: %w", component.Capability, err), cleanupErr, rollbackErr)
		}
		r.started = append(r.started, component)
		r.health.Set(component.Capability, health.Ready, "", 0)
	}
	r.mu.Lock()
	r.state = Running
	r.mu.Unlock()
	return nil
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	r.health.SetLive(false)
	r.opMu.Lock()
	defer r.opMu.Unlock()
	r.mu.Lock()
	if r.state == Stopped {
		r.mu.Unlock()
		return nil
	}
	if r.state != Running && r.state != Failed {
		r.mu.Unlock()
		return ErrInvalidState
	}
	r.state = Stopping
	r.mu.Unlock()
	shutdownCtx, cancel := context.WithTimeout(ctx, r.config.ShutdownTimeout)
	defer cancel()
	var result error
	for i := len(r.started) - 1; i >= 0; i-- {
		component := r.started[i]
		if err := component.Service.Shutdown(shutdownCtx); err != nil {
			result = errors.Join(result, fmt.Errorf("shutdown %s: %w", component.Capability, err))
			r.health.Set(component.Capability, health.Degraded, "shutdown_failed", 0)
		} else {
			r.health.Set(component.Capability, health.Unavailable, "stopped", 0)
		}
	}
	r.started = nil
	r.mu.Lock()
	r.state = Stopped
	r.mu.Unlock()
	return result
}

func (r *Runtime) State() State            { r.mu.RLock(); defer r.mu.RUnlock(); return r.state }
func (r *Runtime) Health() health.Snapshot { return r.health.Snapshot() }

func (r *Runtime) rollback() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.config.ShutdownTimeout)
	defer cancel()
	var result error
	for i := len(r.started) - 1; i >= 0; i-- {
		component := r.started[i]
		if err := component.Service.Shutdown(ctx); err != nil {
			result = errors.Join(result, fmt.Errorf("rollback %s: %w", component.Capability, err))
			r.health.Set(component.Capability, health.Degraded, "rollback_failed", 0)
		} else {
			r.health.Set(component.Capability, health.Unavailable, "stopped", 0)
		}
	}
	r.started = nil
	return result
}

func (r *Runtime) cleanupFailed(component Component) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.config.ShutdownTimeout)
	defer cancel()
	if err := component.Service.Shutdown(ctx); err != nil {
		return fmt.Errorf("cleanup failed start %s: %w", component.Capability, err)
	}
	return nil
}
