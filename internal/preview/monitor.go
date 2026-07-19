package preview

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrMonitorInvalid = errors.New("invalid preview monitor configuration")

type MonitorConfig struct {
	Registry *Registry
	Interval time.Duration
}

type Monitor struct {
	mu      sync.Mutex
	config  MonitorConfig
	cancel  context.CancelFunc
	done    chan struct{}
	running bool
}

func NewMonitor(config MonitorConfig) (*Monitor, error) {
	if config.Interval == 0 {
		config.Interval = 5 * time.Second
	}
	if config.Registry == nil || config.Interval <= 0 {
		return nil, ErrMonitorInvalid
	}
	return &Monitor{config: config}, nil
}

func (m *Monitor) RunOnce(ctx context.Context) error {
	var result error
	for _, record := range m.config.Registry.List() {
		if record.State != Registering && record.State != Ready && record.State != Degraded {
			continue
		}
		if _, err := m.config.Registry.Probe(ctx, record.Identity); err != nil && ctx.Err() != nil {
			return errors.Join(result, ctx.Err())
		} else if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrInvalidTransition) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (m *Monitor) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return ErrMonitorInvalid
	}
	runCtx, cancel := context.WithCancel(context.Background())
	m.cancel, m.done, m.running = cancel, make(chan struct{}), true
	go m.loop(runCtx)
	return nil
}

func (m *Monitor) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return nil
	}
	cancel, done := m.cancel, m.done
	m.mu.Unlock()
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Monitor) loop(ctx context.Context) {
	defer func() { m.mu.Lock(); m.running = false; close(m.done); m.mu.Unlock() }()
	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()
	for {
		_ = m.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
