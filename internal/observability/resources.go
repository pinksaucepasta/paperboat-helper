package observability

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrInvalidResourceSampler = errors.New("invalid resource sampler")

type ResourceSource interface {
	ResourceCounts() map[string]uint64
}

type MetricRecorder interface {
	Record(string, float64, map[string]string) error
}

type ResourceSamplerConfig struct {
	Sources  []ResourceSource
	Metrics  MetricRecorder
	Interval time.Duration
}

type ResourceSampler struct {
	config ResourceSamplerConfig
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func NewResourceSampler(config ResourceSamplerConfig) (*ResourceSampler, error) {
	if config.Metrics == nil || len(config.Sources) == 0 {
		return nil, ErrInvalidResourceSampler
	}
	for _, source := range config.Sources {
		if source == nil {
			return nil, ErrInvalidResourceSampler
		}
	}
	if config.Interval == 0 {
		config.Interval = 10 * time.Second
	}
	if config.Interval <= 0 {
		return nil, ErrInvalidResourceSampler
	}
	return &ResourceSampler{config: config}, nil
}

func (s *ResourceSampler) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	s.sample()
	go s.run(runCtx, s.done)
	return nil
}

func (s *ResourceSampler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.cancel, s.done = nil, nil
	s.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ResourceSampler) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(s.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sample()
		case <-ctx.Done():
			s.sample()
			return
		}
	}
}

func (s *ResourceSampler) sample() {
	counts := map[string]uint64{"sessions": 0, "attachments": 0, "processes": 0, "uploads": 0, "previews": 0, "connectors": 0}
	for _, source := range s.config.Sources {
		for kind, count := range source.ResourceCounts() {
			if _, allowed := counts[kind]; allowed {
				counts[kind] += count
			}
		}
	}
	for kind, count := range counts {
		_ = s.config.Metrics.Record("paperboat_helper_active_resources", float64(count), map[string]string{"kind": kind})
	}
}
