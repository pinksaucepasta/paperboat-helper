package preview

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrReporterInvalid = errors.New("invalid preview reporter configuration")

type Observation struct {
	Identity      string    `json:"identity"`
	EnvironmentID string    `json:"environment_id"`
	LogicalName   string    `json:"logical_name"`
	Target        Target    `json:"target"`
	State         State     `json:"state"`
	Reason        string    `json:"reason,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
	Revision      uint64    `json:"revision"`
}

type ObservationSender interface {
	Send(context.Context, Observation) error
}

type ReporterConfig struct {
	Registry *Registry
	Sender   ObservationSender
	Interval time.Duration
	Timeout  time.Duration
	Metrics  interface {
		Record(string, float64, map[string]string) error
	}
}

type Reporter struct {
	mu           sync.Mutex
	deliveryMu   sync.Mutex
	config       ReporterConfig
	acknowledged map[string]uint64
	active       *Observation
	cancel       context.CancelFunc
	done         chan struct{}
	running      bool
}

func NewReporter(config ReporterConfig) (*Reporter, error) {
	if config.Interval == 0 {
		config.Interval = 5 * time.Second
	}
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}
	if config.Registry == nil || config.Sender == nil || config.Interval <= 0 || config.Timeout <= 0 {
		return nil, ErrReporterInvalid
	}
	return &Reporter{config: config, acknowledged: make(map[string]uint64)}, nil
}

func (r *Reporter) DeliverOnce(ctx context.Context) (bool, error) {
	r.deliveryMu.Lock()
	defer r.deliveryMu.Unlock()
	r.mu.Lock()
	if r.active == nil {
		for _, record := range r.config.Registry.List() {
			if record.Revision > r.acknowledged[record.Identity] {
				observation := observationFrom(record)
				r.active = &observation
				break
			}
		}
	}
	if r.active == nil {
		r.mu.Unlock()
		return false, nil
	}
	observation := *r.active
	r.mu.Unlock()
	sendCtx, cancel := context.WithTimeout(ctx, r.config.Timeout)
	err := r.config.Sender.Send(sendCtx, observation)
	cancel()
	if err != nil {
		r.recordDelivery("failed")
		return true, err
	}
	r.mu.Lock()
	if r.acknowledged[observation.Identity] < observation.Revision {
		r.acknowledged[observation.Identity] = observation.Revision
	}
	if r.active != nil && r.active.Identity == observation.Identity && r.active.Revision == observation.Revision {
		r.active = nil
	}
	r.mu.Unlock()
	r.recordDelivery("delivered")
	return true, nil
}

func (r *Reporter) recordDelivery(result string) {
	if r.config.Metrics != nil {
		_ = r.config.Metrics.Record("paperboat_helper_delivery_total", 1, map[string]string{"kind": "preview", "result": result})
	}
}

func (r *Reporter) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		return ErrReporterInvalid
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel, r.done, r.running = cancel, make(chan struct{}), true
	go r.loop(runCtx)
	return nil
}

func (r *Reporter) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Reporter) loop(ctx context.Context) {
	defer func() { r.mu.Lock(); r.running = false; close(r.done); r.mu.Unlock() }()
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()
	for {
		_, _ = r.DeliverOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func observationFrom(record Record) Observation {
	return Observation{Identity: record.Identity, EnvironmentID: record.EnvironmentID, LogicalName: record.LogicalName, Target: record.Target, State: record.State, Reason: record.Reason, UpdatedAt: record.UpdatedAt, Revision: record.Revision}
}
