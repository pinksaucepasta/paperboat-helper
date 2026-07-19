package activity

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidDelivery = errors.New("invalid activity delivery configuration")
	ErrDeliveryState   = errors.New("invalid activity delivery state")
)

type Sender interface {
	Send(context.Context, Batch) error
}

type DeliveryConfig struct {
	Collector *Collector
	Sender    Sender
	Interval  time.Duration
	Timeout   time.Duration
	Now       func() time.Time
	OnResult  func(error)
	Metrics   interface {
		Record(string, float64, map[string]string) error
	}
}

type DeliveryStatus struct {
	Running             bool      `json:"running"`
	ConsecutiveFailures uint64    `json:"consecutive_failures"`
	LastAttempt         time.Time `json:"last_attempt,omitempty"`
	LastSuccess         time.Time `json:"last_success,omitempty"`
	LastResult          string    `json:"last_result,omitempty"`
}

type Delivery struct {
	mu      sync.Mutex
	config  DeliveryConfig
	cancel  context.CancelFunc
	done    chan struct{}
	runErr  error
	running bool
	status  DeliveryStatus
}

func NewDelivery(config DeliveryConfig) (*Delivery, error) {
	if config.Interval == 0 {
		config.Interval = 5 * time.Second
	}
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	if config.Collector == nil || config.Sender == nil || config.Interval <= 0 || config.Timeout <= 0 {
		return nil, ErrInvalidDelivery
	}
	return &Delivery{config: config}, nil
}

func (d *Delivery) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.running {
		return ErrDeliveryState
	}
	runCtx, cancel := context.WithCancel(context.Background())
	d.cancel, d.done, d.running, d.runErr = cancel, make(chan struct{}), true, nil
	d.status.Running = true
	go d.loop(runCtx)
	return nil
}

func (d *Delivery) Shutdown(ctx context.Context) error {
	d.mu.Lock()
	if !d.running {
		d.mu.Unlock()
		return nil
	}
	cancel, done := d.cancel, d.done
	d.mu.Unlock()
	cancel()
	select {
	case <-done:
		d.mu.Lock()
		err := d.runErr
		d.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Delivery) Status() DeliveryStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	status := d.status
	return status
}

// DeliverOnce sends at most one frozen batch. Failed or uncertain delivery is
// deliberately not acknowledged, so the next call sends identical bytes.
func (d *Delivery) DeliverOnce(ctx context.Context) (bool, error) {
	batch, available, err := d.config.Collector.PeekBatch()
	if err != nil || !available {
		return false, err
	}
	sendCtx, cancel := context.WithTimeout(ctx, d.config.Timeout)
	err = d.config.Sender.Send(sendCtx, batch)
	cancel()
	if err != nil {
		d.recordResult(err)
		return true, err
	}
	if err := d.config.Collector.Acknowledge(batch.ID); err != nil {
		d.recordResult(err)
		return true, err
	}
	d.recordResult(nil)
	return true, nil
}

func (d *Delivery) recordResult(err error) {
	now := d.config.Now()
	d.mu.Lock()
	d.status.LastAttempt = now
	if err == nil {
		d.status.LastSuccess = now
		d.status.LastResult = "delivered"
		d.status.ConsecutiveFailures = 0
	} else {
		d.status.LastResult = "delivery_failed"
		d.status.ConsecutiveFailures++
	}
	d.mu.Unlock()
	if d.config.OnResult != nil {
		d.config.OnResult(err)
	}
	if d.config.Metrics != nil {
		result := "delivered"
		if err != nil {
			result = "failed"
		}
		_ = d.config.Metrics.Record("paperboat_helper_delivery_total", 1, map[string]string{"kind": "activity", "result": result})
	}
}

func (d *Delivery) loop(ctx context.Context) {
	defer func() {
		d.mu.Lock()
		d.running = false
		d.status.Running = false
		close(d.done)
		d.mu.Unlock()
	}()
	ticker := time.NewTicker(d.config.Interval)
	defer ticker.Stop()
	for {
		if _, err := d.DeliverOnce(ctx); err != nil && ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
