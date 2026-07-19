package activity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type senderFunc func(context.Context, Batch) error

func (f senderFunc) Send(ctx context.Context, batch Batch) error { return f(ctx, batch) }

func deliveryCollector(t *testing.T) *Collector {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	collector, err := New(Config{Clock: &fixedClock{now: now}})
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 2; sequence++ {
		if _, err := collector.Record(Event{EnvironmentID: "env_1", SourceID: "cli_1", Source: CLIActivity, Sequence: sequence, OccurredAt: now}, true); err != nil {
			t.Fatal(err)
		}
	}
	return collector
}

func TestDeliveryRetriesUncertainBatchWithIdenticalIdentityAndBytes(t *testing.T) {
	collector := deliveryCollector(t)
	var mu sync.Mutex
	var attempts []Batch
	failure := errors.New("control plane unavailable")
	delivery, err := NewDelivery(DeliveryConfig{Collector: collector, Timeout: time.Second, Sender: senderFunc(func(_ context.Context, batch Batch) error {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, batch)
		if len(attempts) == 1 {
			return failure
		}
		return nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if available, err := delivery.DeliverOnce(context.Background()); !available || !errors.Is(err, failure) {
		t.Fatalf("available=%v err=%v", available, err)
	}
	if status := delivery.Status(); status.ConsecutiveFailures != 1 || status.LastResult != "delivery_failed" {
		t.Fatalf("failure status=%#v", status)
	}
	if available, err := delivery.DeliverOnce(context.Background()); !available || err != nil {
		t.Fatalf("available=%v err=%v", available, err)
	}
	if status := delivery.Status(); status.ConsecutiveFailures != 0 || status.LastResult != "delivered" || status.LastSuccess.IsZero() {
		t.Fatalf("success status=%#v", status)
	}
	mu.Lock()
	if len(attempts) != 2 || attempts[0].ID != attempts[1].ID || string(attempts[0].Body) != string(attempts[1].Body) {
		t.Fatalf("attempts=%#v", attempts)
	}
	mu.Unlock()
	if available, err := delivery.DeliverOnce(context.Background()); available || err != nil {
		t.Fatalf("post-ack available=%v err=%v", available, err)
	}
}

func TestDeliveryBoundsSenderAndStopsCleanly(t *testing.T) {
	collector := deliveryCollector(t)
	entered := make(chan struct{})
	delivery, err := NewDelivery(DeliveryConfig{Collector: collector, Interval: time.Hour, Timeout: 20 * time.Millisecond, Sender: senderFunc(func(ctx context.Context, _ Batch) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := delivery.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown=%v", err)
	}
	batch, available, err := collector.PeekBatch()
	if err != nil || !available || batch.ID == 0 {
		t.Fatalf("batch=%#v available=%v err=%v", batch, available, err)
	}
}
