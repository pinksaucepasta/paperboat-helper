package activity

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type heartbeatSenderStub struct{ calls atomic.Int32 }

func (*heartbeatSenderStub) Send(context.Context, Batch) error { return nil }
func (s *heartbeatSenderStub) Heartbeat(context.Context) error {
	s.calls.Add(1)
	return nil
}

func TestDeliverySendsIdleAndFinalHeartbeats(t *testing.T) {
	collector, err := New(Config{MaxQueued: 4})
	if err != nil {
		t.Fatal(err)
	}
	sender := &heartbeatSenderStub{}
	delivery, err := NewDelivery(DeliveryConfig{Collector: collector, Sender: sender, Interval: 5 * time.Millisecond, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for sender.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	beforeShutdown := sender.calls.Load()
	if beforeShutdown == 0 {
		t.Fatal("idle heartbeat was not sent")
	}
	if err := delivery.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sender.calls.Load() <= beforeShutdown {
		t.Fatal("final heartbeat was not sent")
	}
}
