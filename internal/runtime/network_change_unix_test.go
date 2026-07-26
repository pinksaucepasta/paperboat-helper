//go:build darwin || linux

package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestNetworkChangeServiceWakesOnlyAfterSnapshotChanges(t *testing.T) {
	var value atomic.Int32
	var wakes atomic.Int32
	service, err := newNetworkChangeService(5*time.Millisecond, func() { wakes.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	service.snapshot = func() (string, error) { return string(rune('a' + value.Load())), nil }
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	if wakes.Load() != 0 {
		t.Fatalf("unchanged snapshot wakes=%d", wakes.Load())
	}
	value.Store(1)
	deadline := time.Now().Add(time.Second)
	for wakes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if wakes.Load() != 1 {
		t.Fatalf("changed snapshot wakes=%d", wakes.Load())
	}
	if err := service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
