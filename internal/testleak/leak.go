// Package testleak provides bounded process-resource checks for integration tests.
package testleak

import (
	"fmt"
	"os"
	"runtime"
	"time"
)

type Snapshot struct {
	Goroutines  int
	Descriptors int
}

func Take() (Snapshot, error) {
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Goroutines: runtime.NumGoroutine(), Descriptors: len(entries)}, nil
}

func WaitForBaseline(baseline Snapshot, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		runtime.GC()
		runtime.Gosched()
		current, err := Take()
		if err != nil {
			return err
		}
		if current.Goroutines <= baseline.Goroutines && current.Descriptors <= baseline.Descriptors {
			return nil
		}
		if time.Now().After(deadline) {
			buffer := make([]byte, 1<<20)
			n := runtime.Stack(buffer, true)
			return fmt.Errorf("resource leak: baseline=%+v current=%+v\n%s", baseline, current, buffer[:n])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
