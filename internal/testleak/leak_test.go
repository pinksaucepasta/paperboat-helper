package testleak

import (
	"testing"
	"time"
)

func TestSnapshotReturnsToBaseline(t *testing.T) {
	baseline, err := Take()
	if err != nil {
		t.Skipf("descriptor accounting unavailable: %v", err)
	}
	done := make(chan struct{})
	go func() { close(done) }()
	<-done
	if err := WaitForBaseline(baseline, time.Second); err != nil {
		t.Fatal(err)
	}
}
