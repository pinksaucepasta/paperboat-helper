package activity

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }
func event(sequence uint64, occurred time.Time) Event {
	return Event{EnvironmentID: "env", SourceID: "att", Source: TerminalInput, Sequence: sequence, OccurredAt: occurred}
}

func TestTrustedFreshActivityExtendsIdle(t *testing.T) {
	clock := &fixedClock{time.Now().UTC()}
	c, _ := New(Config{Clock: clock})
	result, err := c.Record(event(1, clock.now.Add(-time.Minute)), true)
	if err != nil || !result.ExtendsIdle || !c.LastActivity().Equal(clock.now.Add(-time.Minute)) {
		t.Fatalf("result=%#v last=%v err=%v", result, c.LastActivity(), err)
	}
}

func TestStaleDuplicateAndForgedEventsDoNotExtendIdle(t *testing.T) {
	clock := &fixedClock{time.Now().UTC()}
	c, _ := New(Config{Clock: clock})
	stale := event(1, clock.now.Add(-6*time.Minute))
	result, err := c.Record(stale, true)
	if err != nil || result.ExtendsIdle {
		t.Fatalf("stale=%#v err=%v", result, err)
	}
	if _, err := c.Record(stale, true); !errors.Is(err, ErrInvalidSequence) {
		t.Fatalf("duplicate err=%v", err)
	}
	forged := event(2, clock.now)
	if _, err := c.Record(forged, false); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("forged err=%v", err)
	}
	if !c.LastActivity().IsZero() {
		t.Fatalf("last=%v", c.LastActivity())
	}
	diagnostics := c.Diagnostics()
	if len(diagnostics) != 3 {
		t.Fatalf("diagnostics=%v", diagnostics)
	}
}

func TestInvalidOpenPTYSourceRejected(t *testing.T) {
	clock := &fixedClock{time.Now()}
	c, _ := New(Config{Clock: clock})
	invalid := event(1, clock.now)
	invalid.Source = "pty_open"
	if _, err := c.Record(invalid, true); !errors.Is(err, ErrInvalidSource) {
		t.Fatalf("err=%v", err)
	}
}

func TestBatchIsBoundedRetryStableAndAcknowledged(t *testing.T) {
	clock := &fixedClock{time.Now().UTC()}
	c, _ := New(Config{Clock: clock, MaxQueued: 200})
	for i := 1; i <= 101; i++ {
		e := event(uint64(i), clock.now)
		if _, err := c.Record(e, true); err != nil {
			t.Fatal(err)
		}
	}
	first, ok, err := c.PeekBatch()
	if err != nil || !ok || len(first.Events) != 100 || len(first.Body) > MaxBatchBytes {
		t.Fatalf("events=%d bytes=%d ok=%v err=%v", len(first.Events), len(first.Body), ok, err)
	}
	retry, _, _ := c.PeekBatch()
	if retry.ID != first.ID || !bytes.Equal(retry.Body, first.Body) {
		t.Fatal("retry batch changed")
	}
	retry.Body[0] = 'x'
	again, _, _ := c.PeekBatch()
	if bytes.Equal(retry.Body, again.Body) {
		t.Fatal("batch body aliases internal state")
	}
	if err := c.Acknowledge(first.ID); err != nil {
		t.Fatal(err)
	}
	next, ok, err := c.PeekBatch()
	if err != nil || !ok || len(next.Events) != 1 {
		t.Fatalf("next=%#v ok=%v err=%v", next, ok, err)
	}
}

func TestConcurrentSourceSequenceAcceptsOnlyNewerArrivalOrder(t *testing.T) {
	clock := &fixedClock{time.Now()}
	c, _ := New(Config{Clock: clock, MaxQueued: 100})
	var wg sync.WaitGroup
	for i := 1; i <= 50; i++ {
		wg.Add(1)
		go func(seq uint64) { defer wg.Done(); _, _ = c.Record(event(seq, clock.now), true) }(uint64(i))
	}
	wg.Wait()
	batch, ok, err := c.PeekBatch()
	if err != nil || !ok || len(batch.Events) < 1 {
		t.Fatalf("batch=%#v ok=%v err=%v", batch, ok, err)
	}
	for i := 1; i < len(batch.Events); i++ {
		if batch.Events[i].Sequence <= batch.Events[i-1].Sequence {
			t.Fatalf("sequences not increasing: %v", batch.Events)
		}
	}
}
