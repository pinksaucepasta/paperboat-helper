package operation

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

func TestCanonicalHashIgnoresWhitespaceAndObjectOrder(t *testing.T) {
	a, err := CanonicalHash([]byte(`{"b":[2,3],"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalHash([]byte(" { \"a\" : 1, \"b\" : [2,3] } \n"))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("canonical hashes differ")
	}
	for _, invalid := range []string{`{"a":1,"a":2}`, `{"a":1} {"b":2}`} {
		if _, err := CanonicalHash([]byte(invalid)); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("request=%s err=%v", invalid, err)
		}
	}
}

func TestConcurrentSameOperationRunsOnce(t *testing.T) {
	j, _ := NewJournal(8)
	ctx := context.Background()
	start := make(chan struct{})
	started := make(chan struct{})
	var calls atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, replay, err := j.Execute(ctx, "op_00000001", []byte(`{"a":1}`), func(context.Context) Outcome {
				calls.Add(1)
				close(started)
				<-start
				return Outcome{Result: []byte(`{"ok":true}`)}
			})
			if err != nil || string(out.Result) != `{"ok":true}` {
				t.Errorf("out=%s replay=%v err=%v", out.Result, replay, err)
			}
		}()
	}
	<-started
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestOperationConflictAndCanceledWait(t *testing.T) {
	j, _ := NewJournal(2)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = j.Execute(context.Background(), "op_00000001", []byte(`{"a":1}`), func(context.Context) Outcome { close(started); <-release; return Outcome{} })
	}()
	<-started
	if _, _, err := j.Execute(context.Background(), "op_00000001", []byte(`{"a":2}`), func(context.Context) Outcome { return Outcome{} }); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := j.Execute(ctx, "op_00000001", []byte(`{"a":1}`), func(context.Context) Outcome { return Outcome{} }); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait err=%v", err)
	}
	close(release)
	<-done
}

func TestJournalEvictsOnlyCompletedEntries(t *testing.T) {
	j, _ := NewJournal(1)
	j.Execute(context.Background(), "op_00000001", []byte(`{}`), func(context.Context) Outcome { return Outcome{Result: []byte(`1`)} })
	if _, replay, err := j.Execute(context.Background(), "op_00000002", []byte(`{}`), func(context.Context) Outcome { return Outcome{Result: []byte(`2`)} }); err != nil || replay {
		t.Fatalf("replay=%v err=%v", replay, err)
	}
}

func TestPersistentJournalReplaysCompletedResultAfterRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	durable, err := store.Open(context.Background(), store.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	journal, err := NewPersistentJournal(context.Background(), 8, durable, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	outcome, replay, err := journal.Execute(context.Background(), "op_00000001", []byte(`{"a":1}`), func(context.Context) Outcome { calls.Add(1); return Outcome{Result: []byte(`{"ok":true}`)} })
	if err != nil || replay || string(outcome.Result) != `{"ok":true}` {
		t.Fatalf("outcome=%s replay=%v err=%v", outcome.Result, replay, err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	durable, err = store.Open(context.Background(), store.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	journal, err = NewPersistentJournal(context.Background(), 8, durable, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	outcome, replay, err = journal.Execute(context.Background(), "op_00000001", []byte(`{"a":1}`), func(context.Context) Outcome { calls.Add(1); return Outcome{} })
	if err != nil || !replay || calls.Load() != 1 || string(outcome.Result) != `{"ok":true}` {
		t.Fatalf("outcome=%s replay=%v calls=%d err=%v", outcome.Result, replay, calls.Load(), err)
	}
}

func TestPersistentJournalRecoversPendingAsUncertainWithoutRerun(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	durable, err := store.Open(context.Background(), store.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	request := []byte(`{"a":1}`)
	hash, err := CanonicalHash(request)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, inserted, err := durable.ReserveOperation(context.Background(), "op_00000001", hash[:], now.Add(time.Hour)); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	journal, err := NewPersistentJournal(context.Background(), 8, durable, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	_, replay, err := journal.Execute(context.Background(), "op_00000001", request, func(context.Context) Outcome { calls.Add(1); return Outcome{} })
	if !errors.Is(err, ErrOperationUncertain) || !replay || calls.Load() != 0 {
		t.Fatalf("replay=%v calls=%d err=%v", replay, calls.Load(), err)
	}
	durable.Close()
}

func TestCompletionPersistenceFailureRemainsPending(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	durable, err := store.Open(context.Background(), store.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	journal, err := NewPersistentJournal(context.Background(), 8, durable, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = journal.Execute(context.Background(), "op_00000001", []byte(`{}`), func(context.Context) Outcome {
		if closeErr := durable.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		return Outcome{Result: []byte(`1`)}
	})
	if !errors.Is(err, ErrOperationUncertain) {
		t.Fatalf("err=%v", err)
	}
	durable, err = store.Open(context.Background(), store.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	journal, err = NewPersistentJournal(context.Background(), 8, durable, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	_, _, err = journal.Execute(context.Background(), "op_00000001", []byte(`{}`), func(context.Context) Outcome { calls.Add(1); return Outcome{} })
	if !errors.Is(err, ErrOperationUncertain) || calls.Load() != 0 {
		t.Fatalf("calls=%d err=%v", calls.Load(), err)
	}
}

func TestPersistentJournalNeverStoresOversizedOutcome(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	durable, err := store.Open(context.Background(), store.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	now := time.Now().UTC()
	journal, err := NewPersistentJournal(context.Background(), 8, durable, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = journal.Execute(context.Background(), "op_large_0001", []byte(`{}`), func(context.Context) Outcome {
		return Outcome{Result: make([]byte, store.MaxOperationResultBytes+1)}
	})
	if !errors.Is(err, ErrOperationUncertain) || !errors.Is(err, store.ErrResultTooLarge) {
		t.Fatalf("err=%v", err)
	}
	records, err := durable.Operations(context.Background(), now, 8)
	if err != nil || len(records) != 1 || records[0].State != "pending" || len(records[0].Result) != 0 {
		t.Fatalf("records=%#v err=%v", records, err)
	}
}

func TestPersistentJournalDeletesExpiredRowsDuringNormalOperation(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	durable, err := store.Open(context.Background(), store.Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	journal, err := NewPersistentJournal(context.Background(), 8, durable, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	oldRequest := []byte(`{"old":true}`)
	oldHash, err := CanonicalHash(oldRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.PutOperation(context.Background(), store.OperationResult{OperationID: "op_expired_0001", RequestHash: oldHash[:], Result: []byte(`{"old":true}`), CompletedAt: now, ExpiresAt: now.Add(30 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	var calls atomic.Int32
	outcome, replay, err := journal.Execute(context.Background(), "op_expired_0001", []byte(`{"new":true}`), func(context.Context) Outcome {
		calls.Add(1)
		return Outcome{Result: []byte(`{"new":true}`)}
	})
	if err != nil || replay || calls.Load() != 1 || string(outcome.Result) != `{"new":true}` {
		t.Fatalf("outcome=%s replay=%v calls=%d err=%v", outcome.Result, replay, calls.Load(), err)
	}
}
