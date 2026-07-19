package session

import (
	"errors"
	"io"
	"sync"
	"testing"
)

type countingWriter struct {
	mu    sync.Mutex
	calls int
	n     int
	err   error
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.n < 0 {
		return len(p), w.err
	}
	return w.n, w.err
}

func TestInputDuplicateWritesExactlyOnce(t *testing.T) {
	j := NewInputJournal(1)
	w := &countingWriter{n: -1}
	key := InputKey{ClientID: "cli", AttachmentID: "att", Generation: 1, InputID: "inp"}
	first, err := j.Write(key, []byte("ls\n"), w)
	if err != nil || first.Status != InputAccepted {
		t.Fatalf("decision=%#v err=%v", first, err)
	}
	second, err := j.Write(key, []byte("ls\n"), w)
	if err != nil || second.Status != InputDuplicate || w.calls != 1 {
		t.Fatalf("decision=%#v calls=%d err=%v", second, w.calls, err)
	}
	if _, err := j.Write(key, []byte("pwd\n"), w); !errors.Is(err, ErrInputConflict) {
		t.Fatalf("conflict err=%v", err)
	}
}

func TestInputGenerationAndUncertainWrite(t *testing.T) {
	j := NewInputJournal(2)
	stale := InputKey{ClientID: "cli", AttachmentID: "att", Generation: 1, InputID: "old"}
	if _, err := j.Write(stale, []byte("x"), &countingWriter{n: -1}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale err=%v", err)
	}
	key := InputKey{ClientID: "cli", AttachmentID: "att", Generation: 2, InputID: "new"}
	decision, err := j.Write(key, []byte("abc"), &countingWriter{n: 1, err: io.ErrUnexpectedEOF})
	if err != nil || decision.Status != InputUncertain || decision.WriteError != "pty_write_failed" {
		t.Fatalf("decision=%#v err=%v", decision, err)
	}
	queried, err := j.Query(key)
	if err != nil || queried.Status != InputUncertain {
		t.Fatalf("query=%#v err=%v", queried, err)
	}
}

func TestConcurrentDuplicateInputWritesOnce(t *testing.T) {
	j := NewInputJournal(1)
	w := &countingWriter{n: -1}
	key := InputKey{ClientID: "cli", AttachmentID: "att", Generation: 1, InputID: "same"}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := j.Write(key, []byte("x"), w); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if w.calls != 1 {
		t.Fatalf("calls=%d", w.calls)
	}
}

func TestBoundedInputJournalRejectsNewIdentityButPreservesDuplicate(t *testing.T) {
	journal := NewBoundedInputJournal(1, 1)
	writer := &countingWriter{n: -1}
	first := InputKey{ClientID: "cli", AttachmentID: "att", Generation: 1, InputID: "input_1"}
	if decision, err := journal.Write(first, []byte("a"), writer); err != nil || decision.Status != InputAccepted {
		t.Fatalf("first=%#v err=%v", decision, err)
	}
	second := InputKey{ClientID: "cli", AttachmentID: "att", Generation: 1, InputID: "input_2"}
	if _, err := journal.Write(second, []byte("b"), writer); !errors.Is(err, ErrInputJournalFull) {
		t.Fatalf("full err=%v", err)
	}
	if decision, err := journal.Write(first, []byte("a"), writer); err != nil || decision.Status != InputDuplicate {
		t.Fatalf("duplicate=%#v err=%v", decision, err)
	}
	if writer.calls != 1 {
		t.Fatalf("writer calls=%d", writer.calls)
	}
}
