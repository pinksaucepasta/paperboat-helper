package history

import (
	"errors"
	"sync"
	"testing"
)

func TestAppendOrdersChannelsAndReplaySlicesExactly(t *testing.T) {
	h, err := New(64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Append(1, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Append(2, []byte("defg")); err != nil {
		t.Fatal(err)
	}
	replay, err := h.Replay(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if replay.FromSequence != 2 || replay.ToSequence != 5 || len(replay.Events) != 2 || string(replay.Events[0].Data) != "c" || string(replay.Events[1].Data) != "de" {
		t.Fatalf("replay=%#v", replay)
	}
}

func TestCompactionEvictsWholeEventsAndReportsGap(t *testing.T) {
	h, _ := New(5)
	h.Append(1, []byte("abc"))
	h.Append(2, []byte("def"))
	earliest, latest, retained := h.Bounds()
	if earliest != 3 || latest != 6 || retained != 3 {
		t.Fatalf("bounds=(%d,%d,%d)", earliest, latest, retained)
	}
	_, err := h.Replay(2, 0)
	var gap *GapError
	if !errors.As(err, &gap) || gap.EarliestSequence != 3 || gap.LatestSequence != 6 {
		t.Fatalf("gap=%#v err=%v", gap, err)
	}
}

func TestClearPreservesMonotonicSequence(t *testing.T) {
	h, _ := New(8)
	h.Append(1, []byte("abc"))
	if next := h.Clear(); next != 3 {
		t.Fatalf("clear=%d", next)
	}
	event, err := h.Append(1, []byte("d"))
	if err != nil || event.StartSequence != 3 {
		t.Fatalf("event=%#v err=%v", event, err)
	}
}

func TestAcknowledgementsAreCumulativeAndBounded(t *testing.T) {
	h, _ := New(8)
	h.Append(1, []byte("abcd"))
	if err := h.Acknowledge("att_1", 3); err != nil {
		t.Fatal(err)
	}
	for _, next := range []uint64{2, 5} {
		if err := h.Acknowledge("att_1", next); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("next=%d err=%v", next, err)
		}
	}
	if next, ok := h.Acknowledged("att_1"); !ok || next != 3 {
		t.Fatalf("ack=%d ok=%v", next, ok)
	}
}

func TestConcurrentAppendAndReplay(t *testing.T) {
	h, _ := New(1024)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := h.Append(1, []byte("x")); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	_, latest, _ := h.Bounds()
	if latest != 200 {
		t.Fatalf("latest=%d", latest)
	}
	replay, err := h.Replay(0, 0)
	if err != nil || replay.ToSequence != 200 {
		t.Fatalf("to=%d err=%v", replay.ToSequence, err)
	}
}
