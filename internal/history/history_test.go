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

func BenchmarkHistoryCompaction(b *testing.B) {
	const chunkSize = 32 << 10
	payload := make([]byte, chunkSize)
	history, err := New(1 << 20)
	if err != nil {
		b.Fatal(err)
	}
	for range (1 << 20) / chunkSize {
		if _, err := history.AppendOwned(1, payload); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.SetBytes(chunkSize)
	b.ResetTimer()
	for range b.N {
		if _, err := history.AppendOwned(1, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPooledBufferProducerSurvivesImmediateHistoryCompaction(t *testing.T) {
	history, _ := New(1)
	buffer := AcquireBuffer()
	copy(buffer, "abc")
	event, err := history.AppendBuffer(1, buffer[:3])
	if err != nil {
		t.Fatal(err)
	}
	if event.owner == nil {
		t.Fatal("pooled event has no owner")
	}
	if refs := event.owner.refs.Load(); refs != 1 {
		t.Fatalf("producer refs after compaction = %d", refs)
	}
	if got := string(event.Data); got != "abc" {
		t.Fatalf("data after compaction = %q", got)
	}
	event.Release()
}

func TestReplayOwnedRetainsCompactedBuffer(t *testing.T) {
	history, _ := New(PooledBufferSize)
	buffer := AcquireBuffer()
	copy(buffer, "abc")
	event, err := history.AppendBuffer(1, buffer[:3])
	if err != nil {
		t.Fatal(err)
	}
	event.Release()
	replay, err := history.ReplayOwned(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if refs := replay.Events[0].owner.refs.Load(); refs != 2 {
		t.Fatalf("history and replay refs = %d", refs)
	}
	if err := history.SetLimit(1); err != nil {
		t.Fatal(err)
	}
	if got := string(replay.Events[0].Data); got != "abc" {
		t.Fatalf("replay data after compaction = %q", got)
	}
	replay.Release()
}

func BenchmarkPooledHistoryCompaction(b *testing.B) {
	history, _ := New(1)
	buffer := AcquireBuffer()
	event, _ := history.AppendBuffer(1, buffer[:1])
	event.Release()
	b.ReportAllocs()
	b.SetBytes(PooledBufferSize)
	b.ResetTimer()
	for range b.N {
		buffer := AcquireBuffer()
		event, err := history.AppendBuffer(1, buffer)
		if err != nil {
			b.Fatal(err)
		}
		event.Release()
	}
}
