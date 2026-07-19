package session

import (
	"context"
	"errors"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/history"
)

func TestWaitNextWakesForOutputDetachEvictionAndCancellation(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		action func(*Fanout)
		want   error
	}{
		{"output", func(f *Fanout) {
			_, _ = f.Publish(history.Event{Channel: 1, StartSequence: 0, EndSequence: 1, Data: []byte("x")})
		}, nil},
		{"detach", func(f *Fanout) { _ = f.Detach("att") }, ErrAttachmentUnknown},
		{"evict", func(f *Fanout) {
			_, _ = f.Publish(history.Event{Channel: 1, StartSequence: 0, EndSequence: 2, Data: []byte("xx")})
		}, ErrAttachmentEvicted},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fanout := NewFanout()
			limit := uint64(1)
			if testCase.name == "output" || testCase.name == "detach" {
				limit = 8
			}
			if err := fanout.Attach("att", limit); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				event, err := fanout.WaitNext(context.Background(), "att")
				if err == nil && string(event.Data) != "x" {
					err = ErrOutputOrder
				}
				done <- err
			}()
			testCase.action(fanout)
			if err := <-done; !errors.Is(err, testCase.want) {
				t.Fatalf("err=%v want=%v", err, testCase.want)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	fanout := NewFanout()
	_ = fanout.Attach("att", 8)
	cancel()
	if _, err := fanout.WaitNext(ctx, "att"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel err=%v", err)
	}
}

func TestFanoutMirrorsOrderedOutput(t *testing.T) {
	f := NewFanout()
	for _, id := range []string{"att_1", "att_2"} {
		if err := f.Attach(id, 16); err != nil {
			t.Fatal(err)
		}
	}
	event := history.Event{Channel: 1, StartSequence: 0, EndSequence: 3, Data: []byte("abc")}
	if evictions, err := f.Publish(event); err != nil || len(evictions) != 0 {
		t.Fatalf("evictions=%v err=%v", evictions, err)
	}
	event.Data[0] = 'x'
	for _, id := range []string{"att_1", "att_2"} {
		got, ok, err := f.Next(id)
		if err != nil || !ok || string(got.Data) != "abc" {
			t.Fatalf("%s event=%#v ok=%v err=%v", id, got, ok, err)
		}
	}
}

func TestSlowConsumerEvictsOnlyFullAttachment(t *testing.T) {
	f := NewFanout()
	f.Attach("slow", 3)
	f.Attach("fast", 3)
	f.Publish(history.Event{StartSequence: 0, EndSequence: 3, Data: []byte("abc")})
	if _, ok, err := f.Next("fast"); err != nil || !ok {
		t.Fatalf("fast drain ok=%v err=%v", ok, err)
	}
	evictions, err := f.Publish(history.Event{StartSequence: 3, EndSequence: 4, Data: []byte("d")})
	if err != nil || len(evictions) != 1 || evictions[0].AttachmentID != "slow" || evictions[0].QueuedBytes != 3 {
		t.Fatalf("evictions=%#v err=%v", evictions, err)
	}
	if _, _, err := f.Next("slow"); !errors.Is(err, ErrAttachmentEvicted) {
		t.Fatalf("slow err=%v", err)
	}
	if state, queued, statusErr := f.Status("slow"); statusErr != nil || state != Evicted || queued != 3 {
		t.Fatalf("slow state=%s queued=%d err=%v", state, queued, statusErr)
	}
	got, ok, err := f.Next("fast")
	if err != nil || !ok || string(got.Data) != "d" {
		t.Fatalf("fast=%#v ok=%v err=%v", got, ok, err)
	}
}

func TestDetachDoesNotAffectOtherAttachments(t *testing.T) {
	f := NewFanout()
	f.Attach("gone", 8)
	f.Attach("live", 8)
	if err := f.Detach("gone"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Publish(history.Event{StartSequence: 10, EndSequence: 11, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.Status("gone"); !errors.Is(err, ErrAttachmentUnknown) {
		t.Fatalf("detached status err=%v", err)
	}
	if _, ok, err := f.Next("live"); err != nil || !ok {
		t.Fatalf("live ok=%v err=%v", ok, err)
	}
}

func TestDetachReleasesAttachmentIdentityAndCapacity(t *testing.T) {
	fanout := NewFanout()
	if err := fanout.Attach("att", 8); err != nil {
		t.Fatal(err)
	}
	if fanout.Count() != 1 {
		t.Fatalf("count=%d", fanout.Count())
	}
	if err := fanout.Detach("att"); err != nil {
		t.Fatal(err)
	}
	if fanout.Count() != 0 {
		t.Fatalf("detached count=%d", fanout.Count())
	}
	if err := fanout.Attach("att", 8); err != nil {
		t.Fatalf("reuse err=%v", err)
	}
}

func TestFanoutRejectsNoncontiguousOutputWithoutPartialDelivery(t *testing.T) {
	f := NewFanout()
	f.Attach("one", 8)
	f.Publish(history.Event{StartSequence: 0, EndSequence: 1, Data: []byte("a")})
	if _, err := f.Publish(history.Event{StartSequence: 2, EndSequence: 3, Data: []byte("b")}); !errors.Is(err, ErrOutputOrder) {
		t.Fatalf("err=%v", err)
	}
	_, queued, _ := f.Status("one")
	if queued != 1 {
		t.Fatalf("queued=%d", queued)
	}
}
