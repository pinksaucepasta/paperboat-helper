package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/pinksaucepasta/paperboat-helper/internal/history"
)

var (
	ErrAttachmentExists  = errors.New("attachment already exists")
	ErrAttachmentUnknown = errors.New("attachment not found")
	ErrAttachmentEvicted = errors.New("slow consumer evicted")
	ErrOutputOrder       = errors.New("output sequence is not contiguous")
	ErrInvalidQueueLimit = errors.New("invalid attachment queue limit")
)

type Eviction struct {
	AttachmentID string
	QueuedBytes  uint64
	Reason       string
}

type attachmentCursor struct {
	state        AttachmentState
	maxBytes     uint64
	queuedBytes  uint64
	evictedBytes uint64
	cursor       uint64
	enqueuedEnd  uint64
	hasCursor    bool
	notify       chan struct{}
}

// Fanout retains one ordered ring of immutable chunks. Attachments track only
// sequence cursors and byte counts; they never own per-client event slices.
type Fanout struct {
	mu          sync.Mutex
	attachments map[string]*attachmentCursor
	active      sync.Map
	ring        []history.Event
}

func NewFanout() *Fanout { return &Fanout{attachments: make(map[string]*attachmentCursor)} }

func (f *Fanout) Attach(attachmentID string, maxPendingBytes uint64) error {
	if attachmentID == "" || maxPendingBytes == 0 {
		return ErrInvalidQueueLimit
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing := f.attachments[attachmentID]; existing != nil && existing.state == Attached {
		return ErrAttachmentExists
	}
	f.attachments[attachmentID] = &attachmentCursor{state: Attached, maxBytes: maxPendingBytes, notify: make(chan struct{}, 1)}
	f.active.Store(attachmentID, struct{}{})
	return nil
}

func (f *Fanout) Detach(attachmentID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cursor, ok := f.attachments[attachmentID]
	if !ok {
		return ErrAttachmentUnknown
	}
	if cursor.state != Attached && cursor.state != Evicted {
		return ErrInvalidTransition
	}
	cursor.state = Detached
	f.active.Delete(attachmentID)
	notify(cursor.notify)
	delete(f.attachments, attachmentID)
	f.compactLocked()
	return nil
}

func (f *Fanout) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, cursor := range f.attachments {
		if cursor.state == Attached {
			count++
		}
	}
	return count
}

// IsAttached is lock-free so terminal input never waits behind output fanout.
func (f *Fanout) IsAttached(attachmentID string) bool {
	_, ok := f.active.Load(attachmentID)
	return ok
}

func (f *Fanout) Publish(event history.Event) ([]Eviction, error) {
	event.Data = append([]byte(nil), event.Data...)
	return f.PublishOwned(event)
}

func (f *Fanout) PublishOwned(event history.Event) ([]Eviction, error) {
	if !validOutputEvent(event) {
		return nil, ErrOutputOrder
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, cursor := range f.attachments {
		if cursor.state == Attached && cursor.hasCursor && cursor.enqueuedEnd != event.StartSequence {
			return nil, fmt.Errorf("attachment %s expected %d, got %d: %w", id, cursor.enqueuedEnd, event.StartSequence, ErrOutputOrder)
		}
	}
	f.insertEventLocked(event)
	var evictions []Eviction
	for id, cursor := range f.attachments {
		if cursor.state != Attached {
			continue
		}
		if uint64(len(event.Data)) > cursor.maxBytes-cursor.queuedBytes {
			evictions = append(evictions, Eviction{AttachmentID: id, QueuedBytes: cursor.queuedBytes, Reason: "slow_consumer"})
			cursor.evictedBytes = cursor.queuedBytes
			cursor.queuedBytes = 0
			cursor.state = Evicted
			f.active.Delete(id)
			notify(cursor.notify)
			continue
		}
		if !cursor.hasCursor {
			cursor.cursor = event.StartSequence
			cursor.hasCursor = true
		}
		cursor.enqueuedEnd = event.EndSequence
		cursor.queuedBytes += uint64(len(event.Data))
		notify(cursor.notify)
	}
	f.compactLocked()
	return evictions, nil
}

func (f *Fanout) Enqueue(attachmentID string, event history.Event) (*Eviction, error) {
	event.Data = append([]byte(nil), event.Data...)
	return f.EnqueueOwned(attachmentID, event)
}

func (f *Fanout) EnqueueOwned(attachmentID string, event history.Event) (*Eviction, error) {
	if !validOutputEvent(event) {
		return nil, ErrOutputOrder
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cursor, ok := f.attachments[attachmentID]
	if !ok {
		return nil, ErrAttachmentUnknown
	}
	if cursor.state == Evicted {
		return nil, ErrAttachmentEvicted
	}
	if cursor.state != Attached {
		return nil, ErrInvalidTransition
	}
	if cursor.hasCursor && cursor.enqueuedEnd != event.StartSequence {
		return nil, ErrOutputOrder
	}
	if uint64(len(event.Data)) > cursor.maxBytes-cursor.queuedBytes {
		eviction := &Eviction{AttachmentID: attachmentID, QueuedBytes: cursor.queuedBytes, Reason: "slow_consumer"}
		cursor.evictedBytes = cursor.queuedBytes
		cursor.queuedBytes = 0
		cursor.state = Evicted
		f.active.Delete(attachmentID)
		notify(cursor.notify)
		f.compactLocked()
		return eviction, nil
	}
	f.insertEventLocked(event)
	if !cursor.hasCursor {
		cursor.cursor = event.StartSequence
		cursor.hasCursor = true
	}
	cursor.enqueuedEnd = event.EndSequence
	cursor.queuedBytes += uint64(len(event.Data))
	notify(cursor.notify)
	return nil, nil
}

func (f *Fanout) WaitNext(ctx context.Context, attachmentID string) (history.Event, error) {
	event, err := f.WaitNextOwned(ctx, attachmentID)
	if err != nil {
		return history.Event{}, err
	}
	data := append([]byte(nil), event.Data...)
	event.Release()
	event.Data = data
	return event, nil
}

func (f *Fanout) WaitNextOwned(ctx context.Context, attachmentID string) (history.Event, error) {
	for {
		f.mu.Lock()
		cursor, ok := f.attachments[attachmentID]
		if !ok {
			f.mu.Unlock()
			return history.Event{}, ErrAttachmentUnknown
		}
		if cursor.state == Evicted {
			f.mu.Unlock()
			return history.Event{}, ErrAttachmentEvicted
		}
		if cursor.state != Attached {
			f.mu.Unlock()
			return history.Event{}, ErrInvalidTransition
		}
		if cursor.hasCursor && cursor.cursor < cursor.enqueuedEnd {
			event, found := f.eventAtLocked(cursor.cursor)
			if !found {
				f.mu.Unlock()
				return history.Event{}, ErrOutputOrder
			}
			start := cursor.cursor
			retained := event.Retain()
			offset := start - event.StartSequence
			retained.StartSequence = start
			retained.Data = event.Data[offset:]
			retained.EndSequence = event.EndSequence
			consumed := retained.EndSequence - retained.StartSequence
			cursor.cursor = retained.EndSequence
			cursor.queuedBytes -= consumed
			f.compactLocked()
			f.mu.Unlock()
			return retained, nil
		}
		notification := cursor.notify
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return history.Event{}, ctx.Err()
		case <-notification:
		}
	}
}

func (f *Fanout) Next(attachmentID string) (history.Event, bool, error) {
	f.mu.Lock()
	cursor, ok := f.attachments[attachmentID]
	if !ok {
		f.mu.Unlock()
		return history.Event{}, false, ErrAttachmentUnknown
	}
	if cursor.state == Evicted {
		f.mu.Unlock()
		return history.Event{}, false, ErrAttachmentEvicted
	}
	if cursor.state != Attached {
		f.mu.Unlock()
		return history.Event{}, false, ErrInvalidTransition
	}
	if !cursor.hasCursor || cursor.cursor >= cursor.enqueuedEnd {
		f.mu.Unlock()
		return history.Event{}, false, nil
	}
	event, found := f.eventAtLocked(cursor.cursor)
	if !found {
		f.mu.Unlock()
		return history.Event{}, false, ErrOutputOrder
	}
	start := cursor.cursor
	data := append([]byte(nil), event.Data[start-event.StartSequence:]...)
	end := event.EndSequence
	cursor.cursor = end
	cursor.queuedBytes -= end - start
	f.compactLocked()
	f.mu.Unlock()
	return history.Event{Channel: event.Channel, StartSequence: start, EndSequence: end, Data: data}, true, nil
}

func (f *Fanout) Status(attachmentID string) (AttachmentState, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cursor, ok := f.attachments[attachmentID]
	if !ok {
		return "", 0, ErrAttachmentUnknown
	}
	queued := cursor.queuedBytes
	if cursor.state == Evicted {
		queued = cursor.evictedBytes
	}
	return cursor.state, queued, nil
}

func validOutputEvent(event history.Event) bool {
	return event.EndSequence >= event.StartSequence && event.EndSequence-event.StartSequence == uint64(len(event.Data)) && len(event.Data) != 0
}

func (f *Fanout) eventAtLocked(sequence uint64) (history.Event, bool) {
	index := sort.Search(len(f.ring), func(i int) bool { return f.ring[i].EndSequence > sequence })
	if index == len(f.ring) || f.ring[index].StartSequence > sequence {
		return history.Event{}, false
	}
	return f.ring[index], true
}

func (f *Fanout) insertEventLocked(event history.Event) {
	for index, existing := range f.ring {
		if existing.StartSequence <= event.StartSequence && existing.EndSequence >= event.EndSequence && existing.Channel == event.Channel && bytes.Equal(existing.Data[event.StartSequence-existing.StartSequence:event.EndSequence-existing.StartSequence], event.Data) {
			return
		}
		if event.StartSequence <= existing.StartSequence && event.EndSequence >= existing.EndSequence && event.Channel == existing.Channel && bytes.Equal(event.Data[existing.StartSequence-event.StartSequence:existing.EndSequence-event.StartSequence], existing.Data) {
			existing.Release()
			f.ring = append(f.ring[:index], f.ring[index+1:]...)
			f.insertEventLocked(event)
			return
		}
	}
	f.ring = append(f.ring, event.Retain())
	sort.Slice(f.ring, func(i, j int) bool {
		if f.ring[i].StartSequence == f.ring[j].StartSequence {
			return f.ring[i].EndSequence > f.ring[j].EndSequence
		}
		return f.ring[i].StartSequence < f.ring[j].StartSequence
	})
}

func (f *Fanout) compactLocked() {
	var minimum uint64
	haveMinimum := false
	for _, cursor := range f.attachments {
		if cursor.state != Attached || !cursor.hasCursor {
			continue
		}
		if !haveMinimum || cursor.cursor < minimum {
			minimum = cursor.cursor
			haveMinimum = true
		}
	}
	if !haveMinimum {
		for _, event := range f.ring {
			event.Release()
		}
		f.ring = nil
		return
	}
	index := 0
	for index < len(f.ring) && f.ring[index].EndSequence <= minimum {
		f.ring[index].Release()
		index++
	}
	if index > 0 {
		copy(f.ring, f.ring[index:])
		clear(f.ring[len(f.ring)-index:])
		f.ring = f.ring[:len(f.ring)-index]
	}
}

func notify(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}
