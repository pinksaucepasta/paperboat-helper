package session

import (
	"context"
	"errors"
	"fmt"
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

type outputQueue struct {
	mu           sync.Mutex
	state        AttachmentState
	maxBytes     uint64
	queuedBytes  uint64
	events       []history.Event
	next         uint64
	hasNext      bool
	notify       chan struct{}
	evictedBytes uint64
}

type Fanout struct {
	mu          sync.RWMutex
	attachments map[string]*outputQueue
}

func NewFanout() *Fanout { return &Fanout{attachments: make(map[string]*outputQueue)} }

func (f *Fanout) Attach(attachmentID string, maxPendingBytes uint64) error {
	if attachmentID == "" || maxPendingBytes == 0 {
		return ErrInvalidQueueLimit
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, exists := f.attachments[attachmentID]; exists {
		existing.mu.Lock()
		state := existing.state
		existing.mu.Unlock()
		if state == Attached {
			return ErrAttachmentExists
		}
	}
	f.attachments[attachmentID] = &outputQueue{state: Attached, maxBytes: maxPendingBytes, notify: make(chan struct{}, 1)}
	return nil
}

func (f *Fanout) Detach(attachmentID string) error {
	f.mu.Lock()
	queue, ok := f.attachments[attachmentID]
	if !ok {
		f.mu.Unlock()
		return ErrAttachmentUnknown
	}
	queue.mu.Lock()
	defer func() { queue.mu.Unlock(); f.mu.Unlock() }()
	if queue.state != Attached && queue.state != Evicted {
		return ErrInvalidTransition
	}
	queue.state = Detached
	queue.events = nil
	queue.queuedBytes = 0
	notify(queue.notify)
	delete(f.attachments, attachmentID)
	return nil
}

func (f *Fanout) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	count := 0
	for _, queue := range f.attachments {
		queue.mu.Lock()
		if queue.state == Attached {
			count++
		}
		queue.mu.Unlock()
	}
	return count
}

func (f *Fanout) Publish(event history.Event) ([]Eviction, error) {
	if event.EndSequence < event.StartSequence || event.EndSequence-event.StartSequence != uint64(len(event.Data)) {
		return nil, ErrOutputOrder
	}
	owned := event
	owned.Data = append([]byte(nil), event.Data...)
	// Publish serializes all attachment queues so every non-evicted attachment
	// observes the same output order.
	f.mu.Lock()
	defer f.mu.Unlock()
	queues := make([]struct {
		id string
		q  *outputQueue
	}, 0, len(f.attachments))
	for id, queue := range f.attachments {
		queue.mu.Lock()
		queues = append(queues, struct {
			id string
			q  *outputQueue
		}{id, queue})
	}
	defer func() {
		for _, item := range queues {
			item.q.mu.Unlock()
		}
	}()
	for _, item := range queues {
		if item.q.state == Attached && item.q.hasNext && item.q.next != event.StartSequence {
			return nil, fmt.Errorf("attachment %s expected %d, got %d: %w", item.id, item.q.next, event.StartSequence, ErrOutputOrder)
		}
	}
	var evictions []Eviction
	for _, item := range queues {
		queue := item.q
		if queue.state != Attached {
			continue
		}
		if uint64(len(owned.Data)) > queue.maxBytes-queue.queuedBytes {
			evictions = append(evictions, Eviction{AttachmentID: item.id, QueuedBytes: queue.queuedBytes, Reason: "slow_consumer"})
			queue.evictedBytes = queue.queuedBytes
			queue.state = Evicted
			queue.events = nil
			queue.queuedBytes = 0
			notify(queue.notify)
			continue
		}
		queue.events = append(queue.events, owned)
		queue.queuedBytes += uint64(len(owned.Data))
		queue.next = owned.EndSequence
		queue.hasNext = true
		notify(queue.notify)
	}
	return evictions, nil
}

// Enqueue adds replay output to one attachment without rebroadcasting it to
// attachments that have already received the same sequence range.
func (f *Fanout) Enqueue(attachmentID string, event history.Event) (*Eviction, error) {
	if event.EndSequence < event.StartSequence || event.EndSequence-event.StartSequence != uint64(len(event.Data)) {
		return nil, ErrOutputOrder
	}
	f.mu.RLock()
	queue, ok := f.attachments[attachmentID]
	f.mu.RUnlock()
	if !ok {
		return nil, ErrAttachmentUnknown
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.state != Attached {
		if queue.state == Evicted {
			return nil, ErrAttachmentEvicted
		}
		return nil, ErrInvalidTransition
	}
	if queue.hasNext && queue.next != event.StartSequence {
		return nil, ErrOutputOrder
	}
	if uint64(len(event.Data)) > queue.maxBytes-queue.queuedBytes {
		eviction := &Eviction{AttachmentID: attachmentID, QueuedBytes: queue.queuedBytes, Reason: "slow_consumer"}
		queue.evictedBytes = queue.queuedBytes
		queue.state = Evicted
		queue.events = nil
		queue.queuedBytes = 0
		notify(queue.notify)
		return eviction, nil
	}
	event.Data = append([]byte(nil), event.Data...)
	queue.events = append(queue.events, event)
	queue.queuedBytes += uint64(len(event.Data))
	queue.next = event.EndSequence
	queue.hasNext = true
	notify(queue.notify)
	return nil, nil
}

func (f *Fanout) WaitNext(ctx context.Context, attachmentID string) (history.Event, error) {
	for {
		f.mu.RLock()
		queue, ok := f.attachments[attachmentID]
		f.mu.RUnlock()
		if !ok {
			return history.Event{}, ErrAttachmentUnknown
		}
		queue.mu.Lock()
		if queue.state == Evicted {
			queue.mu.Unlock()
			return history.Event{}, ErrAttachmentEvicted
		}
		if queue.state != Attached {
			queue.mu.Unlock()
			return history.Event{}, ErrInvalidTransition
		}
		if len(queue.events) > 0 {
			event := queue.events[0]
			queue.events[0] = history.Event{}
			queue.events = queue.events[1:]
			queue.queuedBytes -= uint64(len(event.Data))
			queue.mu.Unlock()
			event.Data = append([]byte(nil), event.Data...)
			return event, nil
		}
		notification := queue.notify
		queue.mu.Unlock()
		select {
		case <-ctx.Done():
			return history.Event{}, ctx.Err()
		case <-notification:
		}
	}
}

func notify(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func (f *Fanout) Next(attachmentID string) (history.Event, bool, error) {
	f.mu.RLock()
	queue, ok := f.attachments[attachmentID]
	f.mu.RUnlock()
	if !ok {
		return history.Event{}, false, ErrAttachmentUnknown
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.state == Evicted {
		return history.Event{}, false, ErrAttachmentEvicted
	}
	if queue.state != Attached {
		return history.Event{}, false, ErrInvalidTransition
	}
	if len(queue.events) == 0 {
		return history.Event{}, false, nil
	}
	event := queue.events[0]
	queue.events[0] = history.Event{}
	queue.events = queue.events[1:]
	queue.queuedBytes -= uint64(len(event.Data))
	event.Data = append([]byte(nil), event.Data...)
	return event, true, nil
}

func (f *Fanout) Status(attachmentID string) (AttachmentState, uint64, error) {
	f.mu.RLock()
	queue, ok := f.attachments[attachmentID]
	f.mu.RUnlock()
	if !ok {
		return "", 0, ErrAttachmentUnknown
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queued := queue.queuedBytes
	if queue.state == Evicted {
		queued = queue.evictedBytes
	}
	return queue.state, queued, nil
}
