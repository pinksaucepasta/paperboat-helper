package history

import (
	"errors"
	"fmt"
	"math"
	"sync"
)

var (
	ErrInvalidLimit  = errors.New("invalid history limit")
	ErrInvalidCursor = errors.New("invalid sequence cursor")
	ErrSequenceFull  = errors.New("output sequence exhausted")
)

type Event struct {
	Channel       byte   `json:"channel"`
	StartSequence uint64 `json:"start_sequence"`
	EndSequence   uint64 `json:"end_sequence"`
	Data          []byte `json:"bytes_base64"`
}

type GapError struct {
	RequestedSequence uint64
	EarliestSequence  uint64
	LatestSequence    uint64
}

func (e *GapError) Error() string {
	return fmt.Sprintf("replay gap: requested %d, retained [%d,%d)", e.RequestedSequence, e.EarliestSequence, e.LatestSequence)
}

type Replay struct {
	FromSequence     uint64  `json:"from_sequence"`
	ToSequence       uint64  `json:"to_sequence"`
	EarliestSequence uint64  `json:"earliest_sequence"`
	LatestSequence   uint64  `json:"latest_sequence"`
	Events           []Event `json:"events"`
}

type History struct {
	mu       sync.RWMutex
	maxBytes uint64
	bytes    uint64
	earliest uint64
	latest   uint64
	events   []Event
	acks     map[string]uint64
}

func New(maxBytes uint64) (*History, error) {
	if maxBytes == 0 {
		return nil, ErrInvalidLimit
	}
	return &History{maxBytes: maxBytes, acks: make(map[string]uint64)}, nil
}

func Restore(maxBytes, earliest, latest uint64, events []Event) (*History, error) {
	history, err := New(maxBytes)
	if err != nil || earliest > latest {
		return nil, ErrInvalidCursor
	}
	history.earliest = earliest
	history.latest = earliest
	for _, event := range events {
		if event.StartSequence != history.latest || event.EndSequence-event.StartSequence != uint64(len(event.Data)) {
			return nil, ErrInvalidCursor
		}
		history.events = append(history.events, cloneEvent(event))
		history.bytes += uint64(len(event.Data))
		history.latest = event.EndSequence
	}
	if history.latest != latest || history.bytes > maxBytes {
		return nil, ErrInvalidCursor
	}
	return history, nil
}

func (h *History) Append(channel byte, data []byte) (Event, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(data) == 0 {
		return Event{Channel: channel, StartSequence: h.latest, EndSequence: h.latest}, nil
	}
	if uint64(len(data)) > math.MaxUint64-h.latest {
		return Event{}, ErrSequenceFull
	}
	owned := append([]byte(nil), data...)
	event := Event{Channel: channel, StartSequence: h.latest, EndSequence: h.latest + uint64(len(owned)), Data: owned}
	h.latest = event.EndSequence
	h.bytes += uint64(len(owned))
	h.events = append(h.events, event)
	h.compactLocked()
	return cloneEvent(event), nil
}

func (h *History) SetLimit(maxBytes uint64) error {
	if maxBytes == 0 {
		return ErrInvalidLimit
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.maxBytes = maxBytes
	h.compactLocked()
	return nil
}

func (h *History) Clear() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = nil
	h.bytes = 0
	h.earliest = h.latest
	return h.latest
}

func (h *History) Replay(fromSequence, byteLimit uint64) (Replay, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := Replay{FromSequence: fromSequence, ToSequence: fromSequence, EarliestSequence: h.earliest, LatestSequence: h.latest}
	if fromSequence < h.earliest {
		return Replay{}, &GapError{RequestedSequence: fromSequence, EarliestSequence: h.earliest, LatestSequence: h.latest}
	}
	if fromSequence > h.latest {
		return Replay{}, ErrInvalidCursor
	}
	remaining := byteLimit
	for _, event := range h.events {
		if event.EndSequence <= fromSequence {
			continue
		}
		start := max(fromSequence, event.StartSequence)
		end := event.EndSequence
		if byteLimit > 0 && end-start > remaining {
			end = start + remaining
		}
		if end > start {
			offsetStart := start - event.StartSequence
			offsetEnd := end - event.StartSequence
			result.Events = append(result.Events, Event{Channel: event.Channel, StartSequence: start, EndSequence: end, Data: append([]byte(nil), event.Data[offsetStart:offsetEnd]...)})
			result.ToSequence = end
			if byteLimit > 0 {
				remaining -= end - start
				if remaining == 0 {
					break
				}
			}
		}
	}
	return result, nil
}

func (h *History) Acknowledge(attachmentID string, nextSequence uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if attachmentID == "" || nextSequence > h.latest || nextSequence < h.acks[attachmentID] {
		return ErrInvalidCursor
	}
	h.acks[attachmentID] = nextSequence
	return nil
}

func (h *History) Acknowledged(attachmentID string) (uint64, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	next, ok := h.acks[attachmentID]
	return next, ok
}

func (h *History) Bounds() (earliest, latest, retainedBytes uint64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.earliest, h.latest, h.bytes
}

func (h *History) compactLocked() {
	for h.bytes > h.maxBytes && len(h.events) > 0 {
		event := h.events[0]
		h.bytes -= uint64(len(event.Data))
		h.earliest = event.EndSequence
		h.events[0] = Event{}
		h.events = h.events[1:]
	}
	if len(h.events) == 0 {
		h.earliest = h.latest
	}
}

func cloneEvent(event Event) Event {
	event.Data = append([]byte(nil), event.Data...)
	return event
}
