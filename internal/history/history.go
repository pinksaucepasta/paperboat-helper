package history

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
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
	owner         *ownedBuffer
}

const PooledBufferSize = 32 << 10

type pooledBuffer [PooledBufferSize]byte

var bufferPool = sync.Pool{New: func() any { return new(pooledBuffer) }}
var ownerPool = sync.Pool{New: func() any { return new(ownedBuffer) }}

type ownedBuffer struct {
	data []byte
	refs atomic.Int64
	pool bool
}

func AcquireBuffer() []byte { return bufferPool.Get().(*pooledBuffer)[:] }

func ReleaseBuffer(buffer []byte) {
	if cap(buffer) == PooledBufferSize {
		bufferPool.Put((*pooledBuffer)(unsafe.Pointer(&buffer[:PooledBufferSize][0])))
	}
}

// AppendBuffer transfers ownership of a buffer obtained from AcquireBuffer.
func (h *History) AppendBuffer(channel byte, data []byte) (Event, error) {
	return h.appendOwned(channel, data, true, true)
}

func (e Event) Retain() Event {
	if e.owner != nil {
		e.owner.refs.Add(1)
	}
	return e
}

func (e Event) Release() {
	if e.owner != nil && e.owner.refs.Add(-1) == 0 {
		if e.owner.pool {
			ReleaseBuffer(e.owner.data)
		}
		e.owner.data = nil
		e.owner.pool = false
		ownerPool.Put(e.owner)
	}
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

func (r Replay) Release() {
	for _, event := range r.Events {
		event.Release()
	}
}

type History struct {
	mu       sync.RWMutex
	maxBytes uint64
	bytes    uint64
	earliest uint64
	latest   uint64
	events   []Event
	head     int
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
	event, err := h.AppendOwned(channel, data)
	return cloneEvent(event), err
}

// AppendOwned copies data once into immutable history ownership and returns a
// reference that internal fanout and persistence consumers may share.
func (h *History) AppendOwned(channel byte, data []byte) (Event, error) {
	return h.appendOwned(channel, append([]byte(nil), data...), false, false)
}

func (h *History) appendOwned(channel byte, data []byte, pooled, producerReference bool) (Event, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(data) == 0 {
		if pooled {
			ReleaseBuffer(data)
		}
		return Event{Channel: channel, StartSequence: h.latest, EndSequence: h.latest}, nil
	}
	if uint64(len(data)) > math.MaxUint64-h.latest {
		if pooled {
			ReleaseBuffer(data)
		}
		return Event{}, ErrSequenceFull
	}
	owner := ownerPool.Get().(*ownedBuffer)
	owner.data = data
	owner.pool = pooled
	refs := int64(1)
	if producerReference {
		refs++
	}
	owner.refs.Store(refs)
	event := Event{Channel: channel, StartSequence: h.latest, EndSequence: h.latest + uint64(len(data)), Data: data, owner: owner}
	h.latest = event.EndSequence
	h.bytes += uint64(len(data))
	h.events = append(h.events, event)
	h.compactLocked()
	return event, nil
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
	for _, event := range h.events[h.head:] {
		event.Release()
	}
	clear(h.events)
	h.events = h.events[:0]
	h.head = 0
	h.bytes = 0
	h.earliest = h.latest
	return h.latest
}

func (h *History) Replay(fromSequence, byteLimit uint64) (Replay, error) {
	replay, err := h.ReplayOwned(fromSequence, byteLimit)
	if err != nil {
		return Replay{}, err
	}
	for index := range replay.Events {
		owned := replay.Events[index]
		replay.Events[index] = cloneEvent(owned)
		owned.Release()
	}
	return replay, nil
}

// ReplayOwned returns immutable references retained by history. References
// remain valid after compaction because consumers keep the backing arrays live.
func (h *History) ReplayOwned(fromSequence, byteLimit uint64) (Replay, error) {
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
	for _, event := range h.events[h.head:] {
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
			retained := event.Retain()
			retained.StartSequence = start
			retained.EndSequence = end
			retained.Data = event.Data[offsetStart:offsetEnd]
			result.Events = append(result.Events, retained)
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
	for h.bytes > h.maxBytes && h.head < len(h.events) {
		event := h.events[h.head]
		h.bytes -= uint64(len(event.Data))
		h.earliest = event.EndSequence
		h.events[h.head] = Event{}
		h.head++
		event.Release()
	}
	if h.head == len(h.events) {
		h.earliest = h.latest
		h.events = h.events[:0]
		h.head = 0
	}
}

func cloneEvent(event Event) Event {
	event.Data = append([]byte(nil), event.Data...)
	event.owner = nil
	return event
}
