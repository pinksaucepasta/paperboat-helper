package activity

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
)

const (
	MaxBatchEvents = 100
	MaxBatchBytes  = 64 << 10
)

var (
	ErrInvalidSource   = errors.New("invalid activity source")
	ErrUnauthenticated = errors.New("unauthenticated activity")
	ErrInvalidSequence = errors.New("activity sequence is not newer")
	ErrInvalidTime     = errors.New("invalid activity time")
	ErrQueueFull       = errors.New("activity queue is full")
	ErrBatchMismatch   = errors.New("activity batch mismatch")
)

type Source string

const (
	TerminalInput    Source = "terminal_input"
	AgentInteraction Source = "agent_interaction"
	CLIActivity      Source = "cli_activity"
	AgentSignal      Source = "agent_signal"
)

var validSources = map[Source]bool{TerminalInput: true, AgentInteraction: true, CLIActivity: true, AgentSignal: true}

type Event struct {
	EnvironmentID string    `json:"environment_id"`
	SourceID      string    `json:"source_id"`
	SessionID     string    `json:"session_id,omitempty"`
	ProcessID     string    `json:"process_id,omitempty"`
	Source        Source    `json:"source"`
	Sequence      uint64    `json:"sequence"`
	OccurredAt    time.Time `json:"occurred_at"`
	ObservedAt    time.Time `json:"observed_at"`
}

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Config struct {
	Clock          Clock
	Freshness      time.Duration
	FutureSkew     time.Duration
	MaxQueued      int
	MaxDiagnostics int
}
type RecordResult struct {
	Fresh       bool
	ExtendsIdle bool
}
type Diagnostic struct {
	EnvironmentID string
	SourceID      string
	Source        Source
	Sequence      uint64
	Reason        string
	ObservedAt    time.Time
}
type Batch struct {
	ID     uint64
	Body   []byte
	Events []Event
}

type sourceKey struct {
	environmentID, sourceID string
	source                  Source
}
type activeBatch struct {
	id     uint64
	count  int
	body   []byte
	events []Event
}

type Collector struct {
	mu           sync.Mutex
	config       Config
	sequences    map[sourceKey]uint64
	queued       []Event
	diagnostics  []Diagnostic
	lastActivity time.Time
	nextBatchID  uint64
	active       *activeBatch
}

func New(config Config) (*Collector, error) {
	if config.Clock == nil {
		config.Clock = realClock{}
	}
	if config.Freshness == 0 {
		config.Freshness = 5 * time.Minute
	}
	if config.FutureSkew == 0 {
		config.FutureSkew = time.Minute
	}
	if config.MaxQueued == 0 {
		config.MaxQueued = helperconfig.DefaultResources.MaxActivityEvents
	}
	if config.MaxDiagnostics == 0 {
		config.MaxDiagnostics = 100
	}
	if config.Freshness <= 0 || config.FutureSkew < 0 || config.MaxQueued < 1 || config.MaxDiagnostics < 1 {
		return nil, ErrQueueFull
	}
	return &Collector{config: config, sequences: make(map[sourceKey]uint64)}, nil
}

func (c *Collector) Record(event Event, authenticated bool) (RecordResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.config.Clock.Now().UTC()
	event.ObservedAt = now
	if !validSources[event.Source] || !validID(event.EnvironmentID) || !validID(event.SourceID) || len(event.SessionID) > 128 || len(event.ProcessID) > 128 {
		c.diagnosticLocked(event, "invalid_source")
		return RecordResult{}, ErrInvalidSource
	}
	if !authenticated {
		c.diagnosticLocked(event, "unauthenticated")
		return RecordResult{}, ErrUnauthenticated
	}
	if event.Sequence == 0 {
		c.diagnosticLocked(event, "invalid_sequence")
		return RecordResult{}, ErrInvalidSequence
	}
	if event.OccurredAt.IsZero() || event.OccurredAt.After(now.Add(c.config.FutureSkew)) {
		c.diagnosticLocked(event, "invalid_time")
		return RecordResult{}, ErrInvalidTime
	}
	key := sourceKey{event.EnvironmentID, event.SourceID, event.Source}
	if event.Sequence <= c.sequences[key] {
		c.diagnosticLocked(event, "duplicate_or_old")
		return RecordResult{}, ErrInvalidSequence
	}
	if len(c.queued) >= c.config.MaxQueued {
		return RecordResult{}, ErrQueueFull
	}
	c.sequences[key] = event.Sequence
	c.queued = append(c.queued, event)
	fresh := !event.OccurredAt.Before(now.Add(-c.config.Freshness))
	if fresh && event.OccurredAt.After(c.lastActivity) {
		c.lastActivity = event.OccurredAt
	}
	if !fresh {
		c.diagnosticLocked(event, "stale")
	}
	return RecordResult{Fresh: fresh, ExtendsIdle: fresh}, nil
}

func (c *Collector) PeekBatch() (Batch, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active != nil {
		return cloneBatch(c.active), true, nil
	}
	if len(c.queued) == 0 {
		return Batch{}, false, nil
	}
	events := make([]Event, 0, min(len(c.queued), MaxBatchEvents))
	var body []byte
	for _, event := range c.queued {
		if len(events) == MaxBatchEvents {
			break
		}
		candidate := append(append([]Event(nil), events...), event)
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return Batch{}, false, err
		}
		if len(encoded) > MaxBatchBytes {
			break
		}
		events = candidate
		body = encoded
	}
	if len(events) == 0 {
		return Batch{}, false, ErrQueueFull
	}
	c.nextBatchID++
	c.active = &activeBatch{id: c.nextBatchID, count: len(events), body: body, events: events}
	return cloneBatch(c.active), true, nil
}

func (c *Collector) Acknowledge(batchID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil || c.active.id != batchID {
		return ErrBatchMismatch
	}
	count := c.active.count
	copy(c.queued, c.queued[count:])
	for i := len(c.queued) - count; i < len(c.queued); i++ {
		c.queued[i] = Event{}
	}
	c.queued = c.queued[:len(c.queued)-count]
	c.active = nil
	return nil
}

func (c *Collector) LastActivity() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.lastActivity }
func (c *Collector) Diagnostics() []Diagnostic {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Diagnostic(nil), c.diagnostics...)
}
func (c *Collector) diagnosticLocked(event Event, reason string) {
	d := Diagnostic{EnvironmentID: event.EnvironmentID, SourceID: event.SourceID, Source: event.Source, Sequence: event.Sequence, Reason: reason, ObservedAt: event.ObservedAt}
	if len(c.diagnostics) == c.config.MaxDiagnostics {
		copy(c.diagnostics, c.diagnostics[1:])
		c.diagnostics[len(c.diagnostics)-1] = d
	} else {
		c.diagnostics = append(c.diagnostics, d)
	}
}
func cloneBatch(active *activeBatch) Batch {
	return Batch{ID: active.id, Body: append([]byte(nil), active.body...), Events: append([]Event(nil), active.events...)}
}

func validID(value string) bool { return len(value) >= 1 && len(value) <= 128 }
