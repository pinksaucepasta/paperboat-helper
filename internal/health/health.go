package health

import (
	"sync"
	"time"
)

type State string

const (
	Ready       State = "ready"
	Degraded    State = "degraded"
	Unavailable State = "unavailable"
)

type Capability struct {
	State        State  `json:"state"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs uint64 `json:"retry_after_ms,omitempty"`
}

type Snapshot struct {
	Live         bool                  `json:"live"`
	Version      string                `json:"version"`
	Capabilities map[string]Capability `json:"capabilities"`
	CheckedAt    time.Time             `json:"checked_at"`
}

type Clock interface{ Now() time.Time }

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now().UTC() }

type Registry struct {
	mu      sync.RWMutex
	version string
	clock   Clock
	states  map[string]Capability
	live    bool
}

func New(version string, capabilities []string, clock Clock) *Registry {
	if clock == nil {
		clock = RealClock{}
	}
	states := make(map[string]Capability, len(capabilities))
	for _, capability := range capabilities {
		states[capability] = Capability{State: Unavailable, Reason: "starting"}
	}
	return &Registry{version: version, clock: clock, states: states, live: true}
}

// SetLive controls process liveness independently from optional capability readiness.
func (r *Registry) SetLive(live bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.live = live
}

func (r *Registry) Set(capability string, state State, reason string, retryAfter time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := Capability{State: state, Reason: reason}
	if retryAfter > 0 {
		c.RetryAfterMs = uint64(retryAfter / time.Millisecond)
	}
	r.states[capability] = c
}

func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copyStates := make(map[string]Capability, len(r.states))
	for name, state := range r.states {
		copyStates[name] = state
	}
	return Snapshot{Live: r.live, Version: r.version, Capabilities: copyStates, CheckedAt: r.clock.Now()}
}
