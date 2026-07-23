package preview

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
)

type State string

const (
	Registering State = "registering"
	Ready       State = "ready"
	Degraded    State = "degraded"
	Offline     State = "offline"
	Expired     State = "expired"
	Removed     State = "removed"
)

var (
	ErrInvalidTarget     = errors.New("invalid preview target")
	ErrIdentityConflict  = errors.New("preview identity conflict")
	ErrNotFound          = errors.New("preview not found")
	ErrInvalidTransition = errors.New("invalid preview transition")
	ErrResourceLimit     = errors.New("preview resource limit")
)

var transitions = map[State]map[State]bool{
	Registering: {Ready: true, Degraded: true, Offline: true, Removed: true},
	Ready:       {Degraded: true, Offline: true, Expired: true, Removed: true},
	Degraded:    {Ready: true, Offline: true, Expired: true, Removed: true},
	Offline:     {Registering: true, Ready: true, Expired: true, Removed: true},
	Expired:     {Registering: true, Removed: true},
	Removed:     {},
}

type Target struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
}
type Record struct {
	Identity      string    `json:"identity"`
	EnvironmentID string    `json:"environment_id"`
	LogicalName   string    `json:"logical_name"`
	Target        Target    `json:"target"`
	State         State     `json:"state"`
	Reason        string    `json:"reason,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
	Revision      uint64    `json:"revision"`
	PublicURL     string    `json:"public_url,omitempty"`
}

// RegisterCanonical records a server-assigned identity and public URL locally.
// The control plane remains authoritative for identity, route, and lifecycle.
func (r *Registry) RegisterCanonical(identity, publicURL, environmentID, logicalName string, target Target) (Record, error) {
	record, err := r.Register(identity, environmentID, logicalName, target, true)
	if err != nil {
		return Record{}, err
	}
	r.mu.Lock()
	record = r.records[identity]
	record.PublicURL = publicURL
	r.records[identity] = record
	r.mu.Unlock()
	return cloneRecord(record), nil
}

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Prober interface {
	Probe(context.Context, Target) error
}
type Config struct {
	Clock               Clock
	Prober              Prober
	ProbeTimeout        time.Duration
	MaxTargets          int
	MaxConcurrentProbes int
}

type Registry struct {
	mu      sync.RWMutex
	config  Config
	records map[string]Record
	logical map[string]string
	slots   chan struct{}
}

func New(config Config) (*Registry, error) {
	if config.Clock == nil {
		config.Clock = realClock{}
	}
	if config.ProbeTimeout == 0 {
		config.ProbeTimeout = 2 * time.Second
	}
	if config.MaxTargets == 0 {
		config.MaxTargets = helperconfig.DefaultResources.MaxPreviewTargets
	}
	if config.MaxConcurrentProbes == 0 {
		config.MaxConcurrentProbes = helperconfig.DefaultResources.MaxConcurrentProbes
	}
	if config.Prober == nil || config.ProbeTimeout <= 0 || config.MaxTargets < 1 || config.MaxConcurrentProbes < 1 {
		return nil, ErrResourceLimit
	}
	return &Registry{config: config, records: make(map[string]Record), logical: make(map[string]string), slots: make(chan struct{}, config.MaxConcurrentProbes)}, nil
}

func (r *Registry) Register(identity, environmentID, logicalName string, target Target, publicAcknowledged bool) (Record, error) {
	if !validID(identity) || !validID(environmentID) || !validName(logicalName) || !validTarget(target) || !publicAcknowledged {
		return Record{}, ErrInvalidTarget
	}
	key := environmentID + "\x00" + logicalName
	r.mu.Lock()
	defer r.mu.Unlock()
	if existingIdentity, ok := r.logical[key]; ok && existingIdentity != identity {
		return Record{}, ErrIdentityConflict
	}
	if existing, ok := r.records[identity]; ok {
		if existing.EnvironmentID != environmentID || existing.LogicalName != logicalName || existing.State == Removed {
			return Record{}, ErrIdentityConflict
		}
		targetChanged := existing.Target != target
		if !targetChanged && existing.State != Offline && existing.State != Expired {
			return cloneRecord(existing), nil
		}
		existing.Target = target
		existing.Revision++
		existing.UpdatedAt = r.config.Clock.Now()
		if existing.State == Offline || existing.State == Expired {
			existing.State = Registering
			existing.Reason = ""
		} else if targetChanged {
			existing.State = Degraded
			existing.Reason = "target_changed"
		}
		r.records[identity] = existing
		return cloneRecord(existing), nil
	}
	if len(r.records) >= r.config.MaxTargets {
		return Record{}, ErrResourceLimit
	}
	record := Record{Identity: identity, EnvironmentID: environmentID, LogicalName: logicalName, Target: target, State: Registering, UpdatedAt: r.config.Clock.Now(), Revision: 1}
	r.records[identity] = record
	r.logical[key] = identity
	return cloneRecord(record), nil
}

func (r *Registry) List() []Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	records := make([]Record, 0, len(r.records))
	for _, record := range r.records {
		records = append(records, cloneRecord(record))
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Identity < records[j].Identity })
	return records
}

func (r *Registry) ListEnvironment(environmentID string) []Record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	records := make([]Record, 0, len(r.records))
	for _, record := range r.records {
		if record.EnvironmentID == environmentID && record.State != Removed {
			records = append(records, cloneRecord(record))
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Identity < records[j].Identity })
	return records
}

func (r *Registry) ResourceCounts() map[string]uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := uint64(0)
	for _, record := range r.records {
		if record.State != Removed {
			count++
		}
	}
	return map[string]uint64{"previews": count}
}

func (r *Registry) Get(identity string) (Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.records[identity]
	if !ok || record.State == Removed {
		return Record{}, ErrNotFound
	}
	return cloneRecord(record), nil
}

func (r *Registry) Probe(ctx context.Context, identity string) (Record, error) {
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	default:
		return Record{}, ErrResourceLimit
	}
	r.mu.RLock()
	record, ok := r.records[identity]
	r.mu.RUnlock()
	if !ok || record.State == Removed {
		return Record{}, ErrNotFound
	}
	if record.State == Expired {
		return Record{}, ErrInvalidTransition
	}
	probeCtx, cancel := context.WithTimeout(ctx, r.config.ProbeTimeout)
	err := r.config.Prober.Probe(probeCtx, record.Target)
	cancel()
	if ctx.Err() != nil {
		return Record{}, ctx.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.records[identity]
	if !ok || current.State == Removed {
		return Record{}, ErrNotFound
	}
	if current.Revision != record.Revision {
		return cloneRecord(current), nil
	}
	next := Ready
	reason := ""
	if err != nil {
		if current.State == Offline {
			next = Offline
			reason = "helper_offline"
		} else {
			next = Degraded
			reason = "target_unhealthy"
		}
	}
	if !transitions[current.State][next] && current.State != next {
		return Record{}, fmt.Errorf("%s to %s: %w", current.State, next, ErrInvalidTransition)
	}
	current.State = next
	current.Reason = reason
	current.UpdatedAt = r.config.Clock.Now()
	current.Revision++
	r.records[identity] = current
	return cloneRecord(current), nil
}

func (r *Registry) SetOffline(identity string) (Record, error) {
	return r.transition(identity, Offline, "helper_offline")
}
func (r *Registry) Expire(identity string) (Record, error) {
	return r.transition(identity, Expired, "expired")
}
func (r *Registry) Remove(identity string) (Record, error) {
	return r.transition(identity, Removed, "removed")
}
func (r *Registry) transition(identity string, next State, reason string) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.records[identity]
	if !ok || current.State == Removed {
		return Record{}, ErrNotFound
	}
	if !transitions[current.State][next] {
		return Record{}, ErrInvalidTransition
	}
	current.State = next
	current.Reason = reason
	current.UpdatedAt = r.config.Clock.Now()
	current.Revision++
	r.records[identity] = current
	return cloneRecord(current), nil
}

type TCPProber struct{ Dialer net.Dialer }

func (p TCPProber) Probe(ctx context.Context, target Target) error {
	connection, err := p.Dialer.DialContext(ctx, "tcp", net.JoinHostPort(target.Host, fmt.Sprint(target.Port)))
	if err != nil {
		return err
	}
	return connection.Close()
}

func validTarget(target Target) bool {
	return (target.Host == "127.0.0.1" || target.Host == "::1") && target.Port > 0
}
func validID(value string) bool { return len(value) >= 1 && len(value) <= 128 }
func validName(value string) bool {
	if !validID(value) {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}
func cloneRecord(record Record) Record { return record }
