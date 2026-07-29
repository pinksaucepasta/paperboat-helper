package connector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
)

type Transport string

const (
	Auto         Transport = "auto"
	QUIC         Transport = "quic"
	TCPDedicated Transport = "tcp_dedicated"
	TCPMux       Transport = "tcp_mux"
	TCPTLS       Transport = TCPMux
)

var (
	ErrAdmissionInvalid  = errors.New("connector admission invalid")
	ErrAdmissionReplayed = errors.New("connector admission replayed")
	ErrGenerationStale   = errors.New("connector generation stale")
	ErrUnavailable       = errors.New("connector transport unavailable")
	ErrShuttingDown      = errors.New("connector shutting down")
)

type Admission struct {
	OperationID        string
	JTI                string
	Credential         string
	EnvironmentID      string
	HelperID           string
	Generation         uint64
	EdgePool           string
	EdgeNodeID         string
	Endpoint           EdgeEndpoint
	Routes             []RouteHandoff
	ProtocolVersion    string
	ExpiresAt          time.Time
	FileTransferPolicy auth.FileTransferPolicy
}
type EdgeEndpoint struct {
	Host     string `json:"host"`
	Port     uint16 `json:"port"`
	TCPPort  uint16 `json:"tcp_port,omitempty"`
	QUICPort uint16 `json:"quic_port,omitempty"`
}
type RouteTarget struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
}
type RouteHandoff struct {
	RouteID     string      `json:"route_id"`
	Revision    uint64      `json:"route_revision"`
	Kind        string      `json:"kind"`
	PublicHost  string      `json:"public_host"`
	ProxyName   string      `json:"proxy_name"`
	LocalTarget RouteTarget `json:"target"`
}
type Connection interface {
	Retire() error
	Drain(context.Context) error
	Close() error
}
type LifecycleConnection interface {
	Connection
	Done() <-chan error
}
type Dialer interface {
	Dial(context.Context, Transport, Admission) (Connection, error)
}
type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Config struct {
	EnvironmentID     string
	HelperID          string
	EdgePool          string
	Dialer            Dialer
	Clock             Clock
	DrainTimeout      time.Duration
	Transport         Transport
	AdmissionAccepted func(auth.FileTransferPolicy)
}
type Result struct {
	Generation     uint64
	Transport      Transport
	Replaced       bool
	DrainEscalated bool
}
type Status struct {
	Connected  bool
	Generation uint64
	Transport  Transport
	Stopping   bool
}
type activeConnection struct {
	generation uint64
	transport  Transport
	connection Connection
}

type Manager struct {
	opMu     sync.Mutex
	mu       sync.RWMutex
	config   Config
	ctx      context.Context
	cancel   context.CancelFunc
	used     map[string]time.Time
	active   *activeConnection
	retired  map[*activeConnection]struct{}
	stopping bool
}

func New(config Config) (*Manager, error) {
	if config.Dialer == nil || config.EnvironmentID == "" || config.HelperID == "" || config.EdgePool == "" {
		return nil, ErrAdmissionInvalid
	}
	if config.Clock == nil {
		config.Clock = realClock{}
	}
	if config.DrainTimeout == 0 {
		config.DrainTimeout = 10 * time.Second
	}
	if config.Transport == "" {
		config.Transport = Auto
	}
	if config.DrainTimeout <= 0 || config.Transport != Auto && config.Transport != QUIC && config.Transport != TCPDedicated && config.Transport != TCPMux {
		return nil, ErrAdmissionInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{config: config, ctx: ctx, cancel: cancel, used: make(map[string]time.Time), retired: make(map[*activeConnection]struct{})}, nil
}

func (m *Manager) Accept(ctx context.Context, admission Admission) (Result, error) {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.RLock()
	stopping := m.stopping
	currentGeneration := uint64(0)
	if m.active != nil {
		currentGeneration = m.active.generation
	}
	m.mu.RUnlock()
	if stopping {
		return Result{}, ErrShuttingDown
	}
	now := m.config.Clock.Now()
	for jti, expiry := range m.used {
		if !expiry.After(now) {
			delete(m.used, jti)
		}
	}
	if len(admission.OperationID) < 8 || len(admission.OperationID) > 128 || admission.JTI == "" || len(admission.Credential) < 32 || len(admission.Credential) > 8192 || admission.EnvironmentID != m.config.EnvironmentID || admission.HelperID != m.config.HelperID || admission.EdgePool != m.config.EdgePool || !identifierPattern.MatchString(admission.EdgeNodeID) || admission.ProtocolVersion != "1.0" || admission.Generation == 0 || !admission.ExpiresAt.After(now) || !validEndpoint(admission.Endpoint) || !validRoutes(admission.Routes) {
		return Result{}, ErrAdmissionInvalid
	}
	if _, used := m.used[admission.JTI]; used {
		return Result{}, ErrAdmissionReplayed
	}
	if admission.Generation < currentGeneration {
		return Result{}, ErrGenerationStale
	}
	m.used[admission.JTI] = admission.ExpiresAt
	dialCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.ctx, cancel)
	connection, transport, err := m.dial(dialCtx, admission)
	stop()
	cancel()
	if err != nil {
		return Result{}, err
	}
	if m.ctx.Err() != nil {
		_ = connection.Close()
		return Result{}, ErrShuttingDown
	}
	m.mu.Lock()
	old := m.active
	m.active = &activeConnection{generation: admission.Generation, transport: transport, connection: connection}
	if old != nil {
		m.retired[old] = struct{}{}
	}
	m.mu.Unlock()
	if m.config.AdmissionAccepted != nil {
		m.config.AdmissionAccepted(admission.FileTransferPolicy)
	}
	result := Result{Generation: admission.Generation, Transport: transport, Replaced: old != nil}
	if old != nil {
		if err := old.connection.Retire(); err != nil {
			_ = old.connection.Close()
		}
		go m.waitRetired(old)
	}
	return result, nil
}

func (m *Manager) waitRetired(retired *activeConnection) {
	connection, ok := retired.connection.(LifecycleConnection)
	if !ok {
		m.mu.Lock()
		delete(m.retired, retired)
		m.mu.Unlock()
		_ = retired.connection.Close()
		return
	}
	select {
	case <-connection.Done():
	case <-m.ctx.Done():
		return
	}
	m.mu.Lock()
	if _, ok := m.retired[retired]; ok {
		delete(m.retired, retired)
		m.mu.Unlock()
		_ = connection.Close()
		return
	}
	m.mu.Unlock()
}

func (m *Manager) dial(ctx context.Context, admission Admission) (Connection, Transport, error) {
	transport := m.config.Transport
	connection, err := m.config.Dialer.Dial(ctx, transport, admission)
	if err == nil && connection != nil {
		if selected, ok := connection.(interface{ SelectedTransport() Transport }); ok && selected.SelectedTransport() != "" {
			transport = selected.SelectedTransport()
		}
		return connection, transport, nil
	}
	if connection != nil {
		_ = connection.Close()
	}
	return nil, "", fmt.Errorf("%w: %s: %w", ErrUnavailable, transport, err)
}

func (m *Manager) Status() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	status := Status{Stopping: m.stopping}
	if m.active != nil {
		status.Connected = true
		status.Generation = m.active.generation
		status.Transport = m.active.transport
	}
	return status
}

func (m *Manager) ResourceCounts() map[string]uint64 {
	m.mu.RLock()
	count := uint64(len(m.retired))
	if m.active != nil {
		count++
	}
	m.mu.RUnlock()
	return map[string]uint64{"connectors": count}
}

// WaitDisconnected waits for the currently selected generation to terminate.
// A newer replacement makes the older waiter return without clearing it.
func (m *Manager) WaitDisconnected(ctx context.Context, generation uint64) error {
	m.mu.RLock()
	active := m.active
	m.mu.RUnlock()
	if active == nil || active.generation != generation {
		return ErrGenerationStale
	}
	connection, ok := active.connection.(LifecycleConnection)
	if !ok {
		return ErrUnavailable
	}
	select {
	case err, open := <-connection.Done():
		if !open {
			err = nil
		}
		m.mu.Lock()
		if m.active == active {
			m.active = nil
		}
		m.mu.Unlock()
		_ = connection.Close()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if m.stopping {
		m.mu.Unlock()
		return nil
	}
	m.stopping = true
	m.cancel()
	m.mu.Unlock()
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.mu.Lock()
	active := m.active
	m.active = nil
	connections := make([]*activeConnection, 0, len(m.retired)+1)
	if active != nil {
		connections = append(connections, active)
	}
	for retired := range m.retired {
		connections = append(connections, retired)
	}
	m.retired = make(map[*activeConnection]struct{})
	m.mu.Unlock()
	if len(connections) == 0 {
		return nil
	}
	done := make(chan bool, len(connections))
	for _, connection := range connections {
		go func() { done <- drainAndClose(connection.connection, m.config.DrainTimeout) }()
	}
	escalated := false
	for range connections {
		select {
		case failed := <-done:
			escalated = escalated || failed
		case <-ctx.Done():
			for _, connection := range connections {
				_ = connection.connection.Close()
			}
			return ctx.Err()
		}
	}
	if escalated {
		return ErrUnavailable
	}
	return nil
}

func drainAndClose(connection Connection, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	err := connection.Drain(ctx)
	cancel()
	closeErr := connection.Close()
	return err != nil || closeErr != nil
}
