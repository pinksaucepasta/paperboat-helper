package session

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/activity"
	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/history"
	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

var (
	ErrSessionExists  = errors.New("session name already exists")
	ErrSessionUnknown = errors.New("session not found")
	ErrSessionRunning = errors.New("session is running")
	ErrInvalidSession = errors.New("invalid session")
	ErrManagerStopped = errors.New("session manager stopped")
	ErrResourceLimit  = errors.New("session resource limit")
)

// initialReplayBytes bounds attach replay to a terminal-sized recent tail while
// reserving attachment queue capacity for output that arrives during setup.
const initialReplayBytes uint64 = 64 << 10

const outputPersistDebounce = 20 * time.Millisecond

type PTYProcess interface {
	io.Reader
	InputWriter
	Resize(pty.Dimensions) error
	Signal(pty.Signal) error
	Wait(context.Context) (pty.ExitResult, error)
	Terminate(context.Context, time.Duration) (pty.ExitResult, error)
	CloseIO() error
}

type Launcher func(pty.Command) (PTYProcess, error)

type ManagerConfig struct {
	Launch             Launcher
	Random             io.Reader
	HistoryBytes       uint64
	AttachmentBytes    uint64
	TerminationTimeout time.Duration
	TerminationGrace   time.Duration
	MaxSessions        int
	Store              *store.Store
	Activity           *activity.Collector
	EnvironmentID      string
	MaxPendingActivity int
	MaxAttachments     int
	MaxInputDecisions  int
	RecoveryExitSignal string
	Metrics            interface {
		Record(string, float64, map[string]string) error
	}
}

type Manager struct {
	mu       sync.RWMutex
	config   ManagerConfig
	sessions map[string]*managedSession
	names    map[string]string
	stopping bool
}

type managedSession struct {
	opMu            sync.Mutex
	id              string
	name            string
	command         pty.Command
	lifecycle       *Lifecycle
	history         *history.History
	fanout          *Fanout
	inputs          *InputJournal
	process         PTYProcess
	exit            *pty.ExitResult
	resizeID        string
	resizeTime      time.Time
	activitySeq     map[string]uint64
	pendingActivity []activity.Event
	persistNotify   chan struct{}
	persistStop     chan struct{}
	persistDone     chan error
	persistErr      error
}

type CreateRequest struct {
	ID      string
	Name    string
	Command pty.Command
}

type Snapshot struct {
	ID               string          `json:"id"`
	Name             string          `json:"name"`
	CWD              string          `json:"cwd"`
	Dimensions       pty.Dimensions  `json:"dimensions"`
	State            State           `json:"state"`
	Generation       uint64          `json:"generation"`
	EarliestSequence uint64          `json:"earliest_sequence"`
	LatestSequence   uint64          `json:"latest_sequence"`
	Exit             *pty.ExitResult `json:"exit,omitempty"`
}

type AttachResult struct {
	Snapshot Snapshot       `json:"snapshot"`
	Replay   history.Replay `json:"replay"`
}

func (m *Manager) ResourceCounts() map[string]uint64 {
	m.mu.RLock()
	sessions := make([]*managedSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.mu.RUnlock()
	counts := map[string]uint64{"sessions": uint64(len(sessions))}
	for _, session := range sessions {
		session.opMu.Lock()
		counts["attachments"] += uint64(session.fanout.Count())
		if session.process != nil {
			counts["processes"]++
		}
		session.opMu.Unlock()
	}
	return counts
}

func NewManager(config ManagerConfig) (*Manager, error) {
	if config.Launch == nil {
		return nil, ErrInvalidSession
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.HistoryBytes == 0 {
		config.HistoryBytes = helperconfig.DefaultResources.HistoryBytes
	}
	if config.AttachmentBytes == 0 {
		config.AttachmentBytes = 1 << 20
	}
	if config.TerminationTimeout == 0 {
		config.TerminationTimeout = 10 * time.Second
	}
	if config.TerminationGrace == 0 {
		config.TerminationGrace = 2 * time.Second
	}
	if config.MaxSessions == 0 {
		config.MaxSessions = helperconfig.DefaultResources.MaxSessions
	}
	if config.MaxPendingActivity == 0 {
		config.MaxPendingActivity = helperconfig.DefaultResources.MaxActivityEvents
	}
	if config.MaxAttachments == 0 {
		config.MaxAttachments = helperconfig.DefaultResources.MaxAttachments
	}
	if config.MaxInputDecisions == 0 {
		config.MaxInputDecisions = helperconfig.DefaultResources.MaxInputDecisions
	}
	if config.RecoveryExitSignal == "" {
		config.RecoveryExitSignal = "helper_restart"
	}
	if config.HistoryBytes < 1 || config.AttachmentBytes < 1 || config.TerminationTimeout <= 0 || config.TerminationGrace < 0 || config.TerminationGrace > config.TerminationTimeout || config.MaxSessions < 1 || config.MaxPendingActivity < 1 || config.MaxAttachments < 1 || config.MaxInputDecisions < 1 || config.Activity != nil && config.EnvironmentID == "" || config.RecoveryExitSignal != "helper_restart" && config.RecoveryExitSignal != "machine_reboot" {
		return nil, ErrInvalidSession
	}
	manager := &Manager{config: config, sessions: make(map[string]*managedSession), names: make(map[string]string)}
	if config.Store != nil {
		if err := manager.recover(context.Background()); err != nil {
			return nil, err
		}
	}
	return manager, nil
}

func (m *Manager) Create(ctx context.Context, request CreateRequest) (Snapshot, error) {
	if !validSessionName(request.Name) {
		return Snapshot{}, ErrInvalidSession
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopping {
		return Snapshot{}, ErrManagerStopped
	}
	if len(m.sessions) >= m.config.MaxSessions {
		return Snapshot{}, ErrResourceLimit
	}
	if _, exists := m.names[request.Name]; exists {
		return Snapshot{}, ErrSessionExists
	}
	id := request.ID
	if id != "" {
		if _, exists := m.sessions[id]; exists {
			return Snapshot{}, ErrSessionExists
		}
	}
	for attempt := 0; attempt < 8; attempt++ {
		if id != "" {
			break
		}
		candidate, err := randomID(m.config.Random)
		if err != nil {
			return Snapshot{}, err
		}
		if _, exists := m.sessions[candidate]; !exists {
			id = candidate
			break
		}
	}
	if id == "" {
		return Snapshot{}, ErrSessionExists
	}
	retained, _ := history.New(m.config.HistoryBytes)
	session := &managedSession{id: id, name: request.Name, command: request.Command, lifecycle: NewLifecycle(), history: retained, fanout: NewFanout(), activitySeq: make(map[string]uint64)}
	if m.config.Store != nil {
		if err := m.config.Store.CreateSession(ctx, store.Session{ID: id, Name: request.Name, CWD: request.Command.CWD, CommandPath: request.Command.Path, CommandArgs: request.Command.Args, CommandEnv: request.Command.Env, Columns: request.Command.Dimensions.Columns, Rows: request.Command.Dimensions.Rows, State: string(Creating), Generation: 0}); err != nil {
			return Snapshot{}, err
		}
	}
	process, err := m.config.Launch(request.Command)
	if err != nil {
		_ = session.lifecycle.Transition(Closed)
		m.discardFailedCreate(ctx, session, Creating)
		return Snapshot{}, err
	}
	if err := session.lifecycle.Transition(Running); err != nil {
		_ = process.CloseIO()
		return Snapshot{}, err
	}
	_, generation := session.lifecycle.Snapshot()
	if err := m.persist(ctx, session, Creating); err != nil {
		terminateCtx, cancel := context.WithTimeout(context.Background(), m.config.TerminationTimeout)
		_, _ = process.Terminate(terminateCtx, 0)
		cancel()
		m.discardFailedCreate(context.Background(), session, Creating)
		return Snapshot{}, err
	}
	session.inputs = NewBoundedInputJournal(generation, m.config.MaxInputDecisions)
	session.process = process
	m.startOutputPersistence(session)
	m.sessions[id] = session
	m.names[request.Name] = id
	snapshot := session.snapshotLocked()
	go m.capture(session, process)
	return snapshot, nil
}

func (m *Manager) Attach(sessionID, attachmentID string, fromSequence uint64) (AttachResult, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return AttachResult{}, err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	return m.attachLocked(session, attachmentID, fromSequence)
}

// AttachLive resolves the current output boundary and registers the attachment
// while holding the same session lock. High-output terminals therefore cannot
// compact past a boundary observed by a separate snapshot request.
func (m *Manager) AttachLive(sessionID, attachmentID string) (AttachResult, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return AttachResult{}, err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	_, latest, _ := session.history.Bounds()
	return m.attachLocked(session, attachmentID, latest)
}

func (m *Manager) attachLocked(session *managedSession, attachmentID string, fromSequence uint64) (AttachResult, error) {
	if session.fanout.Count() >= m.config.MaxAttachments {
		return AttachResult{}, ErrResourceLimit
	}
	replayLimit := attachmentReplayLimit(m.config.AttachmentBytes)
	replay, err := session.history.Replay(fromSequence, replayLimit)
	if err != nil {
		return AttachResult{}, err
	}
	// A durable session may retain more history than a live attachment can
	// queue. Start at the newest bounded tail so reconnect always succeeds and
	// presents the most recent output instead of repeatedly racing replay gaps.
	if replay.ToSequence < replay.LatestSequence {
		boundary := replay.LatestSequence - replayLimit
		if boundary < replay.EarliestSequence {
			boundary = replay.EarliestSequence
		}
		fromSequence = boundary
		replay, err = session.history.Replay(fromSequence, replayLimit)
		if err != nil {
			return AttachResult{}, err
		}
	}
	if err := session.fanout.Attach(attachmentID, m.config.AttachmentBytes); err != nil {
		return AttachResult{}, err
	}
	for _, event := range replay.Events {
		if eviction, err := session.fanout.Enqueue(attachmentID, event); err != nil {
			return AttachResult{}, err
		} else if eviction != nil {
			return AttachResult{}, ErrAttachmentEvicted
		}
	}
	return AttachResult{Snapshot: session.snapshotLocked(), Replay: replay}, nil
}

func attachmentReplayLimit(attachmentBytes uint64) uint64 {
	limit := attachmentBytes / 2
	if limit == 0 {
		limit = 1
	}
	if limit > initialReplayBytes {
		return initialReplayBytes
	}
	return limit
}

func (m *Manager) Detach(sessionID, attachmentID string) error {
	session, err := m.get(sessionID)
	if err != nil {
		return err
	}
	return session.fanout.Detach(attachmentID)
}

func (m *Manager) Next(sessionID, attachmentID string) (history.Event, bool, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return history.Event{}, false, err
	}
	return session.fanout.Next(attachmentID)
}

func (m *Manager) WaitNext(ctx context.Context, sessionID, attachmentID string) (history.Event, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return history.Event{}, err
	}
	return session.fanout.WaitNext(ctx, attachmentID)
}

func (m *Manager) Acknowledge(sessionID, attachmentID string, nextSequence uint64) error {
	session, err := m.get(sessionID)
	if err != nil {
		return err
	}
	state, _, err := session.fanout.Status(attachmentID)
	if err != nil || state != Attached {
		return ErrInvalidInput
	}
	return session.history.Acknowledge(attachmentID, nextSequence)
}

func (m *Manager) AttachmentStatus(sessionID, attachmentID string) (AttachmentState, uint64, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return "", 0, err
	}
	return session.fanout.Status(attachmentID)
}

func (m *Manager) Write(sessionID string, key InputKey, data []byte) (InputDecision, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return InputDecision{}, err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	state, generation := session.lifecycle.Snapshot()
	if state != Running || session.process == nil || key.Generation != generation {
		return InputDecision{}, &StaleGenerationError{CurrentGeneration: generation}
	}
	if attachmentState, _, err := session.fanout.Status(key.AttachmentID); err != nil || attachmentState != Attached {
		return InputDecision{}, ErrInvalidInput
	}
	if m.config.Activity != nil {
		m.flushActivityLocked(session)
		if len(session.pendingActivity) >= m.config.MaxPendingActivity {
			return InputDecision{}, activity.ErrQueueFull
		}
	}
	if _, queryErr := session.inputs.Query(key); queryErr == nil {
		return session.inputs.Write(key, data, session.process)
	} else if !errors.Is(queryErr, ErrInputUnknown) {
		return InputDecision{}, queryErr
	}
	if err := session.inputs.Admit(key); err != nil {
		return InputDecision{}, err
	}
	hash := sha256.Sum256(data)
	if m.config.Store != nil {
		inserted, err := m.config.Store.PutInputDecision(context.Background(), store.InputDecision{SessionID: sessionID, ClientID: key.ClientID, AttachmentID: key.AttachmentID, Generation: key.Generation, InputID: key.InputID, Hash: hash[:], Status: string(InputUncertain), CreatedAt: time.Now().UTC()})
		if err != nil {
			return InputDecision{}, err
		}
		if !inserted {
			return InputDecision{}, ErrInputConflict
		}
	}
	decision, err := session.inputs.Write(key, data, session.process)
	if err != nil {
		return decision, err
	}
	if m.config.Store != nil {
		persisted := store.InputDecision{SessionID: sessionID, ClientID: key.ClientID, AttachmentID: key.AttachmentID, Generation: key.Generation, InputID: key.InputID, Hash: hash[:], Status: string(decision.Status), BytesWritten: decision.BytesWritten, ErrorCode: decision.WriteError}
		if persistErr := m.config.Store.UpdateInputDecision(context.Background(), persisted); persistErr != nil {
			decision.Status = InputUncertain
			decision.WriteError = "decision_persist_failed"
			return decision, persistErr
		}
	}
	if decision.Status == InputAccepted || decision.Status == InputUncertain && decision.BytesWritten > 0 {
		_ = m.recordInputActivityLocked(session, key, time.Now().UTC())
	}
	return decision, nil
}

// WriteStream writes ordered live terminal input without creating an
// idempotency decision. Stream input is never replayed after disconnection.
func (m *Manager) WriteStream(sessionID, attachmentID string, generation uint64, data []byte) error {
	if len(data) == 0 || len(data) > 256<<10 {
		return ErrInvalidInput
	}
	session, err := m.get(sessionID)
	if err != nil {
		return err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	state, currentGeneration := session.lifecycle.Snapshot()
	if state != Running || session.process == nil || generation != currentGeneration {
		return &StaleGenerationError{CurrentGeneration: currentGeneration}
	}
	if attachmentState, _, err := session.fanout.Status(attachmentID); err != nil || attachmentState != Attached {
		return ErrInvalidInput
	}
	n, writeErr := session.process.Write(data)
	if writeErr != nil {
		return writeErr
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	if m.config.Activity != nil {
		m.flushActivityLocked(session)
		if len(session.pendingActivity) >= m.config.MaxPendingActivity {
			return activity.ErrQueueFull
		}
		_ = m.recordInputActivityLocked(session, InputKey{AttachmentID: attachmentID, Generation: generation, InputID: "stream"}, time.Now().UTC())
	}
	return nil
}

func (m *Manager) QueryInput(sessionID string, key InputKey) (InputDecision, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return InputDecision{}, err
	}
	return session.inputs.Query(key)
}

func (m *Manager) Resize(sessionID, attachmentID string, dimensions pty.Dimensions, activeAt time.Time) error {
	session, err := m.get(sessionID)
	if err != nil {
		return err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	attachmentState, _, statusErr := session.fanout.Status(attachmentID)
	if statusErr != nil || attachmentState != Attached || session.process == nil {
		return ErrInvalidInput
	}
	if activeAt.Before(session.resizeTime) || activeAt.Equal(session.resizeTime) && attachmentID < session.resizeID {
		return nil
	}
	if err := session.process.Resize(dimensions); err != nil {
		return err
	}
	session.resizeID, session.resizeTime = attachmentID, activeAt
	session.command.Dimensions = dimensions
	lifecycleState, _ := session.lifecycle.Snapshot()
	if err := m.persist(context.Background(), session, lifecycleState); err != nil {
		return err
	}
	return nil
}

func (m *Manager) Signal(sessionID string, generation uint64, signal pty.Signal) error {
	session, err := m.get(sessionID)
	if err != nil {
		return err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	state, current := session.lifecycle.Snapshot()
	if state != Running || generation != current || session.process == nil {
		return ErrStaleGeneration
	}
	return session.process.Signal(signal)
}

func (m *Manager) Clear(sessionID string) (uint64, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return 0, err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	_, latest, _ := session.history.Bounds()
	if m.config.Store != nil {
		next := session.storeRecordLocked()
		next.EarliestSequence, next.LatestSequence = latest, latest
		state, _ := session.lifecycle.Snapshot()
		if err := m.config.Store.UpdateSession(context.Background(), session.id, string(state), next); err != nil {
			return 0, err
		}
	}
	return session.history.Clear(), nil
}

func (m *Manager) Close(ctx context.Context, sessionID string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	session, err := m.get(sessionID)
	if err != nil {
		return Snapshot{}, err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	state, _ := session.lifecycle.Snapshot()
	if state == Closed {
		return session.snapshotLocked(), nil
	}
	if state == Exited {
		if err := session.lifecycle.Transition(Closing); err != nil {
			return Snapshot{}, err
		}
		if err := session.lifecycle.Transition(Closed); err != nil {
			return Snapshot{}, err
		}
		if err := m.persist(ctx, session, Exited); err != nil {
			return Snapshot{}, err
		}
		return session.snapshotLocked(), nil
	}
	if state != Running && state != Closing || session.process == nil {
		return Snapshot{}, ErrSessionRunning
	}
	if state == Running {
		if err := session.lifecycle.Transition(Closing); err != nil {
			return Snapshot{}, err
		}
		if err := m.persist(ctx, session, Running); err != nil {
			terminateCtx, cancel := context.WithTimeout(context.Background(), m.config.TerminationTimeout)
			result, _ := session.process.Terminate(terminateCtx, 0)
			cancel()
			session.exit, session.process = &result, nil
			_ = session.lifecycle.Transition(Closed)
			return Snapshot{}, err
		}
	}
	terminateCtx, cancel := context.WithTimeout(ctx, m.config.TerminationTimeout)
	result, terminateErr := session.process.Terminate(terminateCtx, m.config.TerminationGrace)
	cancel()
	if terminateErr != nil && (errors.Is(terminateErr, context.Canceled) || errors.Is(terminateErr, context.DeadlineExceeded)) {
		process := session.process
		go m.finishClosing(session, process)
		return session.snapshotLocked(), terminateErr
	}
	session.exit = &result
	session.process = nil
	if err := session.lifecycle.Transition(Closed); err != nil {
		return Snapshot{}, errors.Join(terminateErr, err)
	}
	if err := m.persist(ctx, session, Closing); err != nil {
		return Snapshot{}, errors.Join(terminateErr, err)
	}
	return session.snapshotLocked(), terminateErr
}

func (m *Manager) finishClosing(session *managedSession, process PTYProcess) {
	session.opMu.Lock()
	defer session.opMu.Unlock()
	state, _ := session.lifecycle.Snapshot()
	if state != Closing || session.process != process {
		return
	}
	terminateCtx, cancel := context.WithTimeout(context.Background(), m.config.TerminationTimeout)
	result, err := process.Terminate(terminateCtx, m.config.TerminationGrace)
	cancel()
	if err != nil {
		return
	}
	session.exit = &result
	session.process = nil
	if session.lifecycle.Transition(Closed) != nil {
		return
	}
	_ = m.persist(context.Background(), session, Closing)
}

func (m *Manager) Restart(sessionID string) (Snapshot, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return Snapshot{}, err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	state, _ := session.lifecycle.Snapshot()
	if state != Exited && state != Closed {
		return Snapshot{}, ErrSessionRunning
	}
	if err := session.lifecycle.Transition(Restarting); err != nil {
		return Snapshot{}, err
	}
	if err := m.persist(context.Background(), session, state); err != nil {
		_ = session.lifecycle.Transition(Closed)
		return Snapshot{}, err
	}
	process, err := m.config.Launch(session.command)
	if err != nil {
		_ = session.lifecycle.Transition(Closed)
		_ = m.persist(context.Background(), session, Restarting)
		return Snapshot{}, err
	}
	if err := session.lifecycle.Transition(Running); err != nil {
		_ = process.CloseIO()
		return Snapshot{}, err
	}
	_, generation := session.lifecycle.Snapshot()
	session.inputs.SetGeneration(generation)
	session.process, session.exit = process, nil
	m.startOutputPersistence(session)
	if err := m.persist(context.Background(), session, Restarting); err != nil {
		terminateCtx, cancel := context.WithTimeout(context.Background(), m.config.TerminationTimeout)
		_, _ = process.Terminate(terminateCtx, 0)
		cancel()
		session.process = nil
		_ = session.lifecycle.Transition(Exited)
		return Snapshot{}, err
	}
	go m.capture(session, process)
	return session.snapshotLocked(), nil
}

func (m *Manager) Delete(sessionID string) (resultErr error) {
	defer func() {
		if m.config.Metrics == nil {
			return
		}
		result := "removed"
		if errors.Is(resultErr, ErrSessionRunning) {
			result = "preserved"
		} else if resultErr != nil {
			result = "failed"
		}
		_ = m.config.Metrics.Record("paperboat_helper_cleanup_total", 1, map[string]string{"kind": "session", "result": result})
	}()
	session, err := m.get(sessionID)
	if err != nil {
		return err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	state, _ := session.lifecycle.Snapshot()
	if state != Exited && state != Closed {
		return ErrSessionRunning
	}
	if m.config.Store != nil {
		if err := m.config.Store.DeleteSession(context.Background(), sessionID); err != nil {
			return err
		}
	}
	if err := session.lifecycle.Transition(Deleted); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.sessions, sessionID)
	delete(m.names, session.name)
	m.mu.Unlock()
	return nil
}

func (m *Manager) Snapshot(sessionID string) (Snapshot, error) {
	session, err := m.get(sessionID)
	if err != nil {
		return Snapshot{}, err
	}
	session.opMu.Lock()
	defer session.opMu.Unlock()
	return session.snapshotLocked(), nil
}

func (m *Manager) List() []Snapshot {
	m.mu.RLock()
	sessions := make([]*managedSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.mu.RUnlock()
	snapshots := make([]Snapshot, 0, len(sessions))
	for _, session := range sessions {
		session.opMu.Lock()
		snapshots = append(snapshots, session.snapshotLocked())
		session.opMu.Unlock()
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Name < snapshots[j].Name })
	return snapshots
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if m.stopping {
		m.mu.Unlock()
		return nil
	}
	m.stopping = true
	ids := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	results := make(chan error, len(ids))
	for _, id := range ids {
		go func(sessionID string) {
			_, err := m.Close(ctx, sessionID)
			results <- err
		}(id)
	}
	var result error
	for range ids {
		select {
		case err := <-results:
			if err != nil && !errors.Is(err, ErrSessionRunning) {
				result = errors.Join(result, err)
			}
		case <-ctx.Done():
			return errors.Join(result, ctx.Err())
		}
	}
	return result
}

// ShutdownForRecovery stops live PTYs and flushes their history while leaving
// their durable generations recoverable by the next worker process.
func (m *Manager) ShutdownForRecovery(ctx context.Context) error {
	m.mu.RLock()
	sessions := make(map[string]*managedSession, len(m.sessions))
	for id, session := range m.sessions {
		sessions[id] = session
	}
	m.mu.RUnlock()
	recoverable := make(map[string]State)
	for id, session := range sessions {
		session.opMu.Lock()
		state, _ := session.lifecycle.Snapshot()
		if state == Running || state == Restarting {
			recoverable[id] = state
		}
		session.opMu.Unlock()
	}

	result := m.Shutdown(ctx)
	if m.config.Store == nil {
		return result
	}
	restoreCtx, cancel := context.WithTimeout(context.Background(), m.config.TerminationTimeout)
	defer cancel()
	for id, original := range recoverable {
		session, err := m.get(id)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		session.opMu.Lock()
		state, _ := session.lifecycle.Snapshot()
		if state != Closed {
			session.opMu.Unlock()
			result = errors.Join(result, ErrInvalidTransition)
			continue
		}
		record := session.storeRecordLocked()
		record.State = string(original)
		record.ExitCode, record.ExitSignal, record.ExitedAt = nil, "", nil
		err = m.config.Store.UpdateSession(restoreCtx, id, string(Closed), record)
		session.opMu.Unlock()
		result = errors.Join(result, err)
	}
	return result
}

func (m *Manager) capture(session *managedSession, process PTYProcess) {
	buffer := make([]byte, 32<<10)
	for {
		n, err := process.Read(buffer)
		if n > 0 {
			session.opMu.Lock()
			event, appendErr := session.history.Append(1, buffer[:n])
			if appendErr == nil {
				_, _ = session.fanout.Publish(event)
				m.queueOutputPersistenceLocked(session)
			}
			session.opMu.Unlock()
		}
		if err != nil {
			break
		}
	}
	persistErr := m.stopOutputPersistence(session)
	result, _ := process.Wait(context.Background())
	_ = process.CloseIO()
	session.opMu.Lock()
	defer session.opMu.Unlock()
	if session.process != process {
		return
	}
	session.persistErr = persistErr
	if persistErr != nil && m.config.Metrics != nil {
		_ = m.config.Metrics.Record("paperboat_helper_terminal_persistence_failures_total", 1, map[string]string{"session_id": session.id})
	}
	state, _ := session.lifecycle.Snapshot()
	if state == Running {
		_ = session.lifecycle.Transition(Exited)
		session.exit = &result
		session.process = nil
		_ = m.persist(context.Background(), session, Running)
	}
}

func (m *Manager) startOutputPersistence(session *managedSession) {
	if m.config.Store == nil {
		return
	}
	session.persistNotify = make(chan struct{}, 1)
	session.persistStop = make(chan struct{})
	session.persistDone = make(chan error, 1)
	session.persistErr = nil
	go m.runOutputPersistence(session, session.persistNotify, session.persistStop, session.persistDone)
}

func (m *Manager) queueOutputPersistenceLocked(session *managedSession) {
	if session.persistNotify == nil {
		return
	}
	select {
	case session.persistNotify <- struct{}{}:
	default:
	}
}

func (m *Manager) stopOutputPersistence(session *managedSession) error {
	if session.persistStop == nil {
		return nil
	}
	close(session.persistStop)
	err := <-session.persistDone
	session.persistNotify, session.persistStop, session.persistDone = nil, nil, nil
	return err
}

func (m *Manager) runOutputPersistence(session *managedSession, notify, stop <-chan struct{}, done chan<- error) {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	dirty := false
	flush := func() error {
		session.opMu.Lock()
		earliest, latest, _ := session.history.Bounds()
		replay, err := session.history.Replay(earliest, 0)
		session.opMu.Unlock()
		if err != nil {
			return err
		}
		events := make([]store.OutputEvent, len(replay.Events))
		for i, event := range replay.Events {
			events[i] = store.OutputEvent{Channel: event.Channel, StartSequence: event.StartSequence, EndSequence: event.EndSequence, Data: event.Data}
		}
		return m.config.Store.ReplaceOutput(context.Background(), session.id, earliest, latest, events)
	}
	for {
		select {
		case <-notify:
			if !dirty {
				dirty = true
				timer.Reset(outputPersistDebounce)
			}
		case <-timer.C:
			if dirty {
				if err := flush(); err != nil {
					done <- err
					return
				}
				dirty = false
			}
		case <-stop:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if dirty {
				done <- flush()
			} else {
				done <- nil
			}
			return
		}
	}
}

func (m *Manager) get(sessionID string) (*managedSession, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	session, ok := m.sessions[sessionID]
	if !ok {
		return nil, ErrSessionUnknown
	}
	return session, nil
}

func (s *managedSession) snapshotLocked() Snapshot {
	state, generation := s.lifecycle.Snapshot()
	earliest, latest, _ := s.history.Bounds()
	snapshot := Snapshot{ID: s.id, Name: s.name, CWD: s.command.CWD, Dimensions: s.command.Dimensions, State: state, Generation: generation, EarliestSequence: earliest, LatestSequence: latest}
	if s.exit != nil {
		exit := *s.exit
		snapshot.Exit = &exit
	}
	return snapshot
}

func (s *managedSession) storeRecordLocked() store.Session {
	snapshot := s.snapshotLocked()
	record := store.Session{ID: snapshot.ID, Name: snapshot.Name, CWD: snapshot.CWD, CommandPath: s.command.Path, CommandArgs: append([]string(nil), s.command.Args...), CommandEnv: append([]string(nil), s.command.Env...), Columns: snapshot.Dimensions.Columns, Rows: snapshot.Dimensions.Rows, State: string(snapshot.State), Generation: snapshot.Generation, EarliestSequence: snapshot.EarliestSequence, LatestSequence: snapshot.LatestSequence}
	if snapshot.Exit != nil {
		code := snapshot.Exit.Code
		record.ExitCode, record.ExitSignal = &code, snapshot.Exit.Signal
		exitedAt := snapshot.Exit.ExitedAt
		record.ExitedAt = &exitedAt
	}
	return record
}

func (m *Manager) persist(ctx context.Context, session *managedSession, expected State) error {
	if m.config.Store == nil {
		return nil
	}
	return m.config.Store.UpdateSession(ctx, session.id, string(expected), session.storeRecordLocked())
}

func (m *Manager) discardFailedCreate(ctx context.Context, session *managedSession, expected State) {
	if m.config.Store == nil {
		return
	}
	_ = m.config.Store.UpdateSession(ctx, session.id, string(expected), session.storeRecordLocked())
	_ = m.config.Store.DeleteSession(ctx, session.id)
}

func (m *Manager) recover(ctx context.Context) error {
	records, err := m.config.Store.Sessions(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		original := State(record.State)
		recovered := original
		now := time.Now().UTC()
		switch original {
		case Creating:
			recovered = Closed
		case Running, Restarting:
			recovered = Exited
			code := 255
			record.ExitCode, record.ExitSignal, record.ExitedAt = &code, m.config.RecoveryExitSignal, &now
		case Closing:
			recovered = Closed
		case Exited, Closed:
		default:
			return ErrInvalidSession
		}
		if recovered != original {
			record.State = string(recovered)
			if err := m.config.Store.UpdateSession(ctx, record.ID, string(original), record); err != nil {
				return err
			}
		}
		earliestSequence, err := m.config.Store.TrimOutput(ctx, record.ID, m.config.HistoryBytes)
		if err != nil {
			return err
		}
		storedEvents, earliest, latest, err := m.config.Store.Replay(ctx, record.ID, earliestSequence, 0)
		if err != nil {
			return err
		}
		events := make([]history.Event, len(storedEvents))
		for i, event := range storedEvents {
			events[i] = history.Event{Channel: event.Channel, StartSequence: event.StartSequence, EndSequence: event.EndSequence, Data: event.Data}
		}
		retained, err := history.Restore(m.config.HistoryBytes, earliest, latest, events)
		if err != nil {
			return err
		}
		lifecycle, err := RecoverLifecycle(recovered, record.Generation)
		if err != nil {
			return err
		}
		inputJournal := NewBoundedInputJournal(record.Generation, m.config.MaxInputDecisions)
		decisions, err := m.config.Store.InputDecisions(ctx, record.ID)
		if err != nil {
			return err
		}
		for _, decision := range decisions {
			key := InputKey{ClientID: decision.ClientID, AttachmentID: decision.AttachmentID, Generation: decision.Generation, InputID: decision.InputID}
			if err := inputJournal.Restore(key, decision.Hash, InputStatus(decision.Status), decision.BytesWritten, decision.ErrorCode); err != nil {
				return err
			}
		}
		session := &managedSession{id: record.ID, name: record.Name, command: pty.Command{Path: record.CommandPath, Args: record.CommandArgs, Env: record.CommandEnv, CWD: record.CWD, Dimensions: pty.Dimensions{Columns: record.Columns, Rows: record.Rows}}, lifecycle: lifecycle, history: retained, fanout: NewFanout(), inputs: inputJournal, activitySeq: make(map[string]uint64)}
		for _, decision := range decisions {
			if InputStatus(decision.Status) == InputAccepted || InputStatus(decision.Status) == InputUncertain && decision.BytesWritten > 0 {
				key := InputKey{ClientID: decision.ClientID, AttachmentID: decision.AttachmentID, Generation: decision.Generation, InputID: decision.InputID}
				if err := m.recordInputActivityLocked(session, key, decision.CreatedAt); err != nil {
					return err
				}
			}
		}
		if record.ExitCode != nil {
			session.exit = &pty.ExitResult{Code: *record.ExitCode, Signal: record.ExitSignal}
			if record.ExitedAt != nil {
				session.exit.ExitedAt = *record.ExitedAt
			}
		}
		if _, duplicate := m.sessions[record.ID]; duplicate || m.names[record.Name] != "" {
			return ErrInvalidSession
		}
		m.sessions[record.ID], m.names[record.Name] = session, record.ID
	}
	return nil
}

func randomID(random io.Reader) (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(random, value[:]); err != nil {
		return "", err
	}
	return "ses_" + hex.EncodeToString(value[:]), nil
}

func validSessionName(name string) bool {
	if len(name) < 1 || len(name) > 64 {
		return false
	}
	for _, char := range name {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-' || char == '_') {
			return false
		}
	}
	return !strings.HasPrefix(name, "-")
}

func (m *Manager) recordInputActivityLocked(session *managedSession, key InputKey, occurredAt time.Time) error {
	if m.config.Activity == nil {
		return nil
	}
	if len(session.pendingActivity) >= m.config.MaxPendingActivity {
		return activity.ErrQueueFull
	}
	sourceID := activitySourceID(session.id, key.AttachmentID, key.Generation)
	session.activitySeq[sourceID]++
	event := activity.Event{EnvironmentID: m.config.EnvironmentID, SourceID: sourceID, SessionID: session.id, ProcessID: fmt.Sprint(key.Generation), Source: activity.TerminalInput, Sequence: session.activitySeq[sourceID], OccurredAt: occurredAt}
	session.pendingActivity = append(session.pendingActivity, event)
	m.flushActivityLocked(session)
	return nil
}

func (m *Manager) flushActivityLocked(session *managedSession) {
	for len(session.pendingActivity) > 0 {
		if _, err := m.config.Activity.Record(session.pendingActivity[0], true); err != nil {
			return
		}
		session.pendingActivity[0] = activity.Event{}
		session.pendingActivity = session.pendingActivity[1:]
	}
}

func activitySourceID(sessionID, attachmentID string, generation uint64) string {
	digest := sha256.Sum256([]byte(sessionID + "\x00" + attachmentID + "\x00" + fmt.Sprint(generation)))
	return "act_" + hex.EncodeToString(digest[:16])
}
