package configsync

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

var ErrEngineInvalid = errors.New("invalid config sync engine")

type Syncer interface {
	Sync(context.Context, string) (PublishResult, error)
}

type StatusReporter interface {
	ReportStatus(context.Context, Status, int) error
}

type EngineConfig struct {
	HomeRoot    string
	Descriptor  RuntimeDescriptor
	Syncer      Syncer
	Statuses    StatusReporter
	Diagnostics DiagnosticsSource
	StatusPath  string
	Clock       func() time.Time
}

type Engine struct {
	homeRoot    string
	descriptor  RuntimeDescriptor
	syncer      Syncer
	statuses    StatusReporter
	diagnostics DiagnosticsSource
	statusPath  string
	clock       func() time.Time

	mu             sync.Mutex
	syncMu         sync.Mutex
	cancel         context.CancelFunc
	done           chan struct{}
	watcher        *fsnotify.Watcher
	remoteRevision string
	syncRevision   int64
	lastPush       time.Time
	dirtySince     time.Time
	status         Status
}

func (e *Engine) Apply(ctx context.Context) error {
	return e.syncNow(ctx)
}

func NewEngine(config EngineConfig) (*Engine, error) {
	if !canonicalAbsolutePath(config.HomeRoot) || config.Syncer == nil ||
		validateRuntimeDescriptor(config.Descriptor, Credential{
			EnvironmentID: config.Descriptor.EnvironmentID, HelperID: config.Descriptor.HelperID,
			AssignmentID: config.Descriptor.AssignmentID, WarningRevision: config.Descriptor.WarningRevision,
		}) != nil || (config.StatusPath != "" && !canonicalAbsolutePath(config.StatusPath)) {
		return nil, ErrEngineInvalid
	}
	info, err := os.Lstat(config.HomeRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(ErrEngineInvalid, err)
	}
	resolved, err := filepath.EvalSymlinks(config.HomeRoot)
	if err != nil || resolved != config.HomeRoot {
		return nil, errors.Join(ErrEngineInvalid, err)
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	now := config.Clock().UTC()
	syncRevision := config.Descriptor.SyncRevisionFloor
	acknowledgedKeyVersion := int64(0)
	if config.StatusPath != "" {
		if previous, readErr := ReadStatus(config.StatusPath, config.Descriptor.Policy.SummaryLimit); readErr == nil &&
			previous.RepositoryID == config.Descriptor.RepositoryID &&
			previous.AssignmentID == config.Descriptor.AssignmentID &&
			previous.EnvironmentID == config.Descriptor.EnvironmentID &&
			previous.HelperID == config.Descriptor.HelperID &&
			previous.HelperGeneration == config.Descriptor.HelperGeneration {
			if previous.SyncRevision > syncRevision {
				syncRevision = previous.SyncRevision
			}
			if previous.KeyVersion > 0 && previous.KeyVersion <= int64(config.Descriptor.KeyVersion) {
				acknowledgedKeyVersion = previous.KeyVersion
			}
		}
	}
	return &Engine{
		homeRoot: config.HomeRoot, descriptor: config.Descriptor, syncer: config.Syncer,
		statuses: config.Statuses, statusPath: config.StatusPath, clock: config.Clock,
		diagnostics: config.Diagnostics, syncRevision: syncRevision,
		status: Status{
			State: "restoring", RepositoryID: config.Descriptor.RepositoryID,
			AssignmentID: config.Descriptor.AssignmentID, EnvironmentID: config.Descriptor.EnvironmentID,
			HelperID: config.Descriptor.HelperID, WarningRevision: config.Descriptor.WarningRevision,
			HelperGeneration: config.Descriptor.HelperGeneration,
			PolicyRevision:   config.Descriptor.Policy.Revision, KeyVersion: acknowledgedKeyVersion,
			SyncRevision: syncRevision, UpdatedAt: now,
		},
	}, nil
}

func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cancel != nil {
		return nil
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err := addManagedWatches(watcher, e.homeRoot, e.descriptor.Policy); err != nil {
		_ = watcher.Close()
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	e.cancel, e.watcher, e.done = cancel, watcher, make(chan struct{})
	e.dirtySince = e.clock().UTC()
	go e.run(runCtx, e.done, watcher)
	return nil
}

func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	cancel, done, watcher := e.cancel, e.done, e.watcher
	e.cancel, e.done, e.watcher = nil, nil, nil
	e.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	_ = watcher.Close()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	flushTimeout := e.descriptor.Policy.ShutdownFlushTimeout
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < flushTimeout {
		flushTimeout = time.Until(deadline)
	}
	if flushTimeout <= 0 {
		return context.DeadlineExceeded
	}
	flushCtx, flushCancel := context.WithTimeout(ctx, flushTimeout)
	defer flushCancel()
	return e.syncNow(flushCtx)
}

func (e *Engine) run(ctx context.Context, done chan<- struct{}, watcher *fsnotify.Watcher) {
	defer close(done)
	debounce := time.NewTimer(0)
	defer debounce.Stop()
	poll := time.NewTicker(e.descriptor.Policy.RemotePollInterval)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
				if info, err := os.Lstat(event.Name); err == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
					_ = addManagedWatches(watcher, event.Name, e.descriptor.Policy)
				}
			}
			if e.managedEvent(event.Name) {
				e.markDirty()
				resetTimer(debounce, e.nextDelay())
			}
		case <-watcher.Errors:
			e.markDirty()
			resetTimer(debounce, e.descriptor.Policy.Debounce)
		case <-debounce.C:
			_ = e.syncNow(ctx)
		case <-poll.C:
			_ = e.syncNow(ctx)
		}
	}
}

func (e *Engine) markDirty() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.dirtySince.IsZero() {
		e.dirtySince = e.clock().UTC()
	}
}

func (e *Engine) nextDelay() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clock().UTC()
	delay := e.descriptor.Policy.Debounce
	if !e.lastPush.IsZero() {
		if remaining := e.descriptor.Policy.MinimumPushInterval - now.Sub(e.lastPush); remaining > delay {
			delay = remaining
		}
	}
	if !e.dirtySince.IsZero() {
		if remaining := e.descriptor.Policy.MaximumDirtyDelay - now.Sub(e.dirtySince); remaining < delay {
			delay = remaining
		}
	}
	if delay < 0 {
		return 0
	}
	return delay
}

func (e *Engine) managedEvent(full string) bool {
	relative, err := filepath.Rel(e.homeRoot, full)
	if err != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return false
	}
	return managedPath(filepath.ToSlash(relative), e.descriptor.Policy)
}

func (e *Engine) syncNow(ctx context.Context) error {
	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	e.mu.Lock()
	remoteRevision := e.remoteRevision
	e.status.State = "syncing"
	now := e.clock().UTC()
	e.status.LastAttemptAt = &now
	e.status.UpdatedAt = now
	e.syncRevision++
	e.status.SyncRevision = e.syncRevision
	e.mu.Unlock()
	e.report(ctx)

	result, err := e.syncer.Sync(ctx, remoteRevision)
	diagnostics := ReconciliationDiagnostics{}
	if e.diagnostics != nil {
		diagnostics = e.diagnostics.Diagnostics()
	}
	e.mu.Lock()
	now = e.clock().UTC()
	e.status.UpdatedAt = now
	e.status.ClassifierPending = diagnostics.ClassifierPending
	e.status.Skipped = diagnostics.Skipped
	e.status.Conflicts = diagnostics.Conflicts
	e.status.PendingPathCount = len(diagnostics.ClassifierPending)
	if result.RemoteRevision != "" {
		e.remoteRevision = result.RemoteRevision
		e.status.RemoteRevision = result.RemoteRevision
	}
	switch {
	case err == nil && result.Landed:
		e.remoteRevision = result.RemoteRevision
		e.lastPush = now
		e.dirtySince = time.Time{}
		e.status.State = "healthy"
		if len(diagnostics.Skipped) > 0 || len(diagnostics.ClassifierPending) > 0 {
			e.status.State = "warning"
		}
		e.status.RemoteRevision = result.RemoteRevision
		e.status.LastSuccessfulAt = &now
		e.status.KeyVersion = int64(e.descriptor.KeyVersion)
		e.status.ErrorCode = ""
		e.status.RecoveryActions = nil
	case errors.Is(err, ErrConfigConflict):
		e.status.State = "conflict"
		e.status.ErrorCode = "config_conflict"
		e.status.RecoveryActions = []string{"keep_local", "keep_remote", "externally_resolved"}
	case errors.Is(err, ErrSyncUncertain):
		e.status.State = "sync_uncertain"
		e.status.ErrorCode = "sync_uncertain"
		e.status.RecoveryActions = []string{"observe_remote"}
	case errors.Is(err, ErrWritesDisabled):
		e.status.State = "warning"
		e.status.ErrorCode = "writes_disabled"
		e.status.RecoveryActions = []string{"wait_for_rollout"}
	case errors.Is(err, ErrAuthorization), errors.Is(err, ErrLeaseLost):
		e.status.State = "revoked"
		e.status.ErrorCode = "credential_expired"
	default:
		e.status.State = "error"
		e.status.ErrorCode = "repository_unavailable"
	}
	e.mu.Unlock()
	e.report(ctx)
	return err
}

func (e *Engine) report(ctx context.Context) {
	e.mu.Lock()
	status := e.status
	e.mu.Unlock()
	if e.statusPath != "" {
		_ = WriteStatus(e.statusPath, status, e.descriptor.Policy.SummaryLimit)
	}
	if e.statuses != nil {
		_ = e.statuses.ReportStatus(ctx, status, e.descriptor.Policy.SummaryLimit)
	}
}

func addManagedWatches(watcher *fsnotify.Watcher, root string, policy RuntimePolicy) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return errors.Join(ErrEngineInvalid, err)
		}
		if path != root {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			if !mayContainManagedPath(filepath.ToSlash(relative), policy) {
				return filepath.SkipDir
			}
		}
		return watcher.Add(path)
	})
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}
