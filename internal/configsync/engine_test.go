package configsync

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type recordingSyncer struct {
	mu      sync.Mutex
	calls   int
	results chan struct{}
}

type failingSyncer struct{ err error }

func (s failingSyncer) Sync(context.Context, string) (PublishResult, error) {
	return PublishResult{}, s.err
}

func (s *recordingSyncer) Sync(context.Context, string) (PublishResult, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	select {
	case s.results <- struct{}{}:
	default:
	}
	return PublishResult{RemoteRevision: "head", Landed: true}, nil
}

func TestEngineRunsInitialSyncAndBoundedShutdownFlush(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	descriptor := RuntimeDescriptor{
		WriteMode: "leased_writes", Mode: ModeBidirectional,
		RepositoryID: "repository", AssignmentID: "assignment", EnvironmentID: "environment",
		HelperID: "helper", WarningRevision: "warning",
		HelperGeneration: 1,
		Policy: RuntimePolicy{
			Format: "paperboat-config-plaintext-v1", Revision: "policy",
			ManifestContract: ManifestContractVersion, ManifestMaxBytes: DefaultManifestMaxBytes,
			ManifestMaxLines: DefaultManifestMaxLines, ManifestMaxPatternBytes: DefaultManifestMaxPatternBytes,
			MaxFileBytes: 1 << 20, MaxBatchBytes: 2 << 20, Debounce: time.Second,
			MinimumPushInterval: time.Minute, MaximumDirtyDelay: time.Minute,
			RemotePollInterval: time.Hour, RetryLimit: 1, ShutdownFlushTimeout: time.Second, SummaryLimit: 10,
		},
	}
	syncer := &recordingSyncer{results: make(chan struct{}, 4)}
	engine, err := NewEngine(EngineConfig{
		HomeRoot: home, Descriptor: descriptor, Syncer: syncer,
		StatusPath: filepath.Join(state, "status.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := engine.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-syncer.results:
	case <-time.After(time.Second):
		t.Fatal("initial sync did not run")
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	if err := engine.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-syncer.results:
	case <-time.After(time.Second):
		t.Fatal("shutdown flush did not run")
	}
}

func TestEngineRestoresAndAdvancesSyncRevision(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	descriptor := RuntimeDescriptor{
		WriteMode: "leased_writes", Mode: ModeBidirectional, RepositoryID: "repository", AssignmentID: "assignment",
		EnvironmentID: "environment", HelperID: "helper", WarningRevision: "warning",
		HelperGeneration: 1,
		Policy: RuntimePolicy{
			Format: "paperboat-config-plaintext-v1", Revision: "policy",
			ManifestContract: ManifestContractVersion, ManifestMaxBytes: DefaultManifestMaxBytes,
			ManifestMaxLines: DefaultManifestMaxLines, ManifestMaxPatternBytes: DefaultManifestMaxPatternBytes,
			MaxFileBytes: 1 << 20, MaxBatchBytes: 2 << 20, Debounce: time.Second,
			MinimumPushInterval: time.Minute, MaximumDirtyDelay: time.Minute,
			RemotePollInterval: time.Hour, RetryLimit: 1, ShutdownFlushTimeout: time.Second, SummaryLimit: 10,
		},
	}
	statusPath := filepath.Join(state, "status.json")
	if err := WriteStatus(statusPath, Status{
		State: "healthy", Mode: descriptor.Mode, RepositoryID: descriptor.RepositoryID, AssignmentID: descriptor.AssignmentID,
		EnvironmentID: descriptor.EnvironmentID, HelperID: descriptor.HelperID,
		HelperGeneration: descriptor.HelperGeneration, WarningRevision: descriptor.WarningRevision,
		PolicyRevision: descriptor.Policy.Revision, SyncRevision: 7, UpdatedAt: now,
	}, descriptor.Policy.SummaryLimit); err != nil {
		t.Fatal(err)
	}
	syncer := &recordingSyncer{results: make(chan struct{}, 1)}
	engine, err := NewEngine(EngineConfig{
		HomeRoot: home, Descriptor: descriptor, Syncer: syncer, StatusPath: statusPath,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if engine.status.SyncRevision != 7 {
		t.Fatalf("restored status = %#v, want last proven revision", engine.status)
	}
	if err := engine.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := ReadStatus(statusPath, descriptor.Policy.SummaryLimit)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "healthy" || status.SyncRevision != 8 {
		t.Fatalf("acknowledged status = %#v", status)
	}
}

func TestEngineRecordsFailedSyncRevisionWithoutSuccess(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	descriptor := RuntimeDescriptor{
		WriteMode: "leased_writes", Mode: ModeBidirectional, RepositoryID: "repository", AssignmentID: "assignment",
		EnvironmentID: "environment", HelperID: "helper", WarningRevision: "warning",
		HelperGeneration: 1,
		Policy: RuntimePolicy{
			Format: "paperboat-config-plaintext-v1", Revision: "policy",
			ManifestContract: ManifestContractVersion, ManifestMaxBytes: DefaultManifestMaxBytes,
			ManifestMaxLines: DefaultManifestMaxLines, ManifestMaxPatternBytes: DefaultManifestMaxPatternBytes,
			MaxFileBytes: 1 << 20, MaxBatchBytes: 2 << 20, Debounce: time.Second,
			MinimumPushInterval: time.Minute, MaximumDirtyDelay: time.Minute,
			RemotePollInterval: time.Hour, RetryLimit: 1, ShutdownFlushTimeout: time.Second, SummaryLimit: 10,
		},
	}
	engine, err := NewEngine(EngineConfig{
		HomeRoot: home, Descriptor: descriptor, Syncer: failingSyncer{err: ErrRemoteRevisionChanged},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Apply(context.Background()); !errors.Is(err, ErrRemoteRevisionChanged) {
		t.Fatalf("error = %v", err)
	}
	if engine.status.SyncRevision != 1 || engine.status.State != "error" || engine.status.LastSuccessfulAt != nil {
		t.Fatalf("failed sync status = %#v", engine.status)
	}
}
