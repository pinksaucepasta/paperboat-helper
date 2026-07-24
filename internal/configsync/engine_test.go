package configsync

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
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
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	descriptor := RuntimeDescriptor{
		WriteMode:    "leased_writes",
		RepositoryID: "repository", AssignmentID: "assignment", EnvironmentID: "environment",
		HelperID: "helper", WarningRevision: "warning", KeyVersion: 1,
		HelperGeneration: 1,
		AgeRecipient:     identity.Recipient().String(), AgeIdentities: identity.String(),
		Policy: RuntimePolicy{
			Format: "paperboat-chezmoi-age-v1", Revision: "policy",
			Includes:            []string{".bashrc"},
			MandatoryExclusions: append([]string(nil), requiredMandatoryExclusions...),
			MaxFileBytes:        1 << 20, MaxBatchBytes: 2 << 20, Debounce: time.Second,
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

func TestEngineAcknowledgesRotatedKeyOnlyAfterSuccessfulSync(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	descriptor := RuntimeDescriptor{
		WriteMode: "leased_writes", RepositoryID: "repository", AssignmentID: "assignment",
		EnvironmentID: "environment", HelperID: "helper", WarningRevision: "warning",
		KeyVersion: 2, HelperGeneration: 1, AgeRecipient: identity.Recipient().String(),
		AgeIdentities: identity.String(),
		Policy: RuntimePolicy{
			Format: "paperboat-chezmoi-age-v1", Revision: "policy",
			Includes:            []string{".bashrc"},
			MandatoryExclusions: append([]string(nil), requiredMandatoryExclusions...),
			MaxFileBytes:        1 << 20, MaxBatchBytes: 2 << 20, Debounce: time.Second,
			MinimumPushInterval: time.Minute, MaximumDirtyDelay: time.Minute,
			RemotePollInterval: time.Hour, RetryLimit: 1, ShutdownFlushTimeout: time.Second, SummaryLimit: 10,
		},
	}
	statusPath := filepath.Join(state, "status.json")
	if err := WriteStatus(statusPath, Status{
		State: "healthy", RepositoryID: descriptor.RepositoryID, AssignmentID: descriptor.AssignmentID,
		EnvironmentID: descriptor.EnvironmentID, HelperID: descriptor.HelperID,
		HelperGeneration: descriptor.HelperGeneration, WarningRevision: descriptor.WarningRevision,
		PolicyRevision: descriptor.Policy.Revision, KeyVersion: 1, SyncRevision: 7, UpdatedAt: now,
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
	if engine.status.KeyVersion != 1 || engine.status.SyncRevision != 7 {
		t.Fatalf("restored status = %#v, want last proven key version and revision", engine.status)
	}
	if err := engine.Apply(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := ReadStatus(statusPath, descriptor.Policy.SummaryLimit)
	if err != nil {
		t.Fatal(err)
	}
	if status.KeyVersion != 2 || status.State != "healthy" || status.SyncRevision != 8 {
		t.Fatalf("acknowledged status = %#v", status)
	}
}

func TestEngineDoesNotAcknowledgeRotatedKeyAfterFailedSync(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	descriptor := RuntimeDescriptor{
		WriteMode: "leased_writes", RepositoryID: "repository", AssignmentID: "assignment",
		EnvironmentID: "environment", HelperID: "helper", WarningRevision: "warning",
		KeyVersion: 2, HelperGeneration: 1, AgeRecipient: identity.Recipient().String(),
		AgeIdentities: identity.String(),
		Policy: RuntimePolicy{
			Format: "paperboat-chezmoi-age-v1", Revision: "policy",
			Includes:            []string{".bashrc"},
			MandatoryExclusions: append([]string(nil), requiredMandatoryExclusions...),
			MaxFileBytes:        1 << 20, MaxBatchBytes: 2 << 20, Debounce: time.Second,
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
	if engine.status.KeyVersion != 0 {
		t.Fatalf("failed sync acknowledged key version %d", engine.status.KeyVersion)
	}
}
