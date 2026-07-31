package configsync

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
)

type testResolutionAuthority struct {
	items        []ConflictResolution
	acknowledged []string
}

func (a *testResolutionAuthority) Pending(context.Context) ([]ConflictResolution, error) {
	return append([]ConflictResolution(nil), a.items...), nil
}

func (a *testResolutionAuthority) Acknowledge(_ context.Context, id, _ string) error {
	a.acknowledged = append(a.acknowledged, id)
	return nil
}

func TestWorkspaceReconcilerPublishesCleanMergeFromPersistedBase(t *testing.T) {
	root := resolvedTempDir(t)
	repositoryRoot := filepath.Join(root, "repository")
	homeRoot := filepath.Join(root, "home")
	stateRoot := filepath.Join(root, "state")
	for _, path := range []string{repositoryRoot, homeRoot, stateRoot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writeTestFile(t, filepath.Join(repositoryRoot, ".pbinclude"), "config.txt\nclean.txt\n")
	writeTestFile(t, filepath.Join(repositoryRoot, "config.txt"), "one\ntwo\nthree\n")
	writeTestFile(t, filepath.Join(repositoryRoot, "clean.txt"), "clean base\n")
	if err := os.Chmod(filepath.Join(repositoryRoot, "config.txt"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(repositoryRoot, "clean.txt"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(homeRoot, "config.txt"), "one\ntwo\nthree\n")
	writeTestFile(t, filepath.Join(homeRoot, "clean.txt"), "clean base\n")
	if err := os.Chmod(filepath.Join(homeRoot, "config.txt"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(homeRoot, "clean.txt"), 0o644); err != nil {
		t.Fatal(err)
	}
	repository, err := git.PlainInit(repositoryRoot, false)
	if err != nil {
		t.Fatal(err)
	}
	baseRevision := commitAll(t, repository, "base")

	descriptor := testRuntimeDescriptor()
	reconciler, err := NewPlaintextWorkspaceReconciler(WorkspaceReconcilerConfig{
		HomeRoot: homeRoot, StateRoot: stateRoot, Descriptor: descriptor,
		ChezmoiBinary: writeChezmoiTestShim(t, root),
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := reconciler.Reconcile(context.Background(), repositoryRoot, RemoteSnapshot{Revision: baseRevision})
	if err != nil || prepared.HasChanges || len(reconciler.Diagnostics().Conflicts) > 0 {
		t.Fatalf("initial reconcile = %#v, diagnostics = %#v, %v", prepared, reconciler.Diagnostics(), err)
	}
	if err := reconciler.PublicationCommitted(context.Background(), prepared, baseRevision); err != nil {
		t.Fatal(err)
	}
	initialBaseline, err := ReadBaseline(filepath.Join(stateRoot, "baseline.json"))
	if err != nil || initialBaseline.Files["config.txt"].Hash == "" {
		t.Fatalf("initial baseline = %#v, %v", initialBaseline, err)
	}

	writeTestFile(t, filepath.Join(homeRoot, "config.txt"), "ONE\ntwo\nthree\n")
	if err := os.Chmod(filepath.Join(homeRoot, "config.txt"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(repositoryRoot, "config.txt"), "one\ntwo\nTHREE\n")
	remoteRevision := commitAll(t, repository, "remote change")
	prepared, err = reconciler.Reconcile(context.Background(), repositoryRoot, RemoteSnapshot{Revision: remoteRevision})
	if err != nil || !prepared.HasChanges || prepared.ExpectedRemoteRevision != remoteRevision {
		t.Fatalf("merged reconcile = %#v, diagnostics = %#v, %v", prepared, reconciler.Diagnostics(), err)
	}
	value, err := os.ReadFile(filepath.Join(homeRoot, "config.txt"))
	if err != nil || string(value) != "ONE\ntwo\nTHREE\n" {
		t.Fatalf("merged target = %q, %v", value, err)
	}
	if conflicts := reconciler.Diagnostics().Conflicts; len(conflicts) != 0 {
		t.Fatalf("clean merge conflicts = %#v", conflicts)
	}
	if err := reconciler.PublicationCommitted(context.Background(), prepared, prepared.CommitID); err != nil {
		t.Fatal(err)
	}
	baseline, err := ReadBaseline(filepath.Join(stateRoot, "baseline.json"))
	if err != nil || baseline.RemoteRevision != prepared.CommitID || baseline.ManifestRevision == "" || len(baseline.SelectedRoots) != 2 {
		t.Fatalf("merged baseline = %#v, %v", baseline, err)
	}
	mergedBaseState := baseline.Files["config.txt"]

	writeTestFile(t, filepath.Join(homeRoot, "config.txt"), "LOCAL\ntwo\nTHREE\n")
	writeTestFile(t, filepath.Join(homeRoot, "clean.txt"), "clean local\n")
	writeTestFile(t, filepath.Join(repositoryRoot, "config.txt"), "REMOTE\ntwo\nTHREE\n")
	conflictingRemote := commitAll(t, repository, "overlapping remote change")
	prepared, err = reconciler.Reconcile(context.Background(), repositoryRoot, RemoteSnapshot{Revision: conflictingRemote})
	diagnostics := reconciler.Diagnostics()
	if err != nil || !prepared.HasChanges || len(diagnostics.Conflicts) != 1 ||
		diagnostics.Conflicts[0].Path != "config.txt" || diagnostics.Conflicts[0].Reason != "merge_conflict" {
		t.Fatalf("isolated reconcile = %#v, diagnostics = %#v, %v", prepared, diagnostics, err)
	}
	baseVariant := filepath.Join(stateRoot, "conflicts", diagnostics.Conflicts[0].Revision, "config.txt.base")
	if value, readErr := os.ReadFile(baseVariant); readErr != nil || string(value) != "ONE\ntwo\nTHREE\n" {
		t.Fatalf("preserved base = %q", value)
	}
	conflictedValue, err := os.ReadFile(filepath.Join(homeRoot, "config.txt"))
	if err != nil || string(conflictedValue) != "LOCAL\ntwo\nTHREE\n" {
		t.Fatalf("conflicted target changed = %q, %v", conflictedValue, err)
	}
	repositoryClean, err := os.ReadFile(filepath.Join(repositoryRoot, "clean.txt"))
	if err != nil || string(repositoryClean) != "clean local\n" {
		t.Fatalf("clean path was not prepared = %q, %v", repositoryClean, err)
	}
	if err := reconciler.PublicationCommitted(context.Background(), prepared, prepared.CommitID); err != nil {
		t.Fatal(err)
	}
	baseline, err = ReadBaseline(filepath.Join(stateRoot, "baseline.json"))
	if err != nil || baseline.Files["config.txt"] != mergedBaseState || baseline.Files["clean.txt"].Hash == "" ||
		baseline.FrozenPaths["config.txt"].ConflictRevision != diagnostics.Conflicts[0].Revision {
		t.Fatalf("isolated baseline = %#v, %v", baseline, err)
	}

	authority := &testResolutionAuthority{items: []ConflictResolution{{
		ID: "force-resolution", Path: "config.txt", Scope: "path", Action: "force_pull",
		ConflictRevision:       diagnostics.Conflicts[0].Revision,
		ExpectedRemoteRevision: prepared.CommitID,
	}}}
	reconciler.resolutions = authority
	forced, err := reconciler.Reconcile(context.Background(), repositoryRoot, RemoteSnapshot{Revision: prepared.CommitID})
	if err != nil || forced.HasChanges || len(reconciler.Diagnostics().Conflicts) != 0 {
		t.Fatalf("force pull = %#v, diagnostics = %#v, %v", forced, reconciler.Diagnostics(), err)
	}
	if err := reconciler.PublicationCommitted(context.Background(), forced, forced.CommitID); err != nil {
		t.Fatal(err)
	}
	forcedValue, err := os.ReadFile(filepath.Join(homeRoot, "config.txt"))
	if err != nil || string(forcedValue) != "REMOTE\ntwo\nTHREE\n" || len(authority.acknowledged) != 1 {
		t.Fatalf("forced target = %q, acknowledgements = %#v, %v", forcedValue, authority.acknowledged, err)
	}
	baseline, err = ReadBaseline(filepath.Join(stateRoot, "baseline.json"))
	if err != nil || len(baseline.FrozenPaths) != 0 {
		t.Fatalf("force baseline = %#v, %v", baseline, err)
	}
}

func resolvedTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func testRuntimeDescriptor() RuntimeDescriptor {
	return RuntimeDescriptor{
		WriteMode: "leased_writes", Mode: ModeBidirectional,
		RepositoryID: "repository", AssignmentID: "assignment", EnvironmentID: "environment",
		HelperID: "helper", HelperGeneration: 1, WarningRevision: "warning",
		Policy: RuntimePolicy{
			Format: "paperboat-config-plaintext-v1", Revision: "policy",
			ManifestContract: ManifestContractVersion, ManifestMaxBytes: DefaultManifestMaxBytes,
			ManifestMaxLines: DefaultManifestMaxLines, ManifestMaxPatternBytes: DefaultManifestMaxPatternBytes,
			MaxFileBytes: 1 << 20, MaxBatchBytes: 2 << 20, Debounce: time.Second,
			MinimumPushInterval: time.Minute, MaximumDirtyDelay: time.Minute,
			RemotePollInterval: time.Hour, RetryLimit: 1, ShutdownFlushTimeout: time.Second, SummaryLimit: 10,
		},
	}
}

func commitAll(t *testing.T, repository *git.Repository, message string) string {
	t.Helper()
	worktree, err := repository.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if err := worktree.AddWithOptions(&git.AddOptions{All: true}); err != nil {
		t.Fatal(err)
	}
	hash, err := worktree.Commit(message, &git.CommitOptions{Author: &object.Signature{
		Name: "Test", Email: "test@example.invalid", When: time.Unix(1, 0).UTC(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return hash.String()
}

func writeTestFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeChezmoiTestShim(t *testing.T, root string) string {
	t.Helper()
	path := filepath.Join(root, "chezmoi-test")
	script := `#!/bin/sh
set -eu
config=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--config" ]; then config="$2"; shift 2; break; fi
  shift
done
source_dir=$(sed -n 's/^sourceDir = "\(.*\)"$/\1/p' "$config")
dest_dir=$(sed -n 's/^destDir = "\(.*\)"$/\1/p' "$config")
operation="$1"
shift
case "$operation" in
  apply)
    for source in "$source_dir"/*.txt; do
      if [ -f "$source" ]; then cp "$source" "$dest_dir/$(basename "$source")"; fi
    done
    ;;
  add)
    while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do shift; done
    shift
    while [ "$#" -gt 0 ]; do
      cp "$1" "$source_dir/$(basename "$1")"
      shift
    done
    ;;
  forget)
    while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do shift; done
    shift
    rm -f "$source_dir/$(basename "$1")"
    ;;
  *) exit 2 ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
