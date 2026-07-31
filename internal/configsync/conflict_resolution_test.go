package configsync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConflictRevisionIsStableAcrossUnrelatedHeadsAndChangesWithSides(t *testing.T) {
	base := map[string]FileState{"path": {Hash: "base"}}
	local := map[string]FileState{"path": {Hash: "local"}}
	remote := map[string]FileState{"path": {Hash: "remote"}}
	conflict := []PathSummary{{Path: "path", Reason: "concurrent_update"}}
	first := identifyConflicts("repo", "assignment", "head-1", base, local, remote, conflict)
	replayed := identifyConflicts("repo", "assignment", "head-1", base, local, remote, conflict)
	moved := identifyConflicts("repo", "assignment", "head-2", base, local, remote, conflict)
	remote["path"] = FileState{Hash: "remote-2"}
	changed := identifyConflicts("repo", "assignment", "head-2", base, local, remote, conflict)
	if len(first) != 1 || !safeConflictRevision(first[0].Revision) ||
		first[0].Revision != replayed[0].Revision || first[0].Revision != moved[0].Revision ||
		first[0].Revision == changed[0].Revision {
		t.Fatalf("revisions = %#v, %#v, %#v, %#v", first, replayed, moved, changed)
	}
}

func TestConflictResolutionRejectsRemovedExternalAction(t *testing.T) {
	resolution := ConflictResolution{
		ID: "resolution", Path: "path",
		ConflictRevision:       strings.Repeat("a", 64),
		ExpectedRemoteRevision: "head", Scope: "path", Action: "externally_resolved",
	}
	if resolution.Valid() {
		t.Fatal("externally_resolved remains valid")
	}
}

func TestResolutionCommitRoundTripIsPrivatePlaintextAndBindingChecked(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "resolution-commit.json")
	revision := "commit"
	value := resolutionCommit{
		Format: "paperboat-config-resolution-commit-v1", CommitID: revision,
		Baseline: Baseline{
			Format: "paperboat-config-baseline-v1", RepositoryID: "repo",
			AssignmentID: "assignment", PolicyRevision: "policy",
			ManifestRevision: strings.Repeat("b", 64), SelectedRoots: []ManifestRoot{},
			FrozenPaths:    map[string]FrozenPath{},
			RemoteRevision: revision, Files: map[string]FileState{},
		},
		Resolutions: []ConflictResolution{{
			ID: "resolution", Path: "path",
			ExpectedRemoteRevision: "head", Scope: "path", Action: "keep_local",
		}},
	}
	value.Resolutions[0].ConflictRevision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := writeResolutionCommit(path, value); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(raw), `"assignment_id":"assignment"`) {
		t.Fatalf("journal is not readable plaintext: %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("journal permissions = %v, %v", info.Mode(), err)
	}
	decoded, err := readResolutionCommit(path)
	if err != nil || decoded.CommitID != revision || decoded.Resolutions[0].ID != "resolution" {
		t.Fatalf("decoded = %#v, %v", decoded, err)
	}
}
