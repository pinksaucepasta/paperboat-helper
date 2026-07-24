package configsync

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"filippo.io/age"
)

func TestConflictRevisionChangesWithBindingHeadAndSides(t *testing.T) {
	base := map[string]FileState{"path": {Hash: "base"}}
	local := map[string]FileState{"path": {Hash: "local"}}
	remote := map[string]FileState{"path": {Hash: "remote"}}
	conflict := []PathSummary{{Path: "path", Reason: "concurrent_update"}}
	first := identifyConflicts("repo", "assignment", "head-1", base, local, remote, conflict)
	replayed := identifyConflicts("repo", "assignment", "head-1", base, local, remote, conflict)
	moved := identifyConflicts("repo", "assignment", "head-2", base, local, remote, conflict)
	if len(first) != 1 || !safeConflictRevision(first[0].Revision) ||
		first[0].Revision != replayed[0].Revision || first[0].Revision == moved[0].Revision {
		t.Fatalf("revisions = %#v, %#v, %#v", first, replayed, moved)
	}
}

func TestResolutionCommitRoundTripIsEncryptedAndBindingChecked(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "resolution-commit.age")
	revision := "commit"
	value := resolutionCommit{
		Format: "paperboat-config-resolution-commit-v1", CommitID: revision,
		Baseline: Baseline{
			Format: "paperboat-config-baseline-v1", RepositoryID: "repo",
			AssignmentID: "assignment", PolicyRevision: "policy", KeyVersion: 1,
			RemoteRevision: revision, Files: map[string]FileState{},
		},
		Resolutions: []ConflictResolution{{
			ID: "resolution", Path: "path",
			ExpectedRemoteRevision: "head", Action: "keep_local",
		}},
	}
	value.Resolutions[0].ConflictRevision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := writeResolutionCommit(path, identity.Recipient().String(), value); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) == "" || bytes.Contains(raw, []byte("assignment")) {
		t.Fatalf("journal leaks plaintext or is unreadable: %v", err)
	}
	decoded, err := readResolutionCommit(path, identity.String())
	if err != nil || decoded.CommitID != revision || decoded.Resolutions[0].ID != "resolution" {
		t.Fatalf("decoded = %#v, %v", decoded, err)
	}
}
