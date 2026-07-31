package configsync

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStatusRoundTripAndCanonicalStates(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "status.json")
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	status := Status{
		State: "conflict", Mode: ModeBidirectional, RepositoryID: "repo", AssignmentID: "assignment", EnvironmentID: "environment",
		HelperID: "helper", HelperGeneration: 2, WarningRevision: "warning-1", PolicyRevision: "policy-1",
		SyncRevision: 7, RemoteRevision: "head", UpdatedAt: now,
		PendingCleanPathCount: 1, ManifestHealth: "healthy", ManifestRevision: strings.Repeat("a", 64),
		ManagedPathCount: 2, Conflicts: []PathSummary{{Path: ".config/tool/settings.json", Bytes: 42, Reason: "changed_both"}},
		ErrorCode: "config_conflict", RecoveryActions: []string{"keep_local", "keep_remote"},
	}
	if err := WriteStatus(path, status, 10); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("status mode = %v, %v", info.Mode().Perm(), err)
	}
	got, err := ReadStatus(path, 10)
	if err != nil || got.State != "conflict" || got.SyncRevision != 7 || len(got.Conflicts) != 1 {
		t.Fatalf("status = %#v, %v", got, err)
	}
}

func TestStatusRejectsLeakingOrInconsistentSummaries(t *testing.T) {
	now := time.Now().UTC()
	tests := []Status{
		{State: "unknown", UpdatedAt: now},
		{State: "conflict", UpdatedAt: now},
		{State: "warning", UpdatedAt: now, Skipped: []PathSummary{{Path: "/Users/alice/.secret", Reason: "excluded"}}},
		{State: "warning", UpdatedAt: now, Skipped: []PathSummary{{Path: "../secret", Reason: "excluded"}}},
		{State: "sync_uncertain", UpdatedAt: now, ErrorCode: "network_error"},
		{State: "syncing", UpdatedAt: now, FencingToken: 4},
	}
	for i, status := range tests {
		if err := status.Validate(10); !errors.Is(err, ErrStatusInvalid) {
			t.Fatalf("case %d error = %v", i, err)
		}
	}
}

func TestReadStatusRejectsSymlinkAndPermissiveFile(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.json")
	if err := os.WriteFile(target, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadStatus(target, 10); !errors.Is(err, ErrStatusInvalid) {
		t.Fatalf("permissive file error = %v", err)
	}
	link := filepath.Join(root, "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadStatus(link, 10); !errors.Is(err, ErrStatusInvalid) {
		t.Fatalf("symlink error = %v", err)
	}
}
