package configsync

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTakeSnapshotIsBoundedAndRejectsUnsafeEntries(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".zshrc"), []byte("portable"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".large"), []byte("too large"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, ".ssh"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".ssh", "id"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".config", "paperboat", "helper"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".config", "paperboat", "helper", "agent-token"), []byte("runtime secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../outside", filepath.Join(root, ".escape")); err != nil {
		t.Fatal(err)
	}
	policy := RuntimePolicy{
		MaxFileBytes: 8, MaxBatchBytes: 16,
		MandatoryExclusions: append([]string(nil), requiredMandatoryExclusions...),
	}
	snapshot, err := TakeSnapshot(root, policy)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snapshot.Files[".zshrc"]; !ok {
		t.Fatalf("portable file absent: %#v", snapshot)
	}
	if _, ok := snapshot.Files[".ssh/id"]; ok {
		t.Fatal("mandatory-excluded secret was captured")
	}
	if _, ok := snapshot.Files[".config/paperboat/helper/agent-token"]; ok {
		t.Fatal("Paperboat runtime credential was captured")
	}
	reasons := make(map[string]string)
	for _, skipped := range snapshot.Skipped {
		reasons[skipped.Path] = skipped.Reason
	}
	if reasons[".large"] != "max_file_bytes" || reasons[".escape"] != "unsafe_symlink" {
		t.Fatalf("skipped = %#v", snapshot.Skipped)
	}
}

func TestChangedPathsIncludesAddModifyAndDelete(t *testing.T) {
	before := map[string]FileState{
		".deleted": {Hash: "old"},
		".same":    {Hash: "same"},
		".changed": {Hash: "old"},
	}
	after := map[string]FileState{
		".same":    {Hash: "same"},
		".changed": {Hash: "new"},
		".added":   {Hash: "new"},
	}
	got := ChangedPaths(before, after)
	want := []string{".added", ".changed", ".deleted"}
	if len(got) != len(want) {
		t.Fatalf("changed = %#v", got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("changed = %#v", got)
		}
	}
}
